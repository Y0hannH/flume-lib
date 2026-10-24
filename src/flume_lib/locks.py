"""Verrou consultatif d'une source : `single_instance`.

Deux runs de la même source qui se chevauchent — un notebook relancé à la main
pendant qu'un pipeline tourne — écrivent chacun la même fenêtre : en append,
tout est dupliqué ; avec un watermark, le second repart d'une position que le
premier va dépasser. La lib ne l'empêchait pas.

Ce verrou est **consultatif** : il ne tient que si tous les runs de la source
l'utilisent. Il vit dans la table technique `run_locks` (colonnes :
source_name, run_id, state, ts, expires_ts), à côté de `watermark` et `log_runs`,
parce que delta-rs ne fournit pas de création conditionnelle sur un fichier :
la seule atomicité disponible est celle du commit Delta, et deux écritures
simultanées en ajout ne se gênent pas.

D'où le protocole en deux temps, sûr même sans atomicité :

1. lire les verrous actifs de la source — s'il y en a un, refuser ;
2. écrire sa ligne `acquired`, puis **relire** : si un autre verrou actif est
   visible, retirer le sien et refuser.

Deux runs ne peuvent pas passer tous les deux. Si A relit sans voir B, la ligne
de B n'était pas encore commitée à ce moment-là, donc B la relit après celle de
A et la voit. Le prix : deux runs presque simultanés peuvent se refuser
**tous les deux** — l'un des deux doit alors être relancé. Refuser à tort est
sans conséquence ; laisser passer à tort ne l'est pas.

Le verrou expire (`lock_ttl_seconds`) : un run mort ne le libère pas, et sans
échéance il bloquerait la source pour toujours.
"""

from datetime import datetime, timedelta, timezone

from flume_lib._delta import query_table, sql_quote, table_uri, write_records

LOCKS_TABLE = "run_locks"
STATE_ACQUIRED = "acquired"
STATE_RELEASED = "released"
DEFAULT_LOCK_TTL_SECONDS = 6 * 3600


class RunLockedError(Exception):
    """Un autre run de la même source est en cours."""


def stamp(moment: datetime) -> str:
    """Horodatage UTC à largeur fixe : les comparer comme du texte (ce que fait
    SQL ici) revient à les comparer comme des instants. `isoformat` omet les
    microsecondes quand elles valent zéro, et rompt cet ordre."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def _row(source_name: str, run_id: str, state: str, now: datetime, ttl: float) -> dict:
    return {
        "source_name": source_name,
        "run_id": run_id,
        "state": state,
        "ts": stamp(now),
        "expires_ts": stamp(now + timedelta(seconds=ttl)),
    }


def active_runs(uri: str, source_name: str, storage_options=None, now=None) -> dict[str, str]:
    """Runs qui tiennent le verrou de la source : {run_id: instant de prise}.

    Une prise sans libération, et non expirée. La ligne `released` d'un run porte
    une échéance postérieure à la sienne : si la prise est visible dans
    l'intervalle, la libération l'est aussi."""
    now = now or datetime.now(timezone.utc)
    rows = query_table(
        uri,
        # source_name est cité par `sql_quote`, l'instant sort de `stamp` (format
        # fixe) : le reste est une constante
        "select run_id, state, ts from l "  # nosec B608
        f"where source_name = {sql_quote(source_name)} "
        f"and expires_ts > '{stamp(now)}'",
        alias="l",
        storage_options=storage_options,
    )
    released = {r["run_id"] for r in rows if r["state"] == STATE_RELEASED}
    return {
        r["run_id"]: r["ts"]
        for r in rows
        if r["state"] == STATE_ACQUIRED and r["run_id"] not in released
    }


class RunLock:
    """Le verrou d'un run. Sans effet tant que `acquire` n'a pas une config qui
    le demande ; `release` ne fait rien si rien n'a été pris — il peut donc se
    trouver dans un `finally` sans condition."""

    def __init__(
        self,
        lakehouse_tables_path: str,
        source_name: str,
        run_id: str,
        schema: str,
        storage_options: dict | None,
        enabled: bool = True,
    ):
        self._path = lakehouse_tables_path
        self._source_name = source_name
        self._run_id = run_id
        self._schema = schema
        self._storage_options = storage_options
        self._enabled = enabled
        self._ttl = DEFAULT_LOCK_TTL_SECONDS
        self.held = False

    @property
    def _uri(self) -> str:
        return table_uri(self._path, self._schema, LOCKS_TABLE)

    def acquire(self, config: dict) -> None:
        """Prend le verrou si la config demande `single_instance`. Lève
        `RunLockedError` si la source est déjà prise."""
        if not self._enabled or not config.get("single_instance"):
            return
        self._ttl = config.get("lock_ttl_seconds", DEFAULT_LOCK_TTL_SECONDS)
        holders = active_runs(self._uri, self._source_name, self._storage_options)
        if holders:
            raise self._refusal(holders)
        write_records(
            self._uri,
            [_row(self._source_name, self._run_id, STATE_ACQUIRED,
                  datetime.now(timezone.utc), self._ttl)],
            storage_options=self._storage_options,
        )
        self.held = True
        rivals = {
            run_id: ts
            for run_id, ts in active_runs(
                self._uri, self._source_name, self._storage_options
            ).items()
            if run_id != self._run_id
        }
        if rivals:
            try:
                self.release()
            except Exception:  # noqa: BLE001 — l'échéance du verrou prend le relais
                pass
            raise self._refusal(rivals)

    def release(self) -> None:
        """Libère le verrou, s'il est tenu."""
        if not self.held:
            return
        self.held = False
        write_records(
            self._uri,
            [_row(self._source_name, self._run_id, STATE_RELEASED,
                  datetime.now(timezone.utc), self._ttl)],
            storage_options=self._storage_options,
        )

    def _refusal(self, holders: dict[str, str]) -> RunLockedError:
        run_id, since = sorted(holders.items(), key=lambda item: item[1])[0]
        return RunLockedError(
            f"source '{self._source_name}' is already being loaded by run {run_id} "
            f"(since {since}). \"single_instance\" refuses a second run; if that run "
            f"died, its lock expires after lock_ttl_seconds ({self._ttl:g} s)."
        )
