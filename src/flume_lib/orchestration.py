"""Lancer plusieurs sources d'un coup : `run_sources`.

Chaque notebook réécrivait la même boucle — `for source in sources:
run_source(source)` — avec, selon l'auteur, un `try` ou pas, un résumé ou pas,
et rien qui fasse échouer le notebook quand une source avait échoué : or c'est
l'échec du notebook que voit le pipeline Fabric qui l'orchestre.

`run_source` ne lève jamais : l'isolation entre sources est donc acquise. Ce
module apporte ce qui manquait autour — validation de l'ensemble avant le
premier appel, ordre des résultats, politique d'arrêt, parallélisme borné, un
résumé lisible, et un moyen explicite de faire échouer le notebook."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from flume_lib._delta import DEFAULT_LAKEHOUSE_TABLES_PATH, DEFAULT_LOG_SCHEMA
from flume_lib.source import RunResult, run_source

# Longueur de l'erreur recopiée dans une ligne du résumé : le message complet
# reste dans `RunResult.error_message` et dans `log_runs`.
SUMMARY_ERROR_CHARS = 120


class RunSourcesError(Exception):
    """Une ou plusieurs sources ont échoué (ou n'ont pas été lancées).
    `outcome` porte le `SourcesResult` complet."""

    def __init__(self, message: str, outcome: "SourcesResult"):
        super().__init__(message)
        self.outcome = outcome


@dataclass
class SourcesResult:
    """Ce qu'un `run_sources` a fait. `results` suit l'ordre des configs
    fournies, quel que soit l'ordre d'achèvement ; une source non lancée (voir
    `stop_on_failure`) n'y figure pas et son nom est dans `skipped`."""

    results: list[RunResult]
    skipped: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def succeeded(self) -> list[RunResult]:
        return [r for r in self.results if r.status == "success"]

    @property
    def failed(self) -> list[RunResult]:
        return [r for r in self.results if r.status != "success"]

    @property
    def ok(self) -> bool:
        """Toutes les sources ont réussi et aucune n'a été sautée."""
        return not self.failed and not self.skipped

    @property
    def rows_loaded(self) -> int:
        return sum(r.rows_loaded for r in self.results)

    def summary(self) -> str:
        """Un résumé lisible : une ligne de total, puis une ligne par source."""
        head = (
            f"{len(self.results) + len(self.skipped)} source(s): "
            f"{len(self.succeeded)} succeeded, {len(self.failed)} failed, "
            f"{len(self.skipped)} skipped — {self.rows_loaded} row(s) in "
            f"{self.duration_seconds:.1f} s"
        )
        lines = [head]
        for r in self.results:
            line = (
                f"  {'ok' if r.status == 'success' else 'FAILED':<8} {r.source_name}  "
                f"{r.rows_loaded} row(s), {r.pages} page(s), {r.duration_seconds:.1f} s"
            )
            if r.warnings:
                line += f", {len(r.warnings)} warning(s)"
            if r.error_message:
                line += f"  {r.error_message[:SUMMARY_ERROR_CHARS]}"
            lines.append(line)
        lines.extend(f"  {'skipped':<8} {name}" for name in self.skipped)
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.summary()

    def raise_for_failures(self) -> None:
        """Lève `RunSourcesError` si une source a échoué ou a été sautée.

        À appeler en dernière ligne d'un notebook orchestré : un pipeline ne
        voit l'échec d'un notebook que par l'exception qui le termine, et
        `run_source` ne lève jamais."""
        if self.ok:
            return
        raise RunSourcesError(self.summary(), self)


def _check_arguments(configs, max_workers, stop_on_failure, on_progress) -> list[dict]:
    if not isinstance(configs, (list, tuple)):
        raise ValueError("configs must be a list of source configurations")
    if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1:
        raise ValueError("max_workers must be an integer greater than 0")
    if not isinstance(stop_on_failure, bool):
        raise ValueError("stop_on_failure must be True or False")
    if on_progress is not None and not callable(on_progress):
        raise ValueError("on_progress must be a function taking (source_name, pages, rows)")

    seen: dict[str, int] = {}
    targets: dict[tuple, str] = {}
    for index, config in enumerate(configs):
        if not isinstance(config, dict):
            raise ValueError(f"configs[{index}] is not a source configuration (an object)")
        name = config.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                f"configs[{index}] needs a 'name' — it keys the watermark and the lock"
            )
        if name in seen:
            raise ValueError(
                f"two sources are named '{name}' (configs[{seen[name]}] and configs[{index}]): "
                "they would share one watermark and one lock"
            )
        seen[name] = index
        target = (config.get("target_schema"), config.get("target_table"))
        if max_workers > 1 and all(isinstance(part, str) for part in target):
            if target in targets:
                raise ValueError(
                    f"sources '{targets[target]}' and '{name}' write the same table "
                    f"{target[0]}.{target[1]}: run them one after the other "
                    "(max_workers=1), concurrent commits to one table conflict"
                )
            targets[target] = name
    return list(configs)


def _progress_for(on_progress, source_name: str):
    """Le callback d'une source : celui de l'appelant, avec le nom en plus."""
    if on_progress is None:
        return None

    def report(pages: int, rows: int) -> None:
        on_progress(source_name, pages, rows)

    return report


def run_sources(
    configs: list[dict],
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
    log_schema: str = DEFAULT_LOG_SCHEMA,
    dry_run: bool = False,
    max_workers: int = 1,
    stop_on_failure: bool = False,
    on_progress=None,
) -> SourcesResult:
    """Lance plusieurs sources et rend un `SourcesResult`.

    Chaque source passe par `run_source`, donc une source en échec n'en
    interrompt pas une autre : c'est le comportement par défaut. Avec
    `stop_on_failure=True`, aucune source ne démarre après le premier échec
    (celles déjà en cours finissent) ; les noms des sources sautées sont dans
    `SourcesResult.skipped`.

    `max_workers` borne le parallélisme, en threads (les runs attendent surtout
    le réseau). 1, le défaut, enchaîne les sources dans l'ordre donné. Au-delà,
    deux sources qui écrivent la même table sont refusées d'emblée : leurs
    commits concurrents entreraient en conflit. Les tables techniques
    (`watermark`, `log_runs`, `run_locks`) ne posent pas ce problème, elles ne
    reçoivent que des ajouts.

    `on_progress(source_name, pages, rows)` est appelé après chaque page de
    chaque source — depuis les threads de travail quand `max_workers > 1`, donc
    plusieurs appels peuvent se chevaucher.

    Lève `ValueError` — avant de lancer quoi que ce soit — sur une liste mal
    formée, un nom manquant ou en double, ou des paramètres invalides. Ne lève
    pas sur l'échec d'une source : voir `SourcesResult.raise_for_failures`.
    Une interruption (`KeyboardInterrupt`) arrête le lancement des sources
    suivantes et remonte ; les runs déjà en cours dans un thread vont à leur
    terme."""
    sources = _check_arguments(configs, max_workers, stop_on_failure, on_progress)
    started = time.monotonic()
    stop = threading.Event()

    def run_one(config: dict) -> RunResult | None:
        if stop.is_set():
            return None
        result = run_source(
            config,
            lakehouse_tables_path=lakehouse_tables_path,
            storage_options=storage_options,
            log_schema=log_schema,
            dry_run=dry_run,
            on_progress=_progress_for(on_progress, config["name"]),
        )
        if stop_on_failure and result.status != "success":
            stop.set()
        return result

    outcomes: list[RunResult | None]
    if max_workers == 1:
        outcomes = []
        for config in sources:
            outcomes.append(run_one(config))
    else:
        executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="flume-source"
        )
        try:
            futures = [executor.submit(run_one, config) for config in sources]
            outcomes = [future.result() for future in futures]
        except BaseException:
            stop.set()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        executor.shutdown(wait=True)

    results = [r for r in outcomes if r is not None]
    skipped = [c["name"] for c, r in zip(sources, outcomes, strict=True) if r is None]
    return SourcesResult(results, skipped, round(time.monotonic() - started, 3))
