"""Rattrapage d'une source lue par fenêtres de dates : `run_source_windows`.

Une API dont le filtre est une fenêtre (« du … au … ») et dont la fenêtre
maximale est bornée oblige l'appelant à une boucle : relire le watermark,
calculer la borne haute, lancer le run, recommencer. Chaque notebook la
réécrivait, avec sa propre détection d'une boucle qui ne progresse pas.
Celle-ci est écrite une fois, et elle est stricte : elle ne continue jamais
derrière un run échoué, et elle refuse de relancer une fenêtre qui n'a pas fait
avancer la source."""

from collections.abc import Callable
from datetime import date, timedelta

from flume_lib._delta import (
    DEFAULT_LAKEHOUSE_TABLES_PATH,
    DEFAULT_LOG_SCHEMA,
    resolve_lakehouse_tables_path,
)
from flume_lib.incremental import compare, parse_date
from flume_lib.source import RunResult, run_source
from flume_lib.watermark import read_watermark

DEFAULT_MAX_ITERATIONS = 1000


class WindowRunError(Exception):
    """La boucle de fenêtres s'est arrêtée sans atteindre `until`. `results`
    porte les runs déjà faits — dont le dernier, celui qui a échoué ou qui n'a
    pas avancé. Le watermark, lui, est resté là où le dernier run réussi l'a
    laissé : relancer reprend de là."""

    def __init__(self, message: str, results: list):
        super().__init__(message)
        self.results = results


def _as_date(value, label: str) -> date:
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    parsed = parse_date(value)
    if parsed is None:
        raise ValueError(f"{label}: {value!r} is not a yyyy-MM-dd date")
    return parsed


def _require_positive_int(name: str, value) -> None:
    # bool est un int en Python : True passerait pour 1
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be an integer greater than 0")


def _starting_day(raw, label: str) -> date:
    # un watermark horodaté (« 2026-09-28T10:00:00Z ») donne son jour
    parsed = parse_date(str(raw)[:10])
    if parsed is None:
        raise ValueError(
            f"{label}: {raw!r} does not start with a yyyy-MM-dd date — "
            "run_source_windows slices by day"
        )
    return parsed


def _advanced(result: RunResult) -> bool:
    if result.watermark_after is None:
        return False
    if result.watermark_before is None:
        return True
    return compare(result.watermark_after, result.watermark_before) == 1


def run_source_windows(
    config_factory: Callable[[str], dict],
    until,
    max_span_days: int,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
    log_schema: str = DEFAULT_LOG_SCHEMA,
) -> list[RunResult]:
    """Lance `run_source` fenêtre après fenêtre jusqu'à `until`.

    `config_factory(window_end)` reçoit la borne haute de la fenêtre
    (`yyyy-MM-dd`) et retourne la config complète de la source : elle doit la
    placer dans `params` **et** dans `incremental.window.upper_value`. Elle est
    appelée une première fois avec `until`, pour connaître le nom de la source
    et son `initial_value`.

    À chaque tour : relire le watermark, borne haute = `min(until, départ +
    max_span_days)`, lancer le run. S'arrête quand une fenêtre atteint `until`.
    Lève `WindowRunError` — jamais d'erreur avalée — quand :

    - un run échoue ;
    - un run réussit sans faire avancer le watermark. Avec `advance:
      "field_max"` cela arrive dès que les lignes de la borne basse reviennent
      sans rien de plus récent : la même requête serait relancée à l'infini,
      d'où `"advance": "upper_bound"` pour ces sources ;
    - `max_iterations` fenêtres ont tourné sans atteindre `until`.

    Retourne la liste des `RunResult`, dans l'ordre. Liste vide si la source
    est déjà à `until` ou au-delà."""
    until_day = _as_date(until, "until")
    _require_positive_int("max_span_days", max_span_days)
    _require_positive_int("max_iterations", max_iterations)

    tables = resolve_lakehouse_tables_path(lakehouse_tables_path)
    probe = config_factory(until_day.isoformat())
    source_name = probe.get("name")
    if not source_name:
        raise ValueError("the config returned by config_factory needs a 'name'")
    initial_value = (probe.get("incremental") or {}).get("initial_value")

    results: list[RunResult] = []
    for _ in range(max_iterations):
        stored = read_watermark(
            tables, source_name, schema=log_schema, storage_options=storage_options
        )
        origin = stored if stored is not None else initial_value
        if origin is None:
            raise ValueError(
                f"source '{source_name}' has no watermark and no "
                "incremental.initial_value — the first window has no start"
            )
        start = _starting_day(origin, f"watermark of '{source_name}'")
        if start >= until_day:
            return results

        window_end = min(until_day, start + timedelta(days=max_span_days))
        result = run_source(
            config_factory(window_end.isoformat()),
            lakehouse_tables_path=tables,
            storage_options=storage_options,
            log_schema=log_schema,
        )
        results.append(result)

        if result.status != "success":
            raise WindowRunError(
                f"window ending {window_end} failed: {result.error_message}", results
            )
        if result.window_end is None:
            raise WindowRunError(
                "the config returned by config_factory has no incremental.window — "
                "without it the run has no upper bound to report",
                results,
            )
        if not _advanced(result):
            raise WindowRunError(
                f"window ending {window_end} loaded {result.rows_loaded} row(s) but "
                f"the watermark did not move (still {result.watermark_after!r}). "
                'Re-running would send the same request forever — use "advance": '
                '"upper_bound" when the lower bound keeps returning its own rows.',
                results,
            )
        if _as_date(result.window_end, "window_end") >= until_day:
            return results

    raise WindowRunError(
        f"max_iterations={max_iterations} windows ran without reaching {until_day} "
        f"(watermark: {results[-1].watermark_after!r}) — raise max_iterations or "
        "max_span_days, then call again to resume",
        results,
    )
