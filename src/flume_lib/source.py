"""Point d'entrée unique : run_source(config) -> RunResult. Ne lève jamais
d'exception vers l'appelant."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from flume_lib._delta import append_records, resolve_lakehouse_tables_path, table_uri
from flume_lib.auth import build_auth_headers
from flume_lib.logging_ import write_log_run
from flume_lib.pagination import paginate
from flume_lib.watermark import read_watermark, write_watermark

DEFAULT_LAKEHOUSE_TABLES_PATH = "/lakehouse/default/Tables"
DEFAULT_TIMEOUT_SECONDS = 60


class RetryableHTTPError(Exception):
    def __init__(self, status_code: int, url: str):
        super().__init__(f"HTTP {status_code} sur {url}")
        self.status_code = status_code


_RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    RetryableHTTPError,
)


@dataclass
class RunResult:
    source_name: str
    status: str  # "success" | "failed"
    rows_loaded: int
    error_message: str | None
    start_ts: str
    end_ts: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_fetch_page(config: dict):
    headers = build_auth_headers(config.get("auth"))
    retry_config = config.get("retry", {})
    timeout = config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    retryer = Retrying(
        stop=stop_after_attempt(retry_config.get("max_attempts", 3)),
        wait=wait_exponential(multiplier=retry_config.get("backoff_multiplier", 1)),
        retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    session = requests.Session()
    session.headers.update(headers)

    def _get(url: str, params: dict):
        response = session.get(url, params=params, timeout=timeout)
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableHTTPError(response.status_code, url)
        response.raise_for_status()
        # headers requis par certaines stratégies (ex. total de pages)
        return response.json(), response.headers

    def fetch_page(url: str, params: dict):
        return retryer(_get, url, params)

    return fetch_page


def _max_incremental_value(records: list[dict], field_name: str):
    values = [r[field_name] for r in records if r.get(field_name) is not None]
    return max(values) if values else None


def run_source(
    config: dict,
    lakehouse_tables_path: str = DEFAULT_LAKEHOUSE_TABLES_PATH,
    storage_options: dict | None = None,
) -> RunResult:
    """Exécute l'ingestion d'une source d'après sa config. Toute erreur est
    catchée et remontée dans RunResult, jamais levée vers l'appelant.

    Dans Fabric, le chemin local par défaut est automatiquement résolu vers
    l'URI ABFSS OneLake du lakehouse par défaut (le montage local ne permet
    pas le commit du transaction log delta-rs). storage_options est passé tel
    quel à delta-rs pour un stockage non-Fabric ou une auth spécifique."""
    source_name = config.get("name", "<sans_nom>")
    start_ts = _utc_now()
    status = "failed"
    rows_loaded = 0
    error_message = None

    try:
        lakehouse_tables_path = resolve_lakehouse_tables_path(lakehouse_tables_path)
    except Exception:  # noqa: BLE001
        pass

    try:
        incremental = config.get("incremental", {})
        params = dict(config.get("params", {}))
        if incremental.get("enabled"):
            last_value = read_watermark(
                lakehouse_tables_path, source_name, storage_options=storage_options
            )
            if last_value is not None:
                params[incremental["param_name"]] = last_value

        fetch_page = _build_fetch_page(config)
        records: list[dict] = []
        for page in paginate(
            fetch_page, config["base_url"], params, config.get("pagination")
        ):
            records.extend(page)

        if records:
            append_records(
                table_uri(lakehouse_tables_path, config["target_table"]),
                records,
                storage_options=storage_options,
            )
        rows_loaded = len(records)

        if incremental.get("enabled") and records:
            new_watermark = _max_incremental_value(records, incremental["field"])
            if new_watermark is not None:
                write_watermark(
                    lakehouse_tables_path,
                    source_name,
                    new_watermark,
                    storage_options=storage_options,
                )

        status = "success"
    except Exception as exc:  # noqa: BLE001 — contrat : ne jamais lever
        error_message = f"{type(exc).__name__}: {exc}"

    end_ts = _utc_now()
    result = RunResult(
        source_name=source_name,
        status=status,
        rows_loaded=rows_loaded,
        error_message=error_message,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    try:
        write_log_run(
            lakehouse_tables_path,
            run_id=result.run_id,
            source_name=source_name,
            start_ts=start_ts,
            end_ts=end_ts,
            status=status,
            rows_loaded=rows_loaded,
            error_message=error_message,
            storage_options=storage_options,
        )
    except Exception as exc:  # noqa: BLE001
        log_error = f"écriture log_runs impossible — {type(exc).__name__}: {exc}"
        result.error_message = (
            f"{error_message} | {log_error}" if error_message else log_error
        )

    return result
