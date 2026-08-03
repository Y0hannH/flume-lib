"""Stratégies de pagination. Chaque stratégie est un générateur qui yield des
listes d'enregistrements page par page, via une fonction fetch_page injectée
(fetch_page(url, params) -> (JSON parsé, headers de réponse)) — le retry et
l'auth sont gérés par l'appelant."""

from collections.abc import Callable, Iterator

_DEFAULT_ITEMS_FIELDS = ("data", "items", "results", "value")


class PaginationError(Exception):
    pass


def extract_records(payload, items_field: str | None = None) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if items_field:
            records = payload.get(items_field)
            if records is None:
                raise PaginationError(
                    f"Champ '{items_field}' absent de la réponse"
                )
            return records
        for field in _DEFAULT_ITEMS_FIELDS:
            if isinstance(payload.get(field), list):
                return payload[field]
        raise PaginationError(
            "Impossible de localiser les enregistrements dans la réponse ; "
            "précisez 'items_field' dans la config pagination"
        )
    raise PaginationError(f"Réponse inattendue de type {type(payload).__name__}")


def paginate_offset(
    fetch_page: Callable, base_url: str, params: dict, pagination_config: dict
) -> Iterator[list]:
    limit = pagination_config.get("limit", 100)
    limit_param = pagination_config.get("limit_param", "limit")
    offset_param = pagination_config.get("offset_param", "offset")
    items_field = pagination_config.get("items_field")

    offset = 0
    while True:
        page_params = {**params, limit_param: limit, offset_param: offset}
        payload, _ = fetch_page(base_url, page_params)
        records = extract_records(payload, items_field)
        if not records:
            return
        yield records
        if len(records) < limit:
            return
        offset += limit


def paginate_next_link(
    fetch_page: Callable, base_url: str, params: dict, pagination_config: dict
) -> Iterator[list]:
    next_field = pagination_config.get("next_field", "next")
    items_field = pagination_config.get("items_field")

    url = base_url
    page_params = params
    while url:
        payload, _ = fetch_page(url, page_params)
        records = extract_records(payload, items_field)
        if records:
            yield records
        if not isinstance(payload, dict):
            return
        url = payload.get(next_field)
        # l'URL suivante embarque déjà ses query params
        page_params = {}


def paginate_page(
    fetch_page: Callable, base_url: str, params: dict, pagination_config: dict
) -> Iterator[list]:
    """Pagination par numéro de page. Si 'total_pages_header' est renseigné,
    le nombre total de pages est lu dans les headers de la première réponse ;
    sinon, arrêt sur page vide (ou partielle si 'page_size' est connu)."""
    page_param = pagination_config.get("page_param", "page")
    start_page = pagination_config.get("start_page", 1)
    size_param = pagination_config.get("size_param")
    page_size = pagination_config.get("page_size")
    total_pages_header = pagination_config.get("total_pages_header")
    items_field = pagination_config.get("items_field")

    total_pages = None
    page = start_page
    while True:
        page_params = {**params, page_param: page}
        if size_param and page_size:
            page_params[size_param] = page_size
        payload, headers = fetch_page(base_url, page_params)
        records = extract_records(payload, items_field)

        if total_pages is None and total_pages_header:
            raw = headers.get(total_pages_header)
            if raw is None:
                raise PaginationError(
                    f"Header '{total_pages_header}' absent de la réponse"
                )
            try:
                total_pages = int(raw)
            except ValueError as exc:
                raise PaginationError(
                    f"Header '{total_pages_header}' non numérique : '{raw}'"
                ) from exc

        if records:
            yield records

        if total_pages is not None:
            if page - start_page + 1 >= total_pages:
                return
        elif not records or (page_size and len(records) < page_size):
            return
        page += 1


def paginate_cursor(
    fetch_page: Callable, base_url: str, params: dict, pagination_config: dict
) -> Iterator[list]:
    raise NotImplementedError("La pagination 'cursor' n'est pas encore implémentée")


_STRATEGIES = {
    "offset": paginate_offset,
    "page": paginate_page,
    "next_link": paginate_next_link,
    "cursor": paginate_cursor,
}


def paginate(
    fetch_page: Callable,
    base_url: str,
    params: dict,
    pagination_config: dict | None,
) -> Iterator[list]:
    """Point d'entrée : sélectionne la stratégie selon pagination_config['type'].
    Sans config de pagination, effectue un appel unique."""
    if not pagination_config or pagination_config.get("type") in (None, "none"):
        payload, _ = fetch_page(base_url, params)
        records = extract_records(payload)
        if records:
            yield records
        return

    strategy = _STRATEGIES.get(pagination_config["type"])
    if strategy is None:
        raise PaginationError(
            f"Type de pagination inconnu : '{pagination_config['type']}'"
        )
    yield from strategy(fetch_page, base_url, params, pagination_config)
