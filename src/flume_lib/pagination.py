"""Stratégies de pagination. Chaque stratégie est un générateur qui yield des
listes d'enregistrements page par page, via une fonction fetch_page injectée
(fetch_page(url, params) -> (JSON parsé, headers de réponse)) — le retry et
l'auth sont gérés par l'appelant."""

from collections.abc import Callable, Iterator

_DEFAULT_ITEMS_FIELDS = ("data", "items", "results", "value")

# Distingue « chemin absent » de « chemin présent valant None » : hasNextPage
# à False et hasNextPage absent n'ont pas le même sens, le second est une
# réponse qu'on ne sait pas interpréter.
_MISSING = object()


class PaginationError(Exception):
    pass


def get_path(payload, path: str):
    """Descend un chemin pointé (`data.orders.pageInfo.endCursor`) dans une
    réponse JSON. Retourne `_MISSING` si une étape n'existe pas."""
    value = payload
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


def _locate_records(payload, items_field: str | None) -> list:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        raise PaginationError(f"Réponse inattendue de type {type(payload).__name__}")
    if items_field:
        records = get_path(payload, items_field)
        if records is _MISSING:
            raise PaginationError(f"Champ '{items_field}' absent de la réponse")
        if not isinstance(records, list):
            raise PaginationError(
                f"Champ '{items_field}' : liste attendue, "
                f"{type(records).__name__} reçu"
            )
        return records
    for field in _DEFAULT_ITEMS_FIELDS:
        if isinstance(payload.get(field), list):
            return payload[field]
    raise PaginationError(
        "Impossible de localiser les enregistrements dans la réponse ; "
        "précisez 'items_field' dans la config pagination"
    )


def extract_records(
    payload, items_field: str | None = None, record_field: str | None = None
) -> list:
    """Localise la liste d'enregistrements dans une réponse. `items_field`
    accepte un chemin pointé. `record_field` déballe chaque élément — les
    connexions Relay (GraphQL) enveloppent chaque enregistrement dans un
    `{cursor, node}` dont seul le `node` porte les données."""
    records = _locate_records(payload, items_field)
    if not record_field:
        return records
    unwrapped = []
    for item in records:
        value = get_path(item, record_field) if isinstance(item, dict) else _MISSING
        if value is _MISSING:
            raise PaginationError(
                f"Champ '{record_field}' absent d'un enregistrement — "
                "'record_field' ne correspond pas à la forme de la réponse"
            )
        unwrapped.append(value)
    return unwrapped


def paginate_offset(
    fetch_page: Callable, base_url: str, params: dict, pagination_config: dict
) -> Iterator[list]:
    limit = pagination_config.get("limit", 100)
    limit_param = pagination_config.get("limit_param", "limit")
    offset_param = pagination_config.get("offset_param", "offset")
    items_field = pagination_config.get("items_field")
    record_field = pagination_config.get("record_field")

    offset = 0
    while True:
        page_params = {**params, limit_param: limit, offset_param: offset}
        payload, _ = fetch_page(base_url, page_params)
        records = extract_records(payload, items_field, record_field)
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
    record_field = pagination_config.get("record_field")

    url = base_url
    page_params = params
    while url:
        payload, _ = fetch_page(url, page_params)
        records = extract_records(payload, items_field, record_field)
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
    record_field = pagination_config.get("record_field")

    total_pages = None
    page = start_page
    while True:
        page_params = {**params, page_param: page}
        if size_param and page_size:
            page_params[size_param] = page_size
        payload, headers = fetch_page(base_url, page_params)
        records = extract_records(payload, items_field, record_field)

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
    """Pagination par curseur opaque. Le curseur de la page suivante est lu
    par chemin pointé dans la réponse ('cursor_field') et renvoyé tel quel
    dans le paramètre 'cursor_param' ; la première requête part sans lui.

    'has_more_field' — le `pageInfo.hasNextPage` des connexions Relay
    (GraphQL) — fait autorité quand il est renseigné : une page vide au milieu
    d'une connexion fortement filtrée n'y signifie pas la fin des données,
    alors qu'elle la signifie pour une API qui n'annonce rien.
    """
    cursor_param = pagination_config.get("cursor_param")
    cursor_field = pagination_config.get("cursor_field")
    if not cursor_param or not cursor_field:
        raise PaginationError(
            "pagination 'cursor' : 'cursor_param' et 'cursor_field' requis"
        )
    has_more_field = pagination_config.get("has_more_field")
    limit = pagination_config.get("limit")
    limit_param = pagination_config.get("limit_param", "limit")
    items_field = pagination_config.get("items_field")
    record_field = pagination_config.get("record_field")

    cursor = None
    while True:
        page_params = dict(params)
        if limit is not None:
            page_params[limit_param] = limit
        if cursor is not None:
            page_params[cursor_param] = cursor
        payload, _ = fetch_page(base_url, page_params)
        records = extract_records(payload, items_field, record_field)
        if records:
            yield records

        has_more = None
        if has_more_field:
            has_more = get_path(payload, has_more_field)
            if has_more is _MISSING:
                raise PaginationError(
                    f"Champ '{has_more_field}' absent de la réponse"
                )
            if not has_more:
                return
        elif not records:
            return

        next_cursor = get_path(payload, cursor_field)
        if next_cursor is _MISSING or next_cursor is None:
            if has_more:
                # Tronquer ici passerait pour un succès partiel silencieux.
                raise PaginationError(
                    f"'{has_more_field}' annonce une page suivante mais "
                    f"'{cursor_field}' est absent de la réponse"
                )
            return
        if next_cursor == cursor:
            raise PaginationError(
                f"Le curseur '{cursor_field}' ne progresse pas — "
                "pagination interrompue pour éviter une boucle infinie"
            )
        cursor = next_cursor


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
        records = extract_records(
            payload,
            (pagination_config or {}).get("items_field"),
            (pagination_config or {}).get("record_field"),
        )
        if records:
            yield records
        return

    strategy = _STRATEGIES.get(pagination_config["type"])
    if strategy is None:
        raise PaginationError(
            f"Type de pagination inconnu : '{pagination_config['type']}'"
        )
    yield from strategy(fetch_page, base_url, params, pagination_config)
