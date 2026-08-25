"""Stratégies de pagination. Chaque stratégie est un générateur qui yield des
listes d'enregistrements page par page, via une fonction fetch_page injectée
(fetch_page(url, params) -> (JSON parsé, headers de réponse)) — le retry et
l'auth sont gérés par l'appelant."""

from collections.abc import Callable, Iterator

from flume_lib.templating import check_value

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
        raise PaginationError(f"Unexpected response of type {type(payload).__name__}")
    if items_field:
        records = get_path(payload, items_field)
        if records is _MISSING:
            raise PaginationError(f"Field '{items_field}' missing from the response")
        if not isinstance(records, list):
            raise PaginationError(
                f"Field '{items_field}': expected a list, "
                f"got {type(records).__name__}"
            )
        return records
    for field in _DEFAULT_ITEMS_FIELDS:
        if isinstance(payload.get(field), list):
            return payload[field]
    raise PaginationError(
        "Unable to locate the records in the response; "
        "set 'items_field' in the pagination config"
    )


def extract_records(
    payload, items_field: str | None = None, record_field: str | None = None
) -> list:
    """Localise la liste d'enregistrements dans une réponse. `items_field`
    accepte un chemin pointé. `record_field` déballe chaque élément — les
    connexions GraphQL enveloppent chaque enregistrement dans un
    `{cursor, node}` dont seul le `node` porte les données."""
    records = _locate_records(payload, items_field)
    if not record_field:
        return records
    unwrapped = []
    for item in records:
        value = get_path(item, record_field) if isinstance(item, dict) else _MISSING
        if value is _MISSING:
            raise PaginationError(
                f"Field '{record_field}' missing from a record — "
                "'record_field' does not match the shape of the response"
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
                    f"Header '{total_pages_header}' missing from the response"
                )
            try:
                total_pages = int(raw)
            except ValueError as exc:
                raise PaginationError(
                    f"Header '{total_pages_header}' is not numeric: '{raw}'"
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

    'has_more_field' — le `pageInfo.hasNextPage` des connexions GraphQL —
    fait autorité quand il est renseigné : une page vide au milieu
    d'une connexion fortement filtrée n'y signifie pas la fin des données,
    alors qu'elle la signifie pour une API qui n'annonce rien.
    """
    cursor_param = pagination_config.get("cursor_param")
    cursor_field = pagination_config.get("cursor_field")
    if not cursor_param or not cursor_field:
        raise PaginationError(
            "pagination 'cursor': 'cursor_param' and 'cursor_field' required"
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
                    f"Field '{has_more_field}' missing from the response"
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
                    f"'{has_more_field}' announces a next page but "
                    f"'{cursor_field}' is missing from the response"
                )
            return
        if next_cursor == cursor:
            raise PaginationError(
                f"Cursor '{cursor_field}' is not advancing — "
                "pagination stopped to avoid an infinite loop"
            )
        cursor = next_cursor


def _advances(new_key, previous_key) -> bool:
    """Vrai si la clé progresse. Deux clés incomparables — l'API a changé de
    type en cours de route — sont considérées comme progressant si elles
    diffèrent : c'est le blocage qu'on cherche à détecter, pas le désordre."""
    try:
        return new_key > previous_key
    except TypeError:
        return new_key != previous_key


def paginate_keyset(
    fetch_page: Callable, base_url: str, params: dict, pagination_config: dict
) -> Iterator[list]:
    """Pagination par clé (keyset / seek). Chaque page est filtrée par la
    valeur de `key_field` du dernier enregistrement de la page précédente,
    renvoyée dans `key_param`.

    Contrairement à `offset`, le coût d'une page ne croît pas avec sa
    profondeur et rien ne plafonne : c'est la seule stratégie qui atteint le
    fond d'une table de plusieurs millions de lignes sur les APIs qui bornent
    l'offset (certaines s'arrêtent à 100 000). En contrepartie, elle exige une
    source **triée par `key_field`**, avec des valeurs uniques : la lib
    vérifie que la clé progresse et s'arrête plutôt que de boucler.

    La clé vient de la réponse de l'API. Avec `"params_in": "body_template"`
    elle est interpolée dans le corps de la requête, donc `value_format` la
    contraint à une forme sans place pour de la syntaxe — même règle que le
    watermark incrémental.
    """
    key_field = pagination_config.get("key_field")
    key_param = pagination_config.get("key_param")
    if not key_field or not key_param:
        raise PaginationError(
            "pagination 'keyset': 'key_field' and 'key_param' required"
        )
    value_format = pagination_config.get("value_format", "any")
    limit = pagination_config.get("limit")
    limit_param = pagination_config.get("limit_param", "limit")
    items_field = pagination_config.get("items_field")
    record_field = pagination_config.get("record_field")
    label = f"pagination keyset: key '{key_field}'"

    key = pagination_config.get("initial_value")
    while True:
        page_params = dict(params)
        if limit is not None:
            page_params[limit_param] = limit
        if key is not None:
            page_params[key_param] = check_value(key, value_format, label=label)
        payload, _ = fetch_page(base_url, page_params)
        records = extract_records(payload, items_field, record_field)
        if not records:
            return
        yield records

        last = records[-1]
        next_key = get_path(last, key_field) if isinstance(last, dict) else _MISSING
        if next_key is _MISSING or next_key is None:
            raise PaginationError(
                f"pagination 'keyset': field '{key_field}' missing from the last "
                "record — the next page cannot be built"
            )
        if key is not None and not _advances(next_key, key):
            raise PaginationError(
                f"pagination 'keyset': the key is not advancing ({key!r} -> "
                f"{next_key!r}) — the source is not sorted by '{key_field}', "
                "or its values are not unique"
            )
        # une page incomplète est la dernière, comme en offset
        if limit is not None and len(records) < limit:
            return
        key = next_key


_STRATEGIES = {
    "offset": paginate_offset,
    "page": paginate_page,
    "next_link": paginate_next_link,
    "cursor": paginate_cursor,
    "keyset": paginate_keyset,
}


def _fingerprint(page: list):
    """Empreinte bon marché d'une page, calculée avant qu'elle ne soit livrée
    (l'appelant y ajoute ensuite les colonnes de traçabilité)."""
    return len(page), repr(page[0])[:200], repr(page[-1])[:200]


def _bounded(pages: Iterator[list], pagination_config: dict) -> Iterator[list]:
    """Garde-fous communs à toutes les stratégies.

    `max_pages` et `max_rows` bornent un run dont on connaît l'ordre de
    grandeur. La détection de page répétée, elle, s'applique toujours : une
    API qui reclampe un numéro de page hors limite et resert indéfiniment la
    première n'a aucune condition d'arrêt naturelle, et le notebook tournait
    jusqu'à son timeout, mémoire en hausse.

    Atteindre une borne est une **erreur**, pas un arrêt propre : tronquer en
    silence donnerait un run `success` amputé d'une partie des données. Les
    lignes déjà écrites le restent, et le message dit ce qui s'est passé.
    """
    max_pages = pagination_config.get("max_pages")
    max_rows = pagination_config.get("max_rows")
    pages_seen = 0
    rows_seen = 0
    previous = None

    for page in pages:
        current = _fingerprint(page) if page else None
        if current is not None and current == previous:
            raise PaginationError(
                f"pagination: page {pages_seen + 1} is identical to the "
                "previous one — the source is not advancing, stopping to avoid "
                "an infinite loop"
            )
        previous = current
        pages_seen += 1
        rows_seen += len(page)
        yield page

        if max_pages is not None and pages_seen >= max_pages:
            raise PaginationError(
                f"pagination: 'max_pages' cap of {max_pages} reached "
                f"({rows_seen} rows read) — run stopped before reaching the end "
                "of the source. Raise the cap, or narrow the source window."
            )
        if max_rows is not None and rows_seen >= max_rows:
            raise PaginationError(
                f"pagination: 'max_rows' cap of {max_rows} reached "
                f"({pages_seen} pages read) — run stopped before reaching the end "
                "of the source. Raise the cap, or narrow the source window."
            )


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
            f"Unknown pagination type: '{pagination_config['type']}'"
        )
    yield from _bounded(
        strategy(fetch_page, base_url, params, pagination_config),
        pagination_config,
    )
