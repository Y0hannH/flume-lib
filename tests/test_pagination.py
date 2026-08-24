import pytest

from flume_lib.pagination import PaginationError, extract_records, paginate


def make_fetch(pages_by_call):
    """Chaque entrée est un payload JSON, ou un tuple (payload, headers)."""
    calls = []

    def fetch_page(url, params):
        calls.append((url, dict(params)))
        entry = pages_by_call[len(calls) - 1]
        if isinstance(entry, tuple):
            return entry
        return entry, {}

    fetch_page.calls = calls
    return fetch_page


class TestExtractRecords:
    def test_top_level_list(self):
        assert extract_records([{"a": 1}]) == [{"a": 1}]

    def test_explicit_items_field(self):
        assert extract_records({"rows": [{"a": 1}]}, "rows") == [{"a": 1}]

    def test_common_field_fallback(self):
        assert extract_records({"data": [{"a": 1}]}) == [{"a": 1}]
        assert extract_records({"value": []}) == []

    def test_missing_items_field_raises(self):
        with pytest.raises(PaginationError):
            extract_records({"other": []}, "rows")

    def test_unlocatable_records_raises(self):
        with pytest.raises(PaginationError):
            extract_records({"foo": "bar"})

    def test_dotted_items_field(self):
        payload = {"data": {"orders": {"edges": [{"node": {"id": 1}}]}}}
        assert extract_records(payload, "data.orders.edges") == [{"node": {"id": 1}}]

    def test_missing_dotted_path_raises(self):
        payload = {"data": {"orders": {}}}
        with pytest.raises(PaginationError, match="data.orders.edges"):
            extract_records(payload, "data.orders.edges")

    def test_items_field_pointing_at_a_non_list_raises(self):
        # cas GraphQL typique : 'data' est un objet, pas une liste
        with pytest.raises(PaginationError, match="liste attendue"):
            extract_records({"data": {"orders": {}}}, "data")

    def test_record_field_unwraps_each_item(self):
        payload = {"data": {"orders": {"edges": [
            {"cursor": "c1", "node": {"id": 1}},
            {"cursor": "c2", "node": {"id": 2}},
        ]}}}
        assert extract_records(payload, "data.orders.edges", "node") == [
            {"id": 1}, {"id": 2}
        ]

    def test_record_field_absent_from_an_item_raises(self):
        payload = {"rows": [{"node": {"id": 1}}, {"autre": {}}]}
        with pytest.raises(PaginationError, match="record_field"):
            extract_records(payload, "rows", "node")


class TestOffsetPagination:
    CONFIG = {"type": "offset", "limit": 2, "limit_param": "limit", "offset_param": "offset"}

    def test_iterates_until_partial_page(self):
        fetch = make_fetch([
            [{"id": 1}, {"id": 2}],
            [{"id": 3}],
        ])
        pages = list(paginate(fetch, "http://api/x", {}, self.CONFIG))
        assert pages == [[{"id": 1}, {"id": 2}], [{"id": 3}]]
        assert fetch.calls == [
            ("http://api/x", {"limit": 2, "offset": 0}),
            ("http://api/x", {"limit": 2, "offset": 2}),
        ]

    def test_stops_on_empty_page(self):
        fetch = make_fetch([
            [{"id": 1}, {"id": 2}],
            [],
        ])
        pages = list(paginate(fetch, "http://api/x", {}, self.CONFIG))
        assert pages == [[{"id": 1}, {"id": 2}]]

    def test_preserves_base_params(self):
        fetch = make_fetch([[{"id": 1}]])
        list(paginate(fetch, "http://api/x", {"updated_since": "2024-01-01"}, self.CONFIG))
        assert fetch.calls[0][1] == {"updated_since": "2024-01-01", "limit": 2, "offset": 0}

    def test_custom_param_names(self):
        config = {"type": "offset", "limit": 5, "limit_param": "top", "offset_param": "skip"}
        fetch = make_fetch([[{"id": 1}]])
        list(paginate(fetch, "http://api/x", {}, config))
        assert fetch.calls[0][1] == {"top": 5, "skip": 0}


class TestNextLinkPagination:
    CONFIG = {"type": "next_link", "items_field": "data", "next_field": "next"}

    def test_follows_next_links(self):
        fetch = make_fetch([
            {"data": [{"id": 1}], "next": "http://api/x?page=2"},
            {"data": [{"id": 2}], "next": None},
        ])
        pages = list(paginate(fetch, "http://api/x", {"q": "a"}, self.CONFIG))
        assert pages == [[{"id": 1}], [{"id": 2}]]
        # la première requête porte les params, les suivantes utilisent l'URL brute
        assert fetch.calls == [
            ("http://api/x", {"q": "a"}),
            ("http://api/x?page=2", {}),
        ]

    def test_single_page_without_next(self):
        fetch = make_fetch([{"data": [{"id": 1}]}])
        pages = list(paginate(fetch, "http://api/x", {}, self.CONFIG))
        assert pages == [[{"id": 1}]]
        assert len(fetch.calls) == 1


class TestPagePagination:
    def test_total_pages_from_header(self):
        config = {"type": "page", "total_pages_header": "X-Total-Pages"}
        fetch = make_fetch([
            ([{"id": 1}], {"X-Total-Pages": "3"}),
            ([{"id": 2}], {"X-Total-Pages": "3"}),
            ([{"id": 3}], {"X-Total-Pages": "3"}),
        ])
        pages = list(paginate(fetch, "http://api/x", {}, config))
        assert pages == [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
        assert [c[1] for c in fetch.calls] == [{"page": 1}, {"page": 2}, {"page": 3}]

    def test_single_page_from_header(self):
        config = {"type": "page", "total_pages_header": "X-Total-Pages"}
        fetch = make_fetch([([{"id": 1}], {"X-Total-Pages": "1"})])
        pages = list(paginate(fetch, "http://api/x", {}, config))
        assert pages == [[{"id": 1}]]
        assert len(fetch.calls) == 1

    def test_missing_header_raises(self):
        config = {"type": "page", "total_pages_header": "X-Total-Pages"}
        fetch = make_fetch([([{"id": 1}], {})])
        with pytest.raises(PaginationError, match="X-Total-Pages"):
            list(paginate(fetch, "http://api/x", {}, config))

    def test_non_numeric_header_raises(self):
        config = {"type": "page", "total_pages_header": "X-Total-Pages"}
        fetch = make_fetch([([{"id": 1}], {"X-Total-Pages": "beaucoup"})])
        with pytest.raises(PaginationError, match="non numérique"):
            list(paginate(fetch, "http://api/x", {}, config))

    def test_custom_params_and_start_page(self):
        config = {
            "type": "page",
            "page_param": "p",
            "size_param": "per_page",
            "page_size": 50,
            "start_page": 0,
            "total_pages_header": "Total-Pages",
        }
        fetch = make_fetch([
            ([{"id": 1}], {"Total-Pages": "2"}),
            ([{"id": 2}], {"Total-Pages": "2"}),
        ])
        pages = list(paginate(fetch, "http://api/x", {}, config))
        assert pages == [[{"id": 1}], [{"id": 2}]]
        assert [c[1] for c in fetch.calls] == [
            {"p": 0, "per_page": 50},
            {"p": 1, "per_page": 50},
        ]

    def test_without_header_stops_on_empty(self):
        config = {"type": "page"}
        fetch = make_fetch([[{"id": 1}], [{"id": 2}], []])
        pages = list(paginate(fetch, "http://api/x", {}, config))
        assert pages == [[{"id": 1}], [{"id": 2}]]

    def test_without_header_stops_on_partial_page(self):
        config = {"type": "page", "size_param": "per_page", "page_size": 2}
        fetch = make_fetch([[{"id": 1}, {"id": 2}], [{"id": 3}]])
        pages = list(paginate(fetch, "http://api/x", {}, config))
        assert pages == [[{"id": 1}, {"id": 2}], [{"id": 3}]]
        assert len(fetch.calls) == 2


class TestCursorPagination:
    """Forme Relay/GraphQL : enregistrements sous un chemin pointé, enveloppés
    dans des edges, curseur et hasNextPage dans pageInfo."""

    CONFIG = {
        "type": "cursor",
        "cursor_param": "after",
        "cursor_field": "data.orders.pageInfo.endCursor",
        "has_more_field": "data.orders.pageInfo.hasNextPage",
        "items_field": "data.orders.edges",
        "record_field": "node",
        "limit": 250,
        "limit_param": "first",
    }

    @staticmethod
    def page(ids, end_cursor, has_next):
        return {"data": {"orders": {
            "edges": [{"cursor": f"c{i}", "node": {"id": i}} for i in ids],
            "pageInfo": {"endCursor": end_cursor, "hasNextPage": has_next},
        }}}

    def test_follows_the_cursor_until_has_next_page_is_false(self):
        fetch = make_fetch([
            self.page([1, 2], "c2", True),
            self.page([3], "c3", False),
        ])
        pages = list(paginate(fetch, "http://api/gql", {}, self.CONFIG))

        assert pages == [[{"id": 1}, {"id": 2}], [{"id": 3}]]
        # première requête sans curseur, suivante avec celui de la réponse
        assert [c[1] for c in fetch.calls] == [
            {"first": 250},
            {"first": 250, "after": "c2"},
        ]

    def test_preserves_base_params(self):
        fetch = make_fetch([self.page([1], "c1", False)])
        list(paginate(fetch, "http://api/gql", {"query": "x"}, self.CONFIG))
        assert fetch.calls[0][1] == {"query": "x", "first": 250}

    def test_empty_page_does_not_end_the_run_when_has_next_page_is_true(self):
        """Une connexion très filtrée peut rendre une page vide au milieu."""
        fetch = make_fetch([
            self.page([], "c0", True),
            self.page([7], "c7", False),
        ])
        pages = list(paginate(fetch, "http://api/gql", {}, self.CONFIG))
        assert pages == [[{"id": 7}]]
        assert len(fetch.calls) == 2

    def test_without_has_more_field_stops_on_empty_page(self):
        config = {k: v for k, v in self.CONFIG.items() if k != "has_more_field"}
        fetch = make_fetch([
            self.page([1], "c1", True),
            self.page([], None, True),
        ])
        pages = list(paginate(fetch, "http://api/gql", {}, config))
        assert pages == [[{"id": 1}]]

    def test_without_has_more_field_stops_when_cursor_is_null(self):
        config = {k: v for k, v in self.CONFIG.items() if k != "has_more_field"}
        fetch = make_fetch([self.page([1], None, True)])
        pages = list(paginate(fetch, "http://api/gql", {}, config))
        assert pages == [[{"id": 1}]]
        assert len(fetch.calls) == 1

    def test_missing_has_more_field_raises(self):
        fetch = make_fetch([{"data": {"orders": {"edges": [], "pageInfo": {}}}}])
        with pytest.raises(PaginationError, match="hasNextPage"):
            list(paginate(fetch, "http://api/gql", {}, self.CONFIG))

    def test_announced_next_page_without_cursor_raises(self):
        """Tronquer ici passerait pour un succès partiel silencieux."""
        payload = {"data": {"orders": {
            "edges": [{"node": {"id": 1}}],
            "pageInfo": {"hasNextPage": True},
        }}}
        fetch = make_fetch([payload])
        with pytest.raises(PaginationError, match="annonce une page suivante"):
            list(paginate(fetch, "http://api/gql", {}, self.CONFIG))

    def test_a_stalled_cursor_raises_instead_of_looping(self):
        fetch = make_fetch([
            self.page([1], "c1", True),
            self.page([1], "c1", True),
        ])
        with pytest.raises(PaginationError, match="ne progresse pas"):
            list(paginate(fetch, "http://api/gql", {}, self.CONFIG))

    def test_limit_is_optional(self):
        config = {
            "type": "cursor",
            "cursor_param": "after",
            "cursor_field": "next_cursor",
            "items_field": "items",
        }
        fetch = make_fetch([{"items": [{"id": 1}], "next_cursor": None}])
        list(paginate(fetch, "http://api/x", {}, config))
        assert fetch.calls[0][1] == {}

    def test_missing_cursor_keys_raise(self):
        with pytest.raises(PaginationError, match="cursor_param"):
            list(paginate(make_fetch([]), "http://api/x", {}, {"type": "cursor"}))


class TestStrategySelection:
    def test_no_pagination_single_call(self):
        fetch = make_fetch([[{"id": 1}]])
        pages = list(paginate(fetch, "http://api/x", {}, None))
        assert pages == [[{"id": 1}]]
        assert len(fetch.calls) == 1

    def test_unknown_type_raises(self):
        with pytest.raises(PaginationError):
            list(paginate(make_fetch([]), "http://api/x", {}, {"type": "zigzag"}))

    def test_single_call_honors_items_field(self):
        """Sans pagination, une réponse GraphQL reste imbriquée."""
        payload = {"data": {"shop": {"products": {"edges": [{"node": {"id": 1}}]}}}}
        fetch = make_fetch([payload])
        config = {
            "type": "none",
            "items_field": "data.shop.products.edges",
            "record_field": "node",
        }
        pages = list(paginate(fetch, "http://api/gql", {}, config))
        assert pages == [[{"id": 1}]]


class TestKeysetPagination:
    """Seule stratégie qui atteint le fond d'une table de plusieurs millions
    de lignes sur les APIs qui plafonnent l'offset."""

    CONFIG = {
        "type": "keyset",
        "key_field": "id",
        "key_param": "since_id",
        "limit": 2,
    }

    def test_the_key_of_the_last_record_drives_the_next_page(self):
        fetch = make_fetch([
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
            [{"id": 5}],
        ])
        pages = list(paginate(fetch, "https://api/items", {}, self.CONFIG))

        assert [len(p) for p in pages] == [2, 2, 1]
        sent = [params for _, params in fetch.calls]
        # la première requête part sans clé
        assert "since_id" not in sent[0]
        assert sent[1]["since_id"] == "2"
        assert sent[2]["since_id"] == "4"
        assert all(p["limit"] == 2 for p in sent)

    def test_an_initial_value_seeds_the_first_page(self):
        fetch = make_fetch([[{"id": 11}]])
        list(paginate(
            fetch, "https://api/items", {}, {**self.CONFIG, "initial_value": 10}
        ))
        assert fetch.calls[0][1]["since_id"] == "10"

    def test_a_partial_page_ends_the_pagination(self):
        fetch = make_fetch([[{"id": 1}]])
        pages = list(paginate(fetch, "https://api/items", {}, self.CONFIG))
        assert len(pages) == 1
        assert len(fetch.calls) == 1

    def test_an_empty_page_ends_the_pagination(self):
        fetch = make_fetch([[]])
        assert list(paginate(fetch, "https://api/items", {}, self.CONFIG)) == []

    def test_without_a_limit_it_stops_on_an_empty_page(self):
        config = {k: v for k, v in self.CONFIG.items() if k != "limit"}
        fetch = make_fetch([[{"id": 1}], [{"id": 2}], []])
        pages = list(paginate(fetch, "https://api/items", {}, config))
        assert [len(p) for p in pages] == [1, 1]

    def test_a_key_that_does_not_advance_stops_instead_of_looping(self):
        fetch = make_fetch([
            [{"id": 1}, {"id": 5}],
            [{"id": 9}, {"id": 5}],
        ])
        with pytest.raises(PaginationError, match="n'avance pas"):
            list(paginate(fetch, "https://api/items", {}, self.CONFIG))

    def test_a_missing_key_in_the_last_record_raises(self):
        fetch = make_fetch([[{"id": 1}, {"other": 2}]])
        with pytest.raises(PaginationError, match="key_field|'id'"):
            list(paginate(fetch, "https://api/items", {}, self.CONFIG))

    def test_key_field_and_key_param_are_required(self):
        with pytest.raises(PaginationError, match="key_field"):
            list(paginate(make_fetch([]), "https://api", {}, {"type": "keyset"}))

    def test_a_dotted_key_field_is_supported(self):
        config = {**self.CONFIG, "key_field": "meta.cursor_id", "limit": 1}
        fetch = make_fetch([[{"meta": {"cursor_id": 7}}], []])
        list(paginate(fetch, "https://api/items", {}, config))
        assert fetch.calls[1][1]["since_id"] == "7"

    def test_a_hostile_key_is_rejected_before_being_sent(self):
        config = {**self.CONFIG, "value_format": "numeric"}
        fetch = make_fetch([[{"id": 1}, {"id": "1 OR 1=1"}]])
        with pytest.raises(Exception, match="numeric|interdit"):
            list(paginate(fetch, "https://api/items", {}, config))


class TestPaginationBounds:
    OFFSET = {"type": "offset", "limit": 2}

    def test_max_pages_stops_the_run_instead_of_truncating(self):
        fetch = make_fetch([[{"id": 1}, {"id": 2}], [{"id": 3}, {"id": 4}]])
        pages = paginate(
            fetch, "https://api/items", {}, {**self.OFFSET, "max_pages": 2}
        )
        collected = []
        with pytest.raises(PaginationError, match="max_pages"):
            for page in pages:
                collected.append(page)
        # les pages lues sont bien livrées avant l'erreur : elles sont écrites
        assert [len(p) for p in collected] == [2, 2]

    def test_max_rows_stops_the_run(self):
        fetch = make_fetch([[{"id": 1}, {"id": 2}], [{"id": 3}, {"id": 4}]])
        with pytest.raises(PaginationError, match="max_rows"):
            list(paginate(
                fetch, "https://api/items", {}, {**self.OFFSET, "max_rows": 3}
            ))

    def test_a_run_under_the_bounds_is_untouched(self):
        fetch = make_fetch([[{"id": 1}, {"id": 2}], [{"id": 3}]])
        pages = list(paginate(
            fetch,
            "https://api/items",
            {},
            {**self.OFFSET, "max_pages": 10, "max_rows": 100},
        ))
        assert [len(p) for p in pages] == [2, 1]

    def test_an_api_serving_the_same_page_forever_is_stopped(self):
        """Une API qui reclampe un numéro de page hors limite et resert la
        première n'a aucune condition d'arrêt naturelle."""
        page = [{"id": 1}, {"id": 2}]
        fetch = make_fetch([list(page) for _ in range(50)])
        with pytest.raises(PaginationError, match="identique à la précédente"):
            list(paginate(
                fetch, "https://api/items", {}, {"type": "page", "page_size": 2}
            ))

    def test_two_different_pages_are_not_mistaken_for_a_loop(self):
        fetch = make_fetch([
            [{"id": 1}, {"id": 2}], [{"id": 3}, {"id": 4}], [{"id": 5}],
        ])
        pages = list(paginate(fetch, "https://api/items", {}, self.OFFSET))
        assert [len(p) for p in pages] == [2, 2, 1]
