import pytest

from flume_lib.pagination import PaginationError, extract_records, paginate


def make_fetch(pages_by_call):
    calls = []

    def fetch_page(url, params):
        calls.append((url, dict(params)))
        return pages_by_call[len(calls) - 1]

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


class TestStrategySelection:
    def test_no_pagination_single_call(self):
        fetch = make_fetch([[{"id": 1}]])
        pages = list(paginate(fetch, "http://api/x", {}, None))
        assert pages == [[{"id": 1}]]
        assert len(fetch.calls) == 1

    def test_unknown_type_raises(self):
        with pytest.raises(PaginationError):
            list(paginate(make_fetch([]), "http://api/x", {}, {"type": "zigzag"}))

    def test_cursor_is_stub(self):
        with pytest.raises(NotImplementedError):
            list(paginate(make_fetch([]), "http://api/x", {}, {"type": "cursor"}))
