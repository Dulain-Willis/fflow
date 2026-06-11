# tests/unit/test_rest_source.py
#
# RestSource unit tests — requests.Session mocked.

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from fflow.common.schema import ColumnType, IncrementalConfig
from fflow.sources.rest import (
    APIKeyAuth,
    BearerTokenAuth,
    HeaderLinkPaginator,
    HttpBasicAuth,
    JSONLinkPaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
    OAuth2ClientCredentials,
    PageNumberPaginator,
    RestConnectionConfig,
    RestSource,
    RestStreamConfig,
    SinglePagePaginator,
    _get_nested,
    _parse_link_header,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conn(base_url: str = "https://api.example.com") -> RestConnectionConfig:
    return RestConnectionConfig(base_url=base_url)


def _single_stream(endpoint: str = "/users", **kwargs) -> RestStreamConfig:
    return RestStreamConfig(endpoint=endpoint, **kwargs)


def _mock_session(*pages):
    """Return a mock Session that returns *pages* on successive GET calls."""
    session = MagicMock()
    responses = []
    for page in pages:
        resp = MagicMock()
        resp.json.return_value = page
        resp.raise_for_status.return_value = None
        resp.headers = {}
        responses.append(resp)
    session.get.side_effect = responses
    return session


# ---------------------------------------------------------------------------
# _get_nested helper
# ---------------------------------------------------------------------------


class TestGetNested:
    def test_simple_key(self):
        assert _get_nested({"a": 1}, "a") == 1

    def test_nested_key(self):
        assert _get_nested({"a": {"b": 2}}, "a.b") == 2

    def test_missing_key_returns_none(self):
        assert _get_nested({"a": 1}, "b") is None

    def test_non_dict_in_path_returns_none(self):
        assert _get_nested({"a": [1, 2]}, "a.b") is None


# ---------------------------------------------------------------------------
# _parse_link_header helper
# ---------------------------------------------------------------------------


class TestParseLinkHeader:
    def test_parses_next_url(self):
        header = '<https://api.example.com/users?page=2>; rel="next"'
        assert _parse_link_header(header) == "https://api.example.com/users?page=2"

    def test_returns_none_when_no_next(self):
        header = '<https://api.example.com/users?page=1>; rel="prev"'
        assert _parse_link_header(header) is None

    def test_multiple_rels(self):
        header = '<url1>; rel="prev", <url2>; rel="next"'
        assert _parse_link_header(header) == "url2"


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


class TestCheck:
    def test_check_uses_first_stream_endpoint(self):
        conn = _conn()
        src = RestSource(conn, {"users": _single_stream("/users")})
        mock_sess = _mock_session({})
        with patch.object(src, "_build_session", return_value=mock_sess):
            src.check()
        mock_sess.get.assert_called_once()
        url = mock_sess.get.call_args[0][0]
        assert "users" in url

    def test_check_uses_healthcheck_url_if_set(self):
        conn = RestConnectionConfig(
            base_url="https://api.example.com",
            healthcheck_url="https://api.example.com/health",
        )
        src = RestSource(conn, {"users": _single_stream("/users")})
        mock_sess = _mock_session({})
        with patch.object(src, "_build_session", return_value=mock_sess):
            src.check()
        url = mock_sess.get.call_args[0][0]
        assert url == "https://api.example.com/health"

    def test_check_raises_on_http_error(self):
        src = RestSource(_conn(), {"users": _single_stream("/users")})
        mock_sess = MagicMock()
        resp = MagicMock()
        resp.raise_for_status.side_effect = Exception("404")
        mock_sess.get.return_value = resp
        with patch.object(src, "_build_session", return_value=mock_sess):
            with pytest.raises(Exception, match="404"):
                src.check()


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------


class TestDiscover:
    def test_infers_columns_from_first_page(self):
        src = RestSource(_conn(), {"users": _single_stream("/users")})
        page = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_sess = _mock_session(page)
        with patch.object(src, "_build_session", return_value=mock_sess):
            schema = src.discover()

        stream = schema.get_stream("users")
        col_names = [c.name for c in stream.columns]
        assert "id" in col_names
        assert "name" in col_names

    def test_nested_dict_maps_to_json(self):
        src = RestSource(_conn(), {"users": _single_stream("/users")})
        page = [{"id": 1, "meta": {"foo": "bar"}}]
        mock_sess = _mock_session(page)
        with patch.object(src, "_build_session", return_value=mock_sess):
            schema = src.discover()

        meta_col = schema.get_stream("users").get_column("meta")
        assert meta_col.type == ColumnType.json

    def test_discover_graceful_on_error(self):
        src = RestSource(_conn(), {"users": _single_stream("/users")})
        mock_sess = MagicMock()
        mock_sess.get.side_effect = RuntimeError("unreachable")
        with patch.object(src, "_build_session", return_value=mock_sess):
            schema = src.discover()
        assert schema.get_stream("users").columns == []

    def test_discover_with_data_path(self):
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", data_path="data")},
        )
        page = {"data": [{"id": 1}]}
        mock_sess = _mock_session(page)
        with patch.object(src, "_build_session", return_value=mock_sess):
            schema = src.discover()
        assert schema.get_stream("users").get_column("id") is not None


# ---------------------------------------------------------------------------
# read() — single page
# ---------------------------------------------------------------------------


class TestReadSinglePage:
    def test_yields_all_records(self):
        src = RestSource(_conn(), {"users": _single_stream("/users")})
        page = [{"id": 1}, {"id": 2}]
        mock_sess = _mock_session(page)
        with patch.object(src, "_build_session", return_value=mock_sess):
            rows = list(src.read("users", {}))
        assert rows == page

    def test_data_path_extracts_nested(self):
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", data_path="results")},
        )
        page = {"results": [{"id": 10}]}
        mock_sess = _mock_session(page)
        with patch.object(src, "_build_session", return_value=mock_sess):
            rows = list(src.read("users", {}))
        assert rows == [{"id": 10}]

    def test_missing_data_path_raises(self):
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", data_path="missing")},
        )
        page = {"other_key": []}
        mock_sess = _mock_session(page)
        with patch.object(src, "_build_session", return_value=mock_sess):
            with pytest.raises(ValueError, match="missing"):
                list(src.read("users", {}))


# ---------------------------------------------------------------------------
# read() — pagination
# ---------------------------------------------------------------------------


class TestReadPageNumberPaginator:
    def test_fetches_multiple_pages(self):
        paginator = PageNumberPaginator(page_size=2)
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator)},
        )
        mock_sess = _mock_session(
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
            [],  # stop signal
        )
        with patch.object(src, "_build_session", return_value=mock_sess):
            rows = list(src.read("users", {}))
        assert len(rows) == 4

    def test_stops_on_empty_page(self):
        paginator = PageNumberPaginator(page_size=2)
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator)},
        )
        mock_sess = _mock_session([{"id": 1}], [])
        with patch.object(src, "_build_session", return_value=mock_sess):
            rows = list(src.read("users", {}))
        assert len(rows) == 1


class TestReadOffsetPaginator:
    def test_fetches_until_short_page(self):
        paginator = OffsetPaginator(limit=2)
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator)},
        )
        mock_sess = _mock_session([{"id": 1}, {"id": 2}], [{"id": 3}])
        with patch.object(src, "_build_session", return_value=mock_sess):
            rows = list(src.read("users", {}))
        assert len(rows) == 3


class TestReadJSONLinkPaginator:
    def test_follows_next_link(self):
        paginator = JSONLinkPaginator(next_url_path="next")
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator)},
        )
        page1 = {"items": [{"id": 1}], "next": "https://api.example.com/users?cursor=abc"}
        page2 = {"items": [{"id": 2}], "next": None}
        mock_sess = MagicMock()
        resp1 = MagicMock()
        resp1.json.return_value = page1
        resp1.raise_for_status.return_value = None
        resp2 = MagicMock()
        resp2.json.return_value = page2
        resp2.raise_for_status.return_value = None
        mock_sess.get.side_effect = [resp1, resp2]

        src2 = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator, data_path="items")},
        )
        with patch.object(src2, "_build_session", return_value=mock_sess):
            rows = list(src2.read("users", {}))
        assert len(rows) == 2

    def test_loop_detection_stops_pagination(self):
        paginator = JSONLinkPaginator(next_url_path="next")
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator)},
        )
        looping_page = [{"id": 1}]
        resp = MagicMock()
        resp.json.return_value = {"items": looping_page, "next": "https://api.example.com/users"}
        resp.raise_for_status.return_value = None
        mock_sess = MagicMock()
        mock_sess.get.return_value = resp

        src2 = RestSource(
            _conn(),
            {"users": RestStreamConfig(
                endpoint="/users",
                paginator=paginator,
                data_path="items",
                max_pages=5,
            )},
        )
        with patch.object(src2, "_build_session", return_value=mock_sess):
            rows = list(src2.read("users", {}))
        # Loop detected — stops after first page
        assert len(rows) == 1


class TestReadCursorPaginator:
    def test_follows_cursor(self):
        paginator = JSONResponseCursorPaginator(cursor_path="meta.next_cursor", cursor_param="cursor")
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator)},
        )
        page1 = [{"id": 1}, {"id": 2}]
        page2 = [{"id": 3}]
        resp1 = MagicMock()
        resp1.json.return_value = {"data": page1, "meta": {"next_cursor": "tok123"}}
        resp1.raise_for_status.return_value = None
        resp2 = MagicMock()
        resp2.json.return_value = {"data": page2, "meta": {"next_cursor": None}}
        resp2.raise_for_status.return_value = None
        mock_sess = MagicMock()
        mock_sess.get.side_effect = [resp1, resp2]

        src2 = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", paginator=paginator, data_path="data")},
        )
        with patch.object(src2, "_build_session", return_value=mock_sess):
            rows = list(src2.read("users", {}))
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# Incremental cursor tracking
# ---------------------------------------------------------------------------


class TestIncrementalCursor:
    def test_state_updated_with_max_cursor(self):
        inc = IncrementalConfig(cursor_type="integer", cursor_field="id")
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(endpoint="/users", incremental=inc)},
        )
        mock_sess = _mock_session([{"id": 3}, {"id": 7}, {"id": 5}])
        state = {}
        with patch.object(src, "_build_session", return_value=mock_sess):
            list(src.read("users", state))
        assert state["cursor_value"] == 7

    def test_cursor_param_sent_as_query_param(self):
        inc = IncrementalConfig(cursor_type="integer", cursor_field="id")
        src = RestSource(
            _conn(),
            {"users": RestStreamConfig(
                endpoint="/users",
                incremental=inc,
                cursor_param="since_id",
            )},
        )
        mock_sess = _mock_session([{"id": 10}])
        state = {"cursor_value": 5}
        with patch.object(src, "_build_session", return_value=mock_sess):
            list(src.read("users", state))

        call_kwargs = mock_sess.get.call_args[1]
        assert call_kwargs.get("params", {}).get("since_id") == 5


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class TestAuth:
    def test_bearer_sets_header(self):
        auth = BearerTokenAuth(token="secret")
        conn = RestConnectionConfig(base_url="https://api.example.com", auth=auth)
        src = RestSource(conn, {})
        session = src._build_session()
        assert session.headers["Authorization"] == "Bearer secret"

    def test_api_key_header(self):
        auth = APIKeyAuth(name="X-API-Key", value="abc", location="header")
        conn = RestConnectionConfig(base_url="https://api.example.com", auth=auth)
        src = RestSource(conn, {})
        session = src._build_session()
        assert session.headers["X-API-Key"] == "abc"

    def test_api_key_query(self):
        auth = APIKeyAuth(name="api_key", value="abc", location="query")
        conn = RestConnectionConfig(base_url="https://api.example.com", auth=auth)
        src = RestSource(conn, {})
        session = src._build_session()
        assert session.params["api_key"] == "abc"

    def test_basic_auth(self):
        auth = HttpBasicAuth(username="user", password="pass")
        conn = RestConnectionConfig(base_url="https://api.example.com", auth=auth)
        src = RestSource(conn, {})
        session = src._build_session()
        assert session.auth == ("user", "pass")

    def test_oauth2_fetches_token(self):
        auth = OAuth2ClientCredentials(
            client_id="cid",
            client_secret="csec",
            token_url="https://auth.example.com/token",
        )
        conn = RestConnectionConfig(base_url="https://api.example.com", auth=auth)
        src = RestSource(conn, {})
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok_xyz"}
        mock_resp.raise_for_status.return_value = None
        with patch("fflow.sources.rest.requests.post", return_value=mock_resp):
            session = src._build_session()
        assert "Bearer tok_xyz" in session.headers["Authorization"]
