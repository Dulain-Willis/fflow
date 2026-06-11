"""REST API source connector.

Reads from any HTTP/JSON API with configurable auth, pagination, and
incremental cursor tracking.

Auth types: BearerToken, APIKey (header or query), HttpBasic, OAuth2ClientCredentials.
Paginator types: SinglePage, PageNumber, Offset, JSONLink, HeaderLink, JSONResponseCursor.

Incremental cursors:
- ``cursor_field`` — field name in returned records (watermark tracking).
- ``cursor_param`` — request query param name to pass to API (often different from field name).
  e.g. cursor_field="updated_at", cursor_param="updated_since".
- State key: ``cursor_value``.

``discover()`` fetches the first page of each stream and unions column names
across all records.  Nested objects/arrays → ``ColumnType.json``; all others
→ ``ColumnType.unknown`` (lossy-safe until a real type-inference pass).
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Iterator, Literal, Optional, Union

import requests
from pydantic import BaseModel, Field

from fflow.common.config import StreamConfig
from fflow.common.schema import Column, ColumnType, IncrementalConfig, Schema, Stream

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30
_DEFAULT_MAX_PAGES = 10_000


# ---------------------------------------------------------------------------
# Auth models
# ---------------------------------------------------------------------------

class BearerTokenAuth(BaseModel):
    type: Literal["bearer"] = "bearer"
    token: str


class APIKeyAuth(BaseModel):
    type: Literal["api_key"] = "api_key"
    location: Literal["header", "query"] = "header"
    name: str   # header name or query param name
    value: str


class HttpBasicAuth(BaseModel):
    type: Literal["basic"] = "basic"
    username: str
    password: str


class OAuth2ClientCredentials(BaseModel):
    type: Literal["oauth2"] = "oauth2"
    client_id: str
    client_secret: str
    token_url: str
    scopes: list[str] = []


AuthConfig = Annotated[
    Union[BearerTokenAuth, APIKeyAuth, HttpBasicAuth, OAuth2ClientCredentials],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Paginator models
# ---------------------------------------------------------------------------

class SinglePagePaginator(BaseModel):
    type: Literal["single_page"] = "single_page"


class PageNumberPaginator(BaseModel):
    type: Literal["page_number"] = "page_number"
    page_param: str = "page"
    start_page: int = 1
    page_size_param: str = "per_page"
    page_size: int = 100
    total_path: Optional[str] = None  # dot-path to total count in response


class OffsetPaginator(BaseModel):
    type: Literal["offset"] = "offset"
    offset_param: str = "offset"
    limit_param: str = "limit"
    limit: int = 100
    total_path: Optional[str] = None


class JSONLinkPaginator(BaseModel):
    type: Literal["json_link"] = "json_link"
    next_url_path: str = "next"  # dot-path to next page URL in response body


class HeaderLinkPaginator(BaseModel):
    type: Literal["header_link"] = "header_link"
    # Reads Link: <url>; rel="next" HTTP header


class JSONResponseCursorPaginator(BaseModel):
    type: Literal["json_cursor"] = "json_cursor"
    cursor_path: str              # dot-path to cursor in response body
    cursor_param: str = "cursor"  # query param name sent to API


PaginatorConfig = Annotated[
    Union[
        SinglePagePaginator,
        PageNumberPaginator,
        OffsetPaginator,
        JSONLinkPaginator,
        HeaderLinkPaginator,
        JSONResponseCursorPaginator,
    ],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Connection and stream config
# ---------------------------------------------------------------------------

class RestConnectionConfig(BaseModel):
    base_url: str
    auth: Optional[AuthConfig] = None
    healthcheck_url: Optional[str] = None  # override for check(); defaults to first stream endpoint
    timeout: int = _DEFAULT_TIMEOUT


class RestStreamConfig(StreamConfig):
    """Per-stream config for a REST source.

    Inherits pipeline-level fields (``name``, ``write_disposition``,
    ``merge_key``, ``hash_fields``) from :class:`~fflow.common.config.StreamConfig`.
    Adds REST-specific fetch config.
    """
    endpoint: str                           # path relative to base_url
    paginator: PaginatorConfig = Field(default_factory=SinglePagePaginator)
    incremental: IncrementalConfig = IncrementalConfig()
    cursor_param: Optional[str] = None      # request param name (if different from cursor_field)
    data_path: Optional[str] = None        # dot-notation path to records list in response
    params: dict = Field(default_factory=dict)  # static query params
    chunk_size: int = 1000
    max_pages: int = _DEFAULT_MAX_PAGES


# ---------------------------------------------------------------------------
# RestSource
# ---------------------------------------------------------------------------

class RestSource:
    """Source connector for HTTP/JSON REST APIs.

    Parameters
    ----------
    connection:
        Base URL, auth, and request settings.
    streams:
        Per-stream extraction config keyed by stream name.  Optional when
        using the ``@pipeline`` / ``@stream`` decorator API — streams are
        registered via :meth:`configure_streams` after construction.
    """

    def __init__(
        self,
        connection: RestConnectionConfig,
        streams: dict[str, RestStreamConfig] | None = None,
    ) -> None:
        self._conn_cfg = connection
        self._stream_cfgs: dict[str, RestStreamConfig] = streams or {}

    def configure_streams(self, configs: list[RestStreamConfig]) -> None:
        """Register stream configs collected by the ``@pipeline`` decorator."""
        self._stream_cfgs = {cfg.name: cfg for cfg in configs}

    # ------------------------------------------------------------------
    # Source Protocol
    # ------------------------------------------------------------------

    def check(self) -> None:
        """Verify connectivity.  Uses healthcheck_url or the first stream endpoint."""
        url = self._conn_cfg.healthcheck_url
        if not url and self._stream_cfgs:
            first_stream = next(iter(self._stream_cfgs.values()))
            url = self._build_url(first_stream.endpoint)
        if not url:
            url = self._conn_cfg.base_url

        session = self._build_session()
        resp = session.get(url, timeout=self._conn_cfg.timeout)
        resp.raise_for_status()

    def discover(self) -> Schema:
        """Infer schema by fetching the first page of each stream."""
        streams: list[Stream] = []
        session = self._build_session()
        for stream_name, cfg in self._stream_cfgs.items():
            try:
                first_page = self._fetch_page(session, cfg, {})
                records = self._extract_records(first_page, cfg.data_path)
                columns = self._infer_columns(records)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "RestSource.discover(): stream '%s' failed (%s) — returning empty schema",
                    stream_name, exc,
                )
                columns = []
            streams.append(
                Stream(name=stream_name, columns=columns, incremental=cfg.incremental)
            )
        return Schema(streams=streams)

    def read(self, stream: str, state: dict) -> Iterator[dict]:
        """Yield records for *stream*, updating *state* in-place."""
        cfg = self._stream_cfgs[stream]
        current_cursor = state.get("cursor_value")

        params = dict(cfg.params)
        cursor_param = cfg.cursor_param or (
            cfg.incremental.cursor_field
            if cfg.incremental.cursor_type != "none"
            else None
        )
        if cursor_param and current_cursor is not None:
            params[cursor_param] = current_cursor

        session = self._build_session()
        max_cursor = current_cursor
        chunk: list[dict] = []

        for records in self._paginate(session, cfg, params):
            for record in records:
                if cfg.incremental.cursor_type != "none" and cfg.incremental.cursor_field:
                    val = record.get(cfg.incremental.cursor_field)
                    if val is not None:
                        if max_cursor is None or self._cursor_gt(val, max_cursor, cfg.incremental.cursor_type):
                            max_cursor = val
                chunk.append(record)
                if len(chunk) >= cfg.chunk_size:
                    yield from chunk
                    chunk.clear()

        if chunk:
            yield from chunk

        if max_cursor is not None and max_cursor != current_cursor:
            state["cursor_value"] = max_cursor

    # ------------------------------------------------------------------
    # Internal: pagination
    # ------------------------------------------------------------------

    def _paginate(
        self,
        session: requests.Session,
        cfg: RestStreamConfig,
        base_params: dict,
    ) -> Iterator[list[dict]]:
        """Yield lists of records, one list per page."""
        paginator = cfg.paginator
        params = dict(base_params)
        pages_fetched = 0

        if isinstance(paginator, SinglePagePaginator):
            response = self._fetch_page(session, cfg, params)
            records = self._extract_records(response, cfg.data_path)
            if records:
                yield records

        elif isinstance(paginator, PageNumberPaginator):
            page = paginator.start_page
            params[paginator.page_size_param] = paginator.page_size
            while pages_fetched < cfg.max_pages:
                params[paginator.page_param] = page
                response = self._fetch_page(session, cfg, params)
                records = self._extract_records(response, cfg.data_path)
                if not records:
                    break
                yield records
                pages_fetched += 1
                page += 1

        elif isinstance(paginator, OffsetPaginator):
            offset = 0
            params[paginator.limit_param] = paginator.limit
            while pages_fetched < cfg.max_pages:
                params[paginator.offset_param] = offset
                response = self._fetch_page(session, cfg, params)
                records = self._extract_records(response, cfg.data_path)
                if not records:
                    break
                yield records
                pages_fetched += 1
                offset += len(records)
                if len(records) < paginator.limit:
                    break

        elif isinstance(paginator, JSONLinkPaginator):
            next_url: Optional[str] = self._build_url(cfg.endpoint)
            seen_urls: set = set()
            while next_url and pages_fetched < cfg.max_pages:
                if next_url in seen_urls:
                    logger.warning("RestSource: pagination loop detected at %s", next_url)
                    break
                seen_urls.add(next_url)
                response = self._fetch_url(session, next_url, params if pages_fetched == 0 else {})
                records = self._extract_records(response, cfg.data_path)
                if records:
                    yield records
                pages_fetched += 1
                next_url = _get_nested(response, paginator.next_url_path)
                params = {}  # next URL already has all params encoded

        elif isinstance(paginator, HeaderLinkPaginator):
            url: Optional[str] = self._build_url(cfg.endpoint)
            seen_urls: set = set()
            while url and pages_fetched < cfg.max_pages:
                if url in seen_urls:
                    logger.warning("RestSource: pagination loop detected at %s", url)
                    break
                seen_urls.add(url)
                resp_obj = session.get(url, params=params if pages_fetched == 0 else None, timeout=self._conn_cfg.timeout)
                resp_obj.raise_for_status()
                response = resp_obj.json()
                records = self._extract_records(response, cfg.data_path)
                if records:
                    yield records
                pages_fetched += 1
                url = _parse_link_header(resp_obj.headers.get("Link", ""))
                params = {}

        elif isinstance(paginator, JSONResponseCursorPaginator):
            seen_cursors: set = set()
            while pages_fetched < cfg.max_pages:
                response = self._fetch_page(session, cfg, params)
                records = self._extract_records(response, cfg.data_path)
                if records:
                    yield records
                pages_fetched += 1
                next_cursor = _get_nested(response, paginator.cursor_path)
                if not next_cursor or next_cursor in seen_cursors:
                    break
                seen_cursors.add(next_cursor)
                params[paginator.cursor_param] = next_cursor

    # ------------------------------------------------------------------
    # Internal: HTTP
    # ------------------------------------------------------------------

    def _fetch_page(
        self, session: requests.Session, cfg: RestStreamConfig, params: dict
    ) -> Any:
        url = self._build_url(cfg.endpoint)
        return self._fetch_url(session, url, params)

    def _fetch_url(
        self, session: requests.Session, url: str, params: dict
    ) -> Any:
        resp = session.get(url, params=params, timeout=self._conn_cfg.timeout)
        resp.raise_for_status()
        return resp.json()

    def _build_url(self, endpoint: str) -> str:
        base = self._conn_cfg.base_url.rstrip("/")
        endpoint = endpoint.lstrip("/")
        return f"{base}/{endpoint}"

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        auth = self._conn_cfg.auth
        if auth is None:
            return session

        if isinstance(auth, BearerTokenAuth):
            session.headers["Authorization"] = f"Bearer {auth.token}"

        elif isinstance(auth, APIKeyAuth):
            if auth.location == "header":
                session.headers[auth.name] = auth.value
            else:
                session.params[auth.name] = auth.value  # type: ignore[assignment]

        elif isinstance(auth, HttpBasicAuth):
            session.auth = (auth.username, auth.password)

        elif isinstance(auth, OAuth2ClientCredentials):
            token = self._fetch_oauth2_token(auth)
            session.headers["Authorization"] = f"Bearer {token}"

        return session

    @staticmethod
    def _fetch_oauth2_token(auth: OAuth2ClientCredentials) -> str:
        data: dict = {
            "grant_type": "client_credentials",
            "client_id": auth.client_id,
            "client_secret": auth.client_secret,
        }
        if auth.scopes:
            data["scope"] = " ".join(auth.scopes)
        resp = requests.post(auth.token_url, data=data, timeout=_DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp.json()["access_token"]

    # ------------------------------------------------------------------
    # Internal: data extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_records(response: Any, data_path: Optional[str]) -> list[dict]:
        """Extract list of records from *response* using dot-notation *data_path*."""
        if data_path:
            node = _get_nested(response, data_path)
            if node is None:
                raise ValueError(
                    f"data_path '{data_path}' not found in response. "
                    f"Available keys: {list(response.keys()) if isinstance(response, dict) else 'n/a'}"
                )
        else:
            node = response

        if isinstance(node, list):
            return node
        raise ValueError(
            f"Expected a list at data_path='{data_path}'; got {type(node).__name__}"
        )

    # Mirrors dlt's PY_TYPE_TO_SC_TYPE (type_helpers.py). bool MUST precede int
    # because bool is a subclass of int in Python — exact type(v) lookup avoids
    # isinstance ambiguity entirely.
    _PY_TYPE_TO_COLUMN_TYPE: dict[type, ColumnType] = {
        bool: ColumnType.boolean,
        int: ColumnType.integer,
        float: ColumnType.float_,
        str: ColumnType.string,
        dict: ColumnType.json,
        list: ColumnType.json,
    }

    @classmethod
    def _infer_columns(cls, records: list[dict]) -> list[Column]:
        """Union column names across all records; infer types from JSON values.

        Uses exact type(v) lookup (dlt pattern) — None values leave the column
        as unknown until a non-None value is seen in a later record.
        """
        all_keys: dict[str, ColumnType] = {}
        for record in records:
            for key, val in record.items():
                existing = all_keys.get(key, ColumnType.unknown)
                if existing != ColumnType.unknown or val is None:
                    continue
                all_keys[key] = cls._PY_TYPE_TO_COLUMN_TYPE.get(type(val), ColumnType.unknown)
        return [Column(name=k, type=t) for k, t in all_keys.items()]

    @staticmethod
    def _cursor_gt(new_val: Any, current: Any, cursor_type: str) -> bool:
        """Return True if *new_val* is greater than *current* for *cursor_type*."""
        try:
            if cursor_type == "integer":
                return int(new_val) > int(current)
            # timestamp or unknown — string comparison (ISO 8601 sorts lexicographically)
            return str(new_val) > str(current)
        except (TypeError, ValueError):
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_nested(obj: Any, path: str) -> Any:
    """Traverse *obj* using dot-notation *path*; return None if path is missing."""
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _parse_link_header(link_header: str) -> Optional[str]:
    """Parse ``Link: <url>; rel="next"`` header; return the next URL or None."""
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part or "rel='next'" in part:
            url_part = part.split(";")[0].strip()
            if url_part.startswith("<") and url_part.endswith(">"):
                return url_part[1:-1]
    return None


# ---------------------------------------------------------------------------
# Decorator-friendly factory
# ---------------------------------------------------------------------------

def rest(
    base_url: str,
    auth: Optional[AuthConfig] = None,
    healthcheck_url: Optional[str] = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> RestSource:
    """Create a :class:`RestSource` for use with the ``@pipeline`` decorator.

    Streams are registered automatically by ``@stream()`` — do not pass them here.

    Example::

        @pipeline(
            source=rest("https://api.example.com", auth=BearerTokenAuth(token=...)),
            destination=...,
        )
        def my_pipeline():
            @stream()
            def records():
                return RestStreamConfig(endpoint="/records.json", ...)
    """
    return RestSource(
        connection=RestConnectionConfig(
            base_url=base_url,
            auth=auth,
            healthcheck_url=healthcheck_url,
            timeout=timeout,
        )
    )
