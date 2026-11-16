"""Shared request, pagination, retry, and incremental logic for Clickker streams."""

from __future__ import annotations

import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import backoff
import requests
from dateutil.parser import isoparse
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / "schemas"
REQUEST_TIMEOUT = (10, 300)
# Documented limit; larger `take`/`limit` values are silently capped to it.
MAX_PAGE_SIZE = 1000
MAX_RETRY_AFTER_SECONDS = 120
ERROR_BODY_LIMIT = 500


class ClickkerStream(RESTStream):
    """Clickker list endpoint: a bare JSON array, paged by offset and limit."""

    records_jsonpath = "$[*]"
    page_size = MAX_PAGE_SIZE
    offset_param = "skip"
    limit_param = "take"
    declared_replication_method = "FULL_TABLE"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.forced_replication_method = self.declared_replication_method
        self._previous_first_record: str | None = None

    @property
    def url_base(self) -> str:
        return self.config["api_url"].rstrip("/")

    @property
    def schema_filepath(self) -> Path:
        return SCHEMAS_DIR / f"{self.name}.json"

    @property
    def http_headers(self) -> dict:
        headers = {"Accept": "application/json", "X-ApiKey": self.config["api_key"]}
        if self.config.get("administration"):
            headers["administration"] = self.config["administration"]
        return headers

    @property
    def timeout(self) -> tuple[int, int]:
        return REQUEST_TIMEOUT

    # Requests and retries

    def validate_response(self, response: requests.Response) -> None:
        status = response.status_code
        # Clickker answers a missing or unknown key with 403 "X-ApiKey Unknown or not present".
        if status == 401 or (status == 403 and "X-ApiKey" in response.text):
            raise InvalidCredentialsError(
                f"Clickker rejected the API key for {self._error_path(response)} ({status})."
            )
        if status == 429:
            self._wait_retry_after(response)
            raise RetriableAPIError(self._error_message(response), response)
        if status >= 500:
            if "Cannot open database" in response.text:
                raise FatalAPIError(
                    f"Clickker cannot open administration {self.config.get('administration')!r}; "
                    "check the `administration` config value."
                )
            raise RetriableAPIError(self._error_message(response), response)
        if status >= 400:
            raise FatalAPIError(self._error_message(response))
        if status != 200:
            raise FatalAPIError(f"Unexpected {status} from Clickker for {self._error_path(response)}")

    def _wait_retry_after(self, response: requests.Response) -> None:
        header = response.headers.get("Retry-After")
        if header is None:
            return
        try:
            seconds = float(header)
        except ValueError:
            return
        time.sleep(min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS))

    @staticmethod
    def _error_path(response: requests.Response) -> str:
        return urlparse(response.url).path

    def _error_message(self, response: requests.Response) -> str:
        return (
            f"{response.status_code} {response.reason} from Clickker for "
            f"{self._error_path(response)}: {response.text[:ERROR_BODY_LIMIT]}"
        )

    def backoff_wait_generator(self):
        return backoff.expo(factor=2, max_value=60)

    def backoff_max_tries(self) -> int:
        return 5

    # Pagination

    def _page(self, response: requests.Response) -> list:
        try:
            body = response.json()
        except ValueError as ex:
            raise FatalAPIError(f"Non-JSON Clickker response for {self._error_path(response)}") from ex
        if not isinstance(body, list):
            raise FatalAPIError(f"Malformed Clickker response for {self._error_path(response)}")
        return body

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from self._page(response)

    def get_next_page_token(self, response: requests.Response, previous_token: Any) -> int | None:
        body = self._page(response)
        if previous_token is None:
            self._previous_first_record = None
        if len(body) < self.page_size:
            return None
        first = json.dumps(body[0], sort_keys=True)
        if first == self._previous_first_record:
            raise FatalAPIError(
                f"Clickker returned the same page twice for stream '{self.name}'; "
                f"`{self.offset_param}` is not advancing."
            )
        self._previous_first_record = first
        return (previous_token or 0) + len(body)

    def get_url_params(self, context: dict | None, next_page_token: Any) -> dict[str, Any]:
        params: dict[str, Any] = {self.limit_param: self.page_size}
        if next_page_token:
            params[self.offset_param] = next_page_token
        params.update(self.get_filter_params(context))
        return params

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        """Per-stream query filters, sent on every page."""
        return {}


class IncrementalClickkerStream(ClickkerStream):
    """Stream filtered server-side with `<filter_param> >= bookmark`.

    Lists are ordered by business key, not by change date, so a record changed while
    the sync pages through the list can sort below the final bookmark. Each run
    therefore starts `lookback_minutes` before the bookmark; re-read records are
    deduplicated downstream by primary key.
    """

    filter_param = "changeDate"
    replication_key = "ModifiedDate"
    declared_replication_method = "INCREMENTAL"

    def get_replication_key_signpost(self, context: dict | None) -> None:
        # The SDK signpost is UTC now; Clickker timestamps are naive local time, so
        # comparing them would corrupt the bookmark. The lookback covers in-sync changes.
        return None

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        if not self.replication_key:  # catalog selected FULL_TABLE: read everything
            return {}
        # The SDK seeds the starting value from `start_date` on a first run.
        start = self.get_starting_replication_key_value(context) or self.config["start_date"]
        lookback = timedelta(minutes=int(self.config.get("lookback_minutes", 60)))
        # Clickker timestamps are naive local time, and it converts a filter carrying an
        # offset (`Z`) to local time, so the filter is always sent naive.
        since = isoparse(start).replace(tzinfo=None) - lookback
        return {self.filter_param: since.strftime("%Y-%m-%dT%H:%M:%S")}

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        if self.replication_key and not row.get(self.replication_key):
            raise FatalAPIError(
                f"Record in stream '{self.name}' has no {self.replication_key}: "
                f"{json.dumps({k: row.get(k) for k in self.primary_keys})}"
            )
        return row


class OrderClickkerStream(IncrementalClickkerStream):
    """Document endpoint returning `{"Header": {...}, "Lines": [...]}`.

    Emitted as the header fields with the lines nested under `Lines`, so the
    primary key and replication key are top-level fields.
    """

    filter_param = "modified"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for document in self._page(response):
            if not isinstance(document, dict) or not isinstance(document.get("Header"), dict):
                raise FatalAPIError(f"Clickker document without Header for stream '{self.name}'")
            yield {**document["Header"], "Lines": document.get("Lines") or []}


def latest_timestamp(values: Iterable[str | None]) -> str | None:
    """The latest of Clickker's naive ISO timestamps (fraction lengths vary)."""
    present = [value for value in values if value]
    return max(present, key=isoparse) if present else None
