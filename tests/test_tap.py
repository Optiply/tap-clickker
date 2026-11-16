"""Credential-free tests: every HTTP call goes through a fake transport."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_clickker.client import ClickkerStream
from tap_clickker.tap import TapClickker

API = "https://clickker.test"
CONFIG = {"api_url": API, "api_key": "test-key", "administration": "001", "start_date": "2024-01-01T00:00:00Z"}


class FakeAPI:
    """Routes requests by path to queued responses and records every call."""

    def __init__(self) -> None:
        self.routes: dict[str, list] = {}
        self.calls: list[requests.PreparedRequest] = []

    def add(self, path: str, *responses) -> None:
        self.routes[path] = list(responses)

    def send(self, adapter, request, **kwargs):
        self.calls.append(request)
        path = urlparse(request.url).path
        queue = self.routes.get(path)
        if not queue:
            raise AssertionError(f"Unexpected request {request.method} {request.url}")
        entry = queue.pop(0) if len(queue) > 1 else queue[0]
        status, body, headers = entry
        response = requests.Response()
        response.status_code = status
        response._content = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        response.headers.update(headers)
        response.url = request.url
        response.request = request
        response.reason = "fake"
        return response

    def calls_to(self, path: str) -> list[requests.PreparedRequest]:
        return [c for c in self.calls if urlparse(c.url).path == path]


def query(request: requests.PreparedRequest) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}


def ok(body) -> tuple:
    return (200, body, {})


@pytest.fixture
def api(monkeypatch):
    fake = FakeAPI()
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", lambda adapter, request, **kw: fake.send(adapter, request, **kw))
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    fake.sleeps = sleeps
    return fake


@pytest.fixture
def small_pages(monkeypatch):
    monkeypatch.setattr(ClickkerStream, "page_size", 2)


def make_tap(streams: set[str], state: dict | None = None) -> TapClickker:
    catalog = TapClickker(config=CONFIG).catalog_dict
    for entry in catalog["streams"]:
        for md in entry["metadata"]:
            if md["breadcrumb"] == []:
                md["metadata"]["selected"] = entry["tap_stream_id"] in streams
    return TapClickker(config=CONFIG, catalog=catalog, state=state or {})


def sync(tap: TapClickker, out: io.StringIO) -> list[dict]:
    with redirect_stdout(out):
        tap.sync_all()
    return parse_messages(out)


def parse_messages(out: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]


def run_tap(streams: set[str], state: dict | None = None) -> list[dict]:
    return sync(make_tap(streams, state), io.StringIO())


def records(messages: list[dict], stream: str) -> list[dict]:
    return [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == stream]


def bookmark(messages: list[dict], stream: str) -> str | None:
    state = [m for m in messages if m["type"] == "STATE"][-1]["value"]
    return state["bookmarks"][stream].get("replication_key_value")


def item(code: str, modified: str | None = "2024-03-01T10:00:00") -> dict:
    return {"ItemCode": code, "ModifiedDate": modified}


# Discovery


def test_discovery_exposes_every_stream_with_keys_and_replication():
    streams = {
        s["tap_stream_id"]: (s["key_properties"], s.get("replication_key"), s["replication_method"])
        for s in TapClickker(config=CONFIG).catalog_dict["streams"]
    }
    assert streams == {
        "items": (["ItemCode"], "ModifiedDate", "INCREMENTAL"),
        "warehouses": (["Code"], None, "FULL_TABLE"),
        "stock": (["Warehouse", "Itemcode"], None, "FULL_TABLE"),
        "suppliers": (["ID"], "ModifiedDate", "INCREMENTAL"),
        "supplier_products": (["ID"], "ModifiedDate", "INCREMENTAL"),
        "sales_orders": (["SalesOrderNumber"], "ChangeDate", "INCREMENTAL"),
        "purchase_orders": (["PurchaseOrderNumber"], "ModifiedDate", "INCREMENTAL"),
        "bill_of_materials": (["ID"], "ModifiedDate", "INCREMENTAL"),
    }


# Auth and HTTP


def test_key_and_administration_headers_are_sent(api):
    api.add("/api/MasterData/Warehouse", ok([{"Code": "1   "}]))
    run_tap({"warehouses"})
    call = api.calls_to("/api/MasterData/Warehouse")[0]
    assert call.headers["X-ApiKey"] == "test-key"
    assert call.headers["administration"] == "001"


def test_unknown_key_403_is_invalid_credentials_and_not_retried(api):
    api.add("/api/MasterData/Warehouse", (403, "X-ApiKey Unknown or not present", {}))
    with pytest.raises(InvalidCredentialsError):
        run_tap({"warehouses"})
    assert len(api.calls_to("/api/MasterData/Warehouse")) == 1


def test_unknown_administration_fails_without_retry(api):
    body = {"Message": "An error has occurred.", "ExceptionMessage": 'Cannot open database "999" requested by the login.'}
    api.add("/api/MasterData/Warehouse", (500, body, {}))
    with pytest.raises(FatalAPIError, match="administration"):
        run_tap({"warehouses"})
    assert len(api.calls_to("/api/MasterData/Warehouse")) == 1


def test_400_is_fatal_with_response_detail(api):
    api.add("/api/MasterData/Warehouse", (400, {"Message": "bad changeDate"}, {}))
    with pytest.raises(FatalAPIError, match="bad changeDate"):
        run_tap({"warehouses"})
    assert len(api.calls_to("/api/MasterData/Warehouse")) == 1


def test_429_honours_bounded_retry_after_then_succeeds(api):
    api.add(
        "/api/MasterData/Warehouse",
        (429, "", {"Retry-After": "7"}),
        (429, "", {"Retry-After": "9999"}),
        ok([{"Code": "1   "}]),
    )
    assert len(records(run_tap({"warehouses"}), "warehouses")) == 1
    assert 7.0 in api.sleeps
    assert 120 in api.sleeps
    assert all(s <= 120 for s in api.sleeps)


def test_persistent_5xx_gives_up_after_bounded_attempts(api):
    api.add("/api/MasterData/Warehouse", (502, "bad gateway", {}))
    with pytest.raises(Exception, match="502"):
        run_tap({"warehouses"})
    assert len(api.calls_to("/api/MasterData/Warehouse")) == 5


def test_non_array_body_is_fatal(api):
    api.add("/api/MasterData/Warehouse", ok({"Message": "unexpected"}))
    with pytest.raises(FatalAPIError, match="Malformed"):
        run_tap({"warehouses"})


# Pagination


def test_pages_advance_by_offset_until_a_short_page(api, small_pages):
    api.add("/api/Item", ok([item("A"), item("B")]), ok([item("C"), item("D")]), ok([item("E")]))
    messages = run_tap({"items"})
    assert [r["ItemCode"] for r in records(messages, "items")] == ["A", "B", "C", "D", "E"]
    params = [query(c) for c in api.calls_to("/api/Item")]
    assert [p.get("skip") for p in params] == [None, "2", "4"]
    assert all(p["take"] == "2" and p["changeDate"] == "2023-12-31T23:00:00" for p in params)


def test_exactly_full_last_page_ends_on_empty_page(api, small_pages):
    api.add("/api/Item", ok([item("A"), item("B")]), ok([]))
    assert len(records(run_tap({"items"}), "items")) == 2
    assert len(api.calls_to("/api/Item")) == 2


def test_repeated_full_page_fails(api, small_pages):
    api.add("/api/Item", ok([item("A"), item("B")]))
    with pytest.raises(FatalAPIError, match="same page twice"):
        run_tap({"items"})


def test_stock_pages_with_start_and_limit_without_filter(api, small_pages):
    rows = [{"Warehouse": "1   ", "Itemcode": c, "Stock": 1.0, "ModifiedDate": None} for c in "ABC"]
    api.add("/api/Stock/Current", ok(rows[:2]), ok(rows[2:]))
    assert len(records(run_tap({"stock"}), "stock")) == 3
    params = [query(c) for c in api.calls_to("/api/Stock/Current")]
    assert params == [{"limit": "2"}, {"limit": "2", "start": "2"}]


# Incremental


def test_first_run_sends_naive_start_date_minus_lookback_and_bookmarks_max_of_unsorted_records(api):
    api.add("/api/Item", ok([item("A", "2024-03-01T10:00:00"), item("B", "2024-05-01T08:00:00.12"), item("C", "2024-02-01T00:00:00")]))
    messages = run_tap({"items"})
    assert query(api.calls_to("/api/Item")[0])["changeDate"] == "2023-12-31T23:00:00"
    assert bookmark(messages, "items") == "2024-05-01T08:00:00.12"


def test_second_run_starts_lookback_before_bookmark(api):
    api.add("/api/Item", ok([]))
    state = {"bookmarks": {"items": {"replication_key": "ModifiedDate", "replication_key_value": "2024-05-01T08:30:00.12"}}}
    messages = run_tap({"items"}, state=state)
    assert query(api.calls_to("/api/Item")[0])["changeDate"] == "2024-05-01T07:30:00"
    assert bookmark(messages, "items") == "2024-05-01T08:30:00.12"


def test_failed_page_does_not_advance_bookmark(api, small_pages):
    api.add("/api/Item", ok([item("A", "2024-09-01T00:00:00"), item("B")]), (400, "boom", {}))
    state = {"bookmarks": {"items": {"replication_key": "ModifiedDate", "replication_key_value": "2024-01-15T00:00:00"}}}
    tap = make_tap({"items"}, state=state)
    out = io.StringIO()
    with pytest.raises(FatalAPIError):
        sync(tap, out)
    assert [m for m in parse_messages(out) if m["type"] == "STATE"] == []
    assert tap.state["bookmarks"]["items"]["replication_key_value"] == "2024-01-15T00:00:00"


def test_record_without_replication_value_fails_closed(api):
    api.add("/api/Item", ok([item("A", None)]))
    with pytest.raises(FatalAPIError, match="has no ModifiedDate"):
        run_tap({"items"})


def test_suppliers_filter_on_supplier_accounts(api):
    api.add("/api/Account", ok([{"ID": "g1", "CustomerType": "S", "ModifiedDate": "2024-02-01T00:00:00"}]))
    run_tap({"suppliers"})
    params = query(api.calls_to("/api/Account")[0])
    assert params["customerType"] == "S"
    assert params["changeDate"] == "2023-12-31T23:00:00"


# Documents


def test_sales_order_is_flattened_and_change_date_covers_lines(api):
    order = {
        "Header": {"SalesOrderNumber": "   20000", "ModifiedDate": "2024-04-14T12:04:29"},
        "Lines": [
            {"ID": 6, "ItemCode": "A", "Quantity": 2.0, "ModifiedDate": "2024-09-27T14:25:53"},
            {"ID": 9, "ItemCode": "B", "Quantity": 1.0, "ModifiedDate": "2024-09-27T14:25:53.5"},
        ],
    }
    api.add("/api/SalesOrder", ok([order]))
    messages = run_tap({"sales_orders"})
    [record] = records(messages, "sales_orders")
    assert record["SalesOrderNumber"] == "   20000"
    assert record["ModifiedDate"] == "2024-04-14T12:04:29"
    assert [line["ID"] for line in record["Lines"]] == [6, 9]
    assert record["ChangeDate"] == "2024-09-27T14:25:53.5"
    assert bookmark(messages, "sales_orders") == "2024-09-27T14:25:53.5"
    assert query(api.calls_to("/api/SalesOrder")[0])["modified"] == "2023-12-31T23:00:00"


def test_document_without_header_is_fatal(api):
    api.add("/api/PurchaseOrder", ok([{"Lines": []}]))
    with pytest.raises(FatalAPIError, match="without Header"):
        run_tap({"purchase_orders"})
