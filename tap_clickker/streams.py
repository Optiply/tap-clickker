"""Clickker stream definitions."""

from __future__ import annotations

from typing import Any

from tap_clickker.client import (
    ClickkerStream,
    IncrementalClickkerStream,
    OrderClickkerStream,
    latest_timestamp,
)


class ItemsStream(IncrementalClickkerStream):
    name = "items"
    path = "/api/Item"
    primary_keys = ["ItemCode"]


class WarehousesStream(ClickkerStream):
    """A handful of rows; read in full so blocked warehouses are always current."""

    name = "warehouses"
    path = "/api/MasterData/Warehouse"
    primary_keys = ["Code"]


class StockStream(ClickkerStream):
    """Current stock per warehouse and item, read in full every run.

    Its `changeDate` filter applies to the item, not the stock level, so it would
    miss stock mutations. `ModifiedDate` can be null.
    """

    name = "stock"
    path = "/api/Stock/Current"
    primary_keys = ["Warehouse", "Itemcode"]
    offset_param = "start"
    limit_param = "limit"


class SuppliersStream(IncrementalClickkerStream):
    """Accounts of type supplier (`customerType=S`)."""

    name = "suppliers"
    path = "/api/Account"
    primary_keys = ["ID"]

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        return {"customerType": "S", **super().get_filter_params(context)}


class SupplierProductsStream(IncrementalClickkerStream):
    """Item-supplier links (`ItemAccount`): purchase price, lead time, main supplier."""

    name = "supplier_products"
    path = "/api/ItemAccount"
    primary_keys = ["ID"]


class SalesOrdersStream(OrderClickkerStream):
    """Open and completed sales orders with their lines.

    The `modified` filter matches the header or any line, so the bookmark is
    `ChangeDate`: the latest of the header's and lines' `ModifiedDate`.
    """

    name = "sales_orders"
    path = "/api/SalesOrder"
    primary_keys = ["SalesOrderNumber"]
    replication_key = "ChangeDate"

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["ChangeDate"] = latest_timestamp(
            [row.get("ModifiedDate"), *(line.get("ModifiedDate") for line in row["Lines"])]
        )
        return super().post_process(row, context)


class PurchaseOrdersStream(OrderClickkerStream):
    """Purchase orders with their lines (lines carry no `ModifiedDate`)."""

    name = "purchase_orders"
    path = "/api/PurchaseOrder"
    primary_keys = ["PurchaseOrderNumber"]


class BillOfMaterialsStream(OrderClickkerStream):
    """Bills of material (compositions): header per produced item version, lines per component."""

    name = "bill_of_materials"
    path = "/api/ProductionOrder/BillOfMaterial"
    primary_keys = ["ID"]


STREAM_TYPES = [
    ItemsStream,
    WarehousesStream,
    StockStream,
    SuppliersStream,
    SupplierProductsStream,
    SalesOrdersStream,
    PurchaseOrdersStream,
    BillOfMaterialsStream,
]
