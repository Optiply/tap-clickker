# tap-clickker

Singer tap for the [Clickker API](https://demo-api.clickker.nl/swagger/ui/index) (a REST layer over Exact Globe/Synergy), built on `hotglue-singer-sdk`. It extracts the inventory entities Optiply imports: products, stock, suppliers, supplier products, sales orders, purchase orders, and bills of material.

## Config

| Key | Required | Description |
| --- | --- | --- |
| `api_url` | yes | API base URL, e.g. `https://demo-api.clickker.nl`. |
| `api_key` | yes (secret) | Application key, sent as the `X-ApiKey` header. |
| `administration` | no | Exact administration (SQL database name), sent as the `administration` header. Without it Clickker uses the default administration. |
| `start_date` | yes | Lower bound for incremental streams on the first run, read as Clickker local time. |
| `lookback_minutes` | no (default 60) | Minutes before the bookmark (or `start_date`) that each incremental run starts. |

See `config.example.json`.

## Streams

| Stream | Endpoint | Primary key | Replication | Notes |
| --- | --- | --- | --- | --- |
| `items` | `/api/Item` | `ItemCode` | incremental `ModifiedDate` | `changeDate` filter. |
| `warehouses` | `/api/MasterData/Warehouse` | `Code` | full table | A handful of rows. |
| `stock` | `/api/Stock/Current` | `Warehouse`, `Itemcode` | full table | One row per warehouse and item: `Stock`, `FreeStock`, `AvailableStock`, `ToBeDelivered`, `ToBeReceived`. Its `changeDate` filters on the item, not the stock level, so it is not used. Pages with `start`/`limit`. |
| `suppliers` | `/api/Account?customerType=S` | `ID` | incremental `ModifiedDate` | `changeDate` filter. |
| `supplier_products` | `/api/ItemAccount` | `ID` | incremental `ModifiedDate` | `changeDate` filter. Links `ItemCode` to `CreditorNumber`, with `PurchasePrice`, `DeliveryTimeInDays`, `MainAccount`. |
| `sales_orders` | `/api/SalesOrder` | `SalesOrderNumber` | incremental `ChangeDate` | Open and completed. Header fields plus `Lines`. `modified` matches the header or any line, so `ChangeDate` is the latest `ModifiedDate` of the header and its lines. |
| `purchase_orders` | `/api/PurchaseOrder` | `PurchaseOrderNumber` | incremental `ModifiedDate` | Header fields plus `Lines`; `Status` is Authorized, Confirmed, Received or Completed. |
| `bill_of_materials` | `/api/ProductionOrder/BillOfMaterial` | `ID` | incremental `ModifiedDate` | Compositions: header per produced item version, `Lines` per component (`ItemReq`, `Quantity`). |

Values are emitted as Clickker returns them; codes keep their padding (`"   20000"`, `"1   "`).

Not extracted:

- Cancelled sales orders: the API has no status or delete feed for sales orders. An order removed in Exact is no longer returned and is not emitted.
- Deletes for any entity: no delete feed exists.
- Purchase-order receipts: `/api/PurchaseOrder/{n}/Receipt` posts a receipt (a write), it does not list them. Received quantities are on purchase-order lines (`QuantityDelivered`).
- Warehouse-location stock (`/api/Stock/WarehouseLocation`): `stock` already gives the warehouse totals.

## Behavior

- **Pagination:** `skip`/`take` (`start`/`limit` for stock) with page size 1000, the documented maximum; larger values are silently capped. A page shorter than 1000 ends the listing. A full page identical to the previous one fails the stream.
- **Incremental sync:** filters are `>=` on Clickker's naive local timestamps. Clickker converts a filter with an offset (`Z`) to local time, so the tap always sends naive values, and `start_date` is read as local time. Lists are sorted by business key, not by date, so a record changed mid-sync can sort below the final bookmark: each run starts `lookback_minutes` before the bookmark (the first run before `start_date`), and re-read records are deduplicated by primary key. A record without its replication value fails the stream.
- **Errors:** a missing or unknown key (403 `X-ApiKey Unknown or not present`, or 401) raises `InvalidCredentialsError` without retry. An unknown `administration` (500 `Cannot open database`) fails immediately. Other 4xx are fatal. 429 and 5xx retry up to 5 attempts with exponential backoff (60 s max, `Retry-After` honoured up to 120 s).

## Development

```bash
uv sync
uv run pytest
uv run tap-clickker --config config.json --discover > catalog.json
uv run tap-clickker --config config.json --catalog catalog.json
```
