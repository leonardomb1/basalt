import datetime as dt
import decimal
import uuid

import fastavro

schema = {
    "type": "record",
    "name": "Order",
    "namespace": "shop",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": ["null", "string"]},
        {"name": "price", "type": "double"},
        {"name": "qty", "type": "int"},
        {"name": "ok", "type": "boolean"},
        {"name": "ratio", "type": "float"},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
        {"name": "big", "type": {"type": "fixed", "name": "Dec16", "size": 16, "logicalType": "decimal", "precision": 38, "scale": 6}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "t_ms", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "t_us", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "ts_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "lts_us", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
        {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID", "SHIPPED"]}},
        {"name": "addr", "type": ["null", {"type": "record", "name": "Addr", "fields": [
            {"name": "city", "type": "string"},
            {"name": "zip", "type": ["null", "int"]},
        ]}]},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attrs", "type": {"type": "map", "values": "long"}},
        {"name": "raw", "type": "bytes"},
        {"name": "choice", "type": ["null", "long", "string"]},
        {"name": "parent", "type": ["null", "Order"]},
    ],
}

utc = dt.timezone.utc


def order(i, parent=None):
    return {
        "id": i,
        "name": None if i == 2 else f"order {i} ação",
        "price": [19.99, -0.5, 1e300][i - 1],
        "qty": [3, -2147483648, 2147483647][i - 1],
        "ok": i != 2,
        "ratio": [0.25, -1.5, 3.0][i - 1],
        "amount": [decimal.Decimal("12345678.90"), decimal.Decimal("-0.01"), decimal.Decimal("0.00")][i - 1],
        "big": [decimal.Decimal("12345678901234567890123456789012.345678"), decimal.Decimal("-1.000001"), decimal.Decimal("0")][i - 1],
        "day": [dt.date(2026, 10, 24), dt.date(1969, 12, 31), dt.date(1, 1, 1)][i - 1],
        "t_ms": [dt.time(1, 2, 3, 4000), dt.time(0, 0), dt.time(23, 59, 59, 999000)][i - 1],
        "t_us": [dt.time(1, 2, 3, 4005), dt.time(0, 0), dt.time(23, 59, 59, 999999)][i - 1],
        "ts_ms": [dt.datetime(2026, 10, 24, 12, 30, 15, 123000, tzinfo=utc), dt.datetime(1969, 12, 31, 23, 59, 59, 999000, tzinfo=utc), dt.datetime(1970, 1, 1, tzinfo=utc)][i - 1],
        "ts_us": [dt.datetime(2026, 10, 24, 12, 30, 15, 123456, tzinfo=utc), dt.datetime(1900, 1, 1, tzinfo=utc), dt.datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=utc)][i - 1],
        "lts_us": [dt.datetime(2026, 10, 24, 9, 30, 15, 123456), dt.datetime(1970, 1, 1), dt.datetime(2000, 2, 29, 12)][i - 1],
        "uid": uuid.UUID(int=i * 0x1234567890ABCDEF),
        "status": ["NEW", "PAID", "SHIPPED"][i - 1],
        "addr": None if i == 2 else {"city": ["Lisboa", "", "São Paulo"][i - 1], "zip": None if i == 3 else 1000 + i},
        "tags": [["a", "b \"q\""], [], ["x"]][i - 1],
        "attrs": [{"k": 1, "z": -2}, {}, {"big": 9223372036854775807}][i - 1],
        "raw": [b"\x00\x01\xff", b"", b"plain"][i - 1],
        "choice": [7, None, "seven"][i - 1],
        "parent": parent,
    }


records = [order(1), order(2), order(3, parent=order(1))]
for codec in ["null", "deflate", "snappy", "zstandard"]:
    with open(f"/out/orders_{codec}.avro", "wb") as fo:
        fastavro.writer(fo, schema, records, codec=codec, sync_interval=200, sync_marker=b"basalt-fixture!!")
