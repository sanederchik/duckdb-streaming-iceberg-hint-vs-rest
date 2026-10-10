"""Writer process: appends a batch of BATCH_ROWS rows every INTERVAL seconds for DURATION seconds.

variant=hint: PyIceberg SqlCatalog (a local SQLite file only the writer can see) + conditional PUT of
              <table>/metadata/version-hint.text after every commit; readers never see the catalog.
variant=rest: PyIceberg RestCatalog; the catalog service is the only pointer readers use.
Prints one JSON line per commit to stdout.
"""

import json
import sys
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from bench.config import BUCKET, HINT_FILE, NAMESPACE, Settings, table_location

SCHEMA = Schema(
    NestedField(1, "seq", LongType(), required=False),
    NestedField(2, "batch", LongType(), required=False),
    NestedField(3, "written_at", TimestampType(), required=False),
    NestedField(4, "payload", StringType(), required=False),
)
ARROW = pa.schema([("seq", pa.int64()), ("batch", pa.int64()), ("written_at", pa.timestamp("us")), ("payload", pa.string())])


def file_io(settings: Settings) -> dict[str, str]:
    return {
        "s3.endpoint": settings.s3_endpoint,
        "s3.access-key-id": settings.access_key,
        "s3.secret-access-key": settings.secret_key,
        "s3.region": settings.region,
        "s3.path-style-access": "true",
    }


def open_catalog(variant: str, settings: Settings) -> Catalog:
    if variant == "hint":
        return SqlCatalog("bench", uri="sqlite:////tmp/writer-catalog.db", warehouse=f"s3://{BUCKET}/hint", **file_io(settings))
    return RestCatalog("bench", uri=settings.rest_uri, **file_io(settings))


class HintPublisher:
    """Compare-and-swap publication of the hint: the body is the metadata file name without `.metadata.json`."""

    def __init__(self, settings: Settings, table: str) -> None:
        self.s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            region_name=settings.region,
        )
        self.key = f"hint/{table}/metadata/{HINT_FILE}"
        self.etag: str | None = None

    def publish(self, metadata_location: str) -> None:
        stem = metadata_location.rsplit("/", 1)[1].removesuffix(".metadata.json")
        condition = {"IfMatch": self.etag} if self.etag else {"IfNoneMatch": "*"}
        self.etag = self.s3.put_object(Bucket=BUCKET, Key=self.key, Body=stem.encode("ascii"), **condition)["ETag"]


def main() -> int:
    variant, table, interval, duration, batch_rows = (
        sys.argv[1],
        sys.argv[2],
        float(sys.argv[3]),
        float(sys.argv[4]),
        int(sys.argv[5]),
    )
    settings = Settings.from_env()
    catalog = open_catalog(variant, settings)
    catalog.create_namespace_if_not_exists(NAMESPACE)
    location = table_location(variant, table) if variant == "hint" else None
    iceberg_table = catalog.create_table((NAMESPACE, table), SCHEMA, location=location)
    hint = HintPublisher(settings, table) if variant == "hint" else None
    if hint:
        hint.publish(iceberg_table.metadata_location)
    print(json.dumps({"event": "created", "metadata_location": iceberg_table.metadata_location}), flush=True)

    deadline = time.time() + duration
    batch = seq = 0
    while time.time() < deadline:
        t_start = time.time()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = pa.table(
            {
                "seq": list(range(seq + 1, seq + batch_rows + 1)),
                "batch": [batch + 1] * batch_rows,
                "written_at": [now] * batch_rows,
                "payload": [f"row-{i}" for i in range(seq + 1, seq + batch_rows + 1)],
            },
            schema=ARROW,
        )
        iceberg_table.append(rows)
        t_commit = time.time()
        if hint:
            hint.publish(iceberg_table.metadata_location)
        t_visible = time.time()
        batch += 1
        seq += batch_rows
        print(
            json.dumps(
                {
                    "event": "commit",
                    "batch": batch,
                    "max_seq": seq,
                    "t_start": t_start,
                    "t_commit": t_commit,
                    "t_visible": t_visible,
                    "snapshots": len(iceberg_table.metadata.snapshots),
                    "metadata_location": iceberg_table.metadata_location,
                }
            ),
            flush=True,
        )
        time.sleep(max(0.0, interval - (time.time() - t_start)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
