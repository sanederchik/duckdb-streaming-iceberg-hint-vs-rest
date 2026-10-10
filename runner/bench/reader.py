"""Reader process: one long-lived DuckDB session that queries the table in a loop.

variant=hint: ATTACH the .duckdb catalog file from S3 (READ_ONLY); it holds one view
              `events AS SELECT * FROM iceberg_scan('<table location>')`, i.e. every query reads version-hint.text.
variant=rest: ATTACH the Iceberg REST catalog (TYPE iceberg); every query asks the catalog for the current metadata.
Prints one JSON line per query (or failure) to stdout.
"""

import json
import os
import sys
import time

import duckdb

from bench.config import BUCKET, NAMESPACE, Settings, catalog_file_key

QUERY = "SELECT max(seq), count(*), count(DISTINCT seq) FROM {relation}"


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(
        config={
            "allow_persistent_secrets": False,
            "autoinstall_known_extensions": False,
            "extension_directory": os.environ["DUCKDB_EXTENSION_DIRECTORY"],
        }
    )
    for extension in ("httpfs", "avro", "iceberg"):
        con.execute(f"LOAD {extension}")
    con.execute(
        "CREATE SECRET s3 (TYPE S3, KEY_ID $k, SECRET $s, ENDPOINT $e, REGION $r, URL_STYLE 'path', USE_SSL false)",
        {"k": settings.access_key, "s": settings.secret_key, "e": settings.endpoint_host, "r": settings.region},
    )
    return con


def attach(con: duckdb.DuckDBPyConnection, variant: str, table: str, settings: Settings) -> str:
    if variant == "hint":
        con.execute(f"ATTACH 's3://{BUCKET}/{catalog_file_key(table)}' AS c (READ_ONLY)")
        return "c.events"
    con.execute(
        f"ATTACH 'warehouse' AS c (TYPE iceberg, ENDPOINT '{settings.rest_uri}', AUTHORIZATION_TYPE 'none')"
    )
    return f'c.{NAMESPACE}."{table}"'


def main() -> int:
    variant, table, duration, pause = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
    settings = Settings.from_env()
    con = connect(settings)
    relation = attach(con, variant, table, settings)
    sql = QUERY.format(relation=relation)
    deadline = time.time() + duration
    while time.time() < deadline:
        t_start = time.time()
        try:
            max_seq, count, distinct = con.execute(sql).fetchone()
            t_end = time.time()
            print(json.dumps({"t_start": t_start, "t_end": t_end, "max_seq": max_seq or 0, "count": count,
                              "distinct": distinct}), flush=True)
        except Exception as exc:  # benchmark boundary: every failure kind is recorded, never hidden
            print(json.dumps({"t_start": t_start, "t_end": time.time(), "error": type(exc).__name__,
                              "message": str(exc)[:500]}), flush=True)
        time.sleep(pause)
    return 0


if __name__ == "__main__":
    sys.exit(main())
