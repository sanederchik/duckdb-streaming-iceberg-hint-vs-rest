"""Runs every (variant, scenario) pair one after another and writes /results/<variant>-<scenario>/ + /results/summary.md.

SCENARIOS = "name:interval_s:duration_s,..."   VARIANTS = "hint,rest"
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import boto3
import duckdb

from bench.analyze import analyze, summary
from bench.config import BUCKET, Settings, catalog_file_key, table_location

RESULTS = Path("/results")


def wait_for_rest(settings: Settings, timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            with urllib.request.urlopen(f"{settings.rest_uri}/v1/config", timeout=3) as response:
                if response.status == 200:
                    return
        except OSError:
            if time.time() > deadline:
                raise
        time.sleep(1)


def wait_for_commit(writer_log: Path, writer: subprocess.Popen[bytes], timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if '"event": "commit"' in writer_log.read_text():
            return
        if writer.poll() is not None:
            raise RuntimeError(f"writer exited early, see {writer_log}")
        time.sleep(0.2)
    raise TimeoutError("no first commit")


def publish_catalog_file(settings: Settings, table: str, work: Path) -> None:
    """The '.duckdb catalog': a DuckDB file holding only a view over the table location (no credentials inside)."""
    path = work / "catalog.duckdb"
    con = duckdb.connect(str(path), config={"extension_directory": os.environ["DUCKDB_EXTENSION_DIRECTORY"]})
    for extension in ("httpfs", "avro", "iceberg"):
        con.execute(f"LOAD {extension}")
    con.execute(
        "CREATE TEMPORARY SECRET s3 (TYPE S3, KEY_ID $k, SECRET $s, ENDPOINT $e, REGION $r, URL_STYLE 'path', USE_SSL false)",
        {"k": settings.access_key, "s": settings.secret_key, "e": settings.endpoint_host, "r": settings.region},
    )
    con.execute(f"CREATE VIEW events AS SELECT * FROM iceberg_scan('{table_location('hint', table)}')")
    con.execute("CHECKPOINT")
    con.close()
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
    )
    s3.put_object(Bucket=BUCKET, Key=catalog_file_key(table), Body=path.read_bytes(), IfNoneMatch="*")


def run(variant: str, scenario: str, interval: float, duration: float, settings: Settings) -> dict:
    table = f"{variant}_{scenario}_{int(time.time())}"
    work = RESULTS / f"{variant}-{scenario}"
    work.mkdir(parents=True, exist_ok=True)
    readers, batch_rows, pause = int(os.environ["READERS"]), int(os.environ["BATCH_ROWS"]), os.environ["READER_PAUSE_S"]
    print(f"== {variant} · {scenario}: commit every {interval} s for {duration} s, {readers} readers", flush=True)
    writer_log = work / "writer.jsonl"
    writer = subprocess.Popen(
        [sys.executable, "-m", "bench.writer", variant, table, str(interval), str(duration), str(batch_rows)],
        stdout=writer_log.open("w"),
        stderr=(work / "writer.err").open("w"),
    )
    wait_for_commit(writer_log, writer)
    if variant == "hint":
        publish_catalog_file(settings, table, work)
    remaining = max(5.0, duration - 2)
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "bench.reader", variant, table, str(remaining), pause],
            stdout=(work / f"reader-{i}.jsonl").open("w"),
            stderr=(work / f"reader-{i}.err").open("w"),
        )
        for i in range(1, readers + 1)
    ]
    for proc in procs:
        proc.wait()
    if writer.wait() != 0:
        raise RuntimeError(f"writer failed, see {work / 'writer.err'}")
    prefix = table_location(variant, table).split(f"s3://{BUCKET}/", 1)[1]
    meta = {"variant": variant, "scenario": scenario, "interval_s": interval, "duration_s": duration,
            "readers": readers, "batch_rows": batch_rows, "table": table}
    metrics = analyze(work, settings, prefix, meta)
    (work / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False))
    return metrics


def main() -> int:
    settings = Settings.from_env()
    wait_for_rest(settings)
    scenarios = [s.split(":") for s in os.environ["SCENARIOS"].split(",")]
    results = [
        run(variant, name, float(interval), float(duration), settings)
        for name, interval, duration in scenarios
        for variant in os.environ["VARIANTS"].split(",")
    ]
    text = summary(results)
    (RESULTS / "summary.md").write_text(text)
    (RESULTS / "summary.json").write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(text, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
