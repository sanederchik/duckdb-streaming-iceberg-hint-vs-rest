"""Turns the raw JSON lines of one run into metrics (see README.md, section "Метрики")."""

import bisect
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

import boto3

from bench.config import BUCKET, Settings


def _lines(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")]


def _stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"median": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "median": round(statistics.median(ordered), 4),
        "p95": round(ordered[max(0, int(len(ordered) * 0.95) - 1)], 4),
        "max": round(ordered[-1], 4),
    }


def _tenth_medians(values: list[float]) -> dict[str, float | None]:
    """Median of the first and of the last 10 % of the run: how the metric drifts as snapshots accumulate."""
    if not values:
        return {"first_10pct": None, "last_10pct": None}
    tenth = max(1, len(values) // 10)
    return {"first_10pct": round(statistics.median(values[:tenth]), 4), "last_10pct": round(statistics.median(values[-tenth:]), 4)}


def _storage(settings: Settings, prefix: str, metadata_location: str) -> dict[str, int]:
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
    )
    objects = size = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            objects += 1
            size += item["Size"]
    key = metadata_location.split(f"s3://{BUCKET}/", 1)[1]
    return {"objects": objects, "bytes": size, "last_metadata_json_bytes": s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]}


def analyze(run_dir: Path, settings: Settings, prefix: str, meta: dict[str, Any]) -> dict[str, Any]:
    commits = [e for e in _lines(run_dir / "writer.jsonl") if e["event"] == "commit"]
    visible_at = [c["t_visible"] for c in commits]
    by_seq = {c["max_seq"]: c for c in commits}
    batch_rows = meta["batch_rows"]

    reads, errors, inconsistent = [], Counter(), 0
    missed, data_age = [], []
    for path in sorted(run_dir.glob("reader-*.jsonl")):
        for event in _lines(path):
            if "error" in event:
                errors[f'{event["error"]}: {event["message"][:160]}'] += 1
                continue
            reads.append(event)
            seen = event["max_seq"]
            if event["count"] != seen or event["distinct"] != seen:
                inconsistent += 1
            # newest batch that was already visible (published) when this query started
            index = bisect.bisect_right(visible_at, event["t_start"]) - 1
            visible_seq = commits[index]["max_seq"] if index >= 0 else 0
            missed.append(max(0, visible_seq - seen) // batch_rows)
            if seen in by_seq:
                data_age.append(event["t_end"] - by_seq[seen]["t_commit"])
    reads.sort(key=lambda e: e["t_start"])
    query_s = [e["t_end"] - e["t_start"] for e in reads]
    commit_s = [c["t_commit"] - c["t_start"] for c in commits]
    publish_s = [c["t_visible"] - c["t_commit"] for c in commits]
    attempts = len(reads) + sum(errors.values())
    return {
        **meta,
        "commits": len(commits),
        "snapshots_at_end": commits[-1]["snapshots"] if commits else 0,
        "writer_commit_s": {**_stats(commit_s), **_tenth_medians(commit_s)},
        "writer_publish_s": _stats(publish_s),
        "queries": len(reads),
        "query_errors": sum(errors.values()),
        "query_error_rate_pct": round(100 * sum(errors.values()) / attempts, 3) if attempts else None,
        "query_errors_by_kind": dict(errors),
        "inconsistent_results": inconsistent,
        "query_s": {**_stats(query_s), **_tenth_medians(query_s)},
        "data_age_s": _stats(data_age),
        "missed_batches": dict(sorted(Counter(missed).items())),
        "missed_batches_pct_nonzero": round(100 * sum(1 for m in missed if m) / len(missed), 3) if missed else None,
        "storage": _storage(settings, prefix, commits[-1]["metadata_location"]) if commits else {},
    }


SUMMARY_ROWS = [
    ("Фиксаций / снимков в конце", lambda m: f'{m["commits"]} / {m["snapshots_at_end"]}'),
    ("Запросов читателей", lambda m: m["queries"]),
    ("Ошибок запросов (доля)", lambda m: f'{m["query_errors"]} ({m["query_error_rate_pct"]} %)'),
    ("Несогласованных результатов", lambda m: m["inconsistent_results"]),
    ("Возраст данных, медиана / p95 / max, с", lambda m: "{median} / {p95} / {max}".format(**m["data_age_s"])),
    ("Запросов, пропустивших уже видимый пакет", lambda m: f'{m["missed_batches_pct_nonzero"]} %'),
    ("Запрос: медиана / p95, с", lambda m: "{median} / {p95}".format(**m["query_s"])),
    ("Запрос: медиана первых → последних 10 %, с", lambda m: "{first_10pct} → {last_10pct}".format(**m["query_s"])),
    ("Фиксация: медиана первых → последних 10 %, с", lambda m: "{first_10pct} → {last_10pct}".format(**m["writer_commit_s"])),
    ("Публикация указателя (hint), медиана, с", lambda m: m["writer_publish_s"]["median"]),
    ("Объектов / МБ в префиксе таблицы", lambda m: f'{m["storage"].get("objects")} / {round(m["storage"].get("bytes", 0) / 1e6, 1)}'),
    ("Последний metadata.json, КБ", lambda m: round(m["storage"].get("last_metadata_json_bytes", 0) / 1e3, 1)),
]


def summary(results: list[dict[str, Any]]) -> str:
    header = "| Метрика | " + " | ".join(f'{m["variant"]} · {m["scenario"]}' for m in results) + " |"
    lines = [header, "|---|" + "---|" * len(results)]
    for title, cell in SUMMARY_ROWS:
        lines.append(f"| {title} | " + " | ".join(str(cell(m)) for m in results) + " |")
    errors = [f'- **{m["variant"]} · {m["scenario"]}**: {kind} — {count}' for m in results
              for kind, count in m["query_errors_by_kind"].items()]
    return "\n".join(lines) + ("\n\nОшибки по видам:\n" + "\n".join(errors) if errors else "\n\nОшибок нет.") + "\n"
