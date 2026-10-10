"""Settings shared by the writer, the readers and the orchestrator (all read from the environment)."""

import os
from dataclasses import dataclass

BUCKET = "warehouse"
NAMESPACE = "bench"
HINT_FILE = "version-hint.text"


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    access_key: str
    secret_key: str
    region: str
    rest_uri: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            os.environ["S3_ENDPOINT"],
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"],
            os.environ.get("AWS_REGION", "us-east-1"),
            os.environ["REST_URI"],
        )

    @property
    def endpoint_host(self) -> str:
        return self.s3_endpoint.split("://", 1)[1]


def table_location(variant: str, table: str) -> str:
    """Physical location of the table; the REST catalog chooses its own location under s3://warehouse/rest/."""
    return f"s3://{BUCKET}/hint/{table}" if variant == "hint" else f"s3://{BUCKET}/rest/{NAMESPACE}/{table}"


def catalog_file_key(table: str) -> str:
    return f"catalog/{table}.duckdb"
