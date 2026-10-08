"""Connection to your Databricks workspace (used by every job that reads or writes the lakehouse).

  client()             Databricks SDK client  - upload files to the landing Volume, manage the DLT pipeline
  query(sql, params)   SQL warehouse          - read Silver / Gold tables (parameters are bound, never pasted into SQL)
  execute(sql)         SQL warehouse          - run a statement (e.g. compute the payouts)
  write_table(df, ..)  pandas DataFrame -> Gold table (Parquet file to the Volume, then CREATE/INSERT ... read_files)

Settings (.env): DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH, DATABRICKS_CATALOG, DATABRICKS_SCHEMA.
"""
import io
import sys
import uuid
from datetime import date, datetime, timezone
from typing import Any, List, Sequence
from urllib.parse import urlparse

from common.config import SKIP, databricks_configured, env

CATALOG = env("DATABRICKS_CATALOG", "workspace")
SCHEMA = env("DATABRICKS_SCHEMA", "ridehail")
VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/landing"


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


def host() -> str:
    """Only scheme://host is kept (a copied URL with a path or ?o=... still works)."""
    h = env("DATABRICKS_HOST")
    u = urlparse(h if "://" in h else "https://" + h)
    return f"https://{u.netloc}"


def require() -> None:
    """Exit with code 99 (Airflow: skipped) when the Databricks keys are not filled in."""
    if not databricks_configured():
        print("[dbx] DATABRICKS_HOST / DATABRICKS_TOKEN are empty in .env - skipping this step")
        sys.exit(SKIP)


_client = None


def client():
    global _client
    if _client is None:
        from databricks.sdk import WorkspaceClient
        _client = WorkspaceClient(host=host(), token=env("DATABRICKS_TOKEN"))
    return _client


# ------------------------------------------------------------------ SQL warehouse
_conn = None


def _connect():
    from databricks import sql as dbsql
    if not env("DATABRICKS_HTTP_PATH"):
        raise RuntimeError("DATABRICKS_HTTP_PATH is empty in .env (SQL Warehouses > your warehouse > Connection details)")
    return dbsql.connect(server_hostname=urlparse(host()).netloc, http_path=env("DATABRICKS_HTTP_PATH"),
                         access_token=env("DATABRICKS_TOKEN"))


def _clean(v: Any) -> Any:
    if isinstance(v, datetime) and v.tzinfo is not None:
        # Gold timestamps already hold New York wall-clock time; drop the "+00:00" label so nobody converts them again
        v = v.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if type(v).__name__ == "Decimal":
        return float(v)
    return v


def query(sql: str, params: Sequence[Any] = ()) -> List[dict]:
    """Rows as dicts. A table (or schema) that is not built yet gives no rows. A connection that timed out while
    idle is opened again once."""
    global _conn
    for attempt in (1, 2):
        try:
            if _conn is None:
                _conn = _connect()
            with _conn.cursor() as cur:
                cur.execute(sql, list(params) or None)
                if cur.description is None:
                    return []
                cols = [d[0] for d in cur.description]
                return [{c: _clean(v) for c, v in zip(cols, r)} for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001
            if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "SCHEMA_NOT_FOUND" in str(exc):
                return []
            try:
                _conn.close()
            except Exception:  # noqa: BLE001
                pass
            _conn = None
            if attempt == 2:
                raise
    return []


def execute(sql: str, params: Sequence[Any] = ()) -> None:
    query(sql, params)


def exists(name: str) -> bool:
    return bool(query(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE '{name}'"))


# ------------------------------------------------------------------ files
def upload_bytes(data: bytes, volume_path: str) -> None:
    client().files.upload(volume_path, io.BytesIO(data), overwrite=True)


def write_table(df, name: str, mode: str = "append") -> int:
    """pandas DataFrame -> Delta table workspace.ridehail.<name> (mode 'append' or 'overwrite').
    The rows go as one Parquet file to the landing Volume and are loaded with read_files() on the SQL warehouse."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    if df.empty:
        return 0
    t = pa.Table.from_pandas(df, preserve_index=False)
    # timestamps as microseconds labelled UTC (Databricks refuses nanoseconds and needs a feature flag for NTZ);
    # the values themselves are not shifted
    t = t.cast(pa.schema([pa.field(f.name, pa.timestamp("us", tz="UTC")) if pa.types.is_timestamp(f.type) else f
                          for f in t.schema]), safe=False)
    buf = io.BytesIO()
    pq.write_table(t, buf)
    path = f"{VOLUME}/jobs/{name}/{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}.parquet"
    upload_bytes(buf.getvalue(), path)
    cols = ", ".join(f"`{c}`" for c in df.columns)
    src = f"SELECT {cols} FROM read_files('{path}', format => 'parquet')"
    if mode == "overwrite" or not exists(name):
        execute(f"CREATE OR REPLACE TABLE {table(name)} AS {src}")
    else:
        execute(f"INSERT INTO {table(name)} ({cols}) {src}")
    try:
        client().files.delete(path)          # the rows are in the table now
    except Exception:  # noqa: BLE001
        pass
    return len(df)
