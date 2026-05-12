
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    NVARCHAR,
)
import urllib.parse

SERVER   = os.environ.get("AZURE_SQL_SERVER", "")
DATABASE = os.environ.get("AZURE_SQL_DB", "")
USERNAME = os.environ.get("AZURE_SQL_USER", "")
PASSWORD = os.environ.get("AZURE_SQL_PWD", "")
SCHEMA   = "dw"
DRIVER   = os.environ.get("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

EXPORT_BASE_DIR = Path(os.environ.get("MAILCHIMP_EXPORT_DIR", "export_mailchimp"))


def latest_export_dir() -> Path:
    subdirs = sorted(
        [d for d in EXPORT_BASE_DIR.iterdir() if d.is_dir()],
        key=lambda d: d.name,
        reverse=True,
    )
    if not subdirs:
        raise FileNotFoundError(f"No export folders found in {EXPORT_BASE_DIR}")
    return subdirs[0]

# Upload order: dimensions first, then facts
FILES_IN_ORDER = [
    "dim_user.csv",
    "dim_date.csv",
    "dim_mailchimp_audience.csv",
    "dim_mailchimp_campaign.csv",
    "dim_mailchimp_member.csv",
    "fact_mailchimp_audience_monthly_members_based.csv",
    "fact_mailchimp_campaign_monthly.csv",
]

# Optional: explicitly parse these columns as dates/datetimes if they exist
DATE_LIKE_COLUMNS = {
    "date": "date",
    "week_start_date": "date",
    "week_end_date": "date",
    "send_time": "datetime",
    "create_time": "datetime",
    "created_at": "datetime",
    "updated_at": "datetime",
    "timestamp_signup": "datetime",
    "timestamp_opt": "datetime",
    "last_changed": "datetime",
}

# ============================================================
# CONNECTION
# ============================================================
def make_engine():
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    params = urllib.parse.quote_plus(conn_str)
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        future=True,
    )
    return engine


# ============================================================
# DATA CLEANING / TYPE INFERENCE
# ============================================================
def parse_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col, kind in DATE_LIKE_COLUMNS.items():
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if kind == "date":
                # keep as Python date objects for SQL DATE
                df[col] = parsed.dt.date
            else:
                # keep as pandas datetime for SQL DATETIME2
                df[col] = parsed
    return df


def infer_sqlalchemy_type(series: pd.Series):
    non_null = series.dropna()

    # Boolean
    if pd.api.types.is_bool_dtype(series):
        return Boolean()

    # Integer
    if pd.api.types.is_integer_dtype(series):
        # choose Integer or BigInteger based on range
        if len(non_null) == 0:
            return Integer()
        min_val = int(non_null.min())
        max_val = int(non_null.max())
        if min_val < -2147483648 or max_val > 2147483647:
            return BigInteger()
        return Integer()

    # Float / decimal
    if pd.api.types.is_float_dtype(series):
        # Numeric preserves better than plain FLOAT for KPI fields
        return Numeric(18, 6)

    # Datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime()

    # Python date objects usually become object dtype, so detect by values
    if len(non_null) > 0:
        sample = non_null.iloc[0]
        sample_type_name = type(sample).__name__.lower()
        if sample_type_name == "date":
            return Date()
        if sample_type_name == "datetime":
            return DateTime()

    # Strings / fallback
    if len(non_null) == 0:
        return NVARCHAR(255)

    max_len = int(non_null.astype(str).map(len).max())
    # keep a minimum size, but cap for normal strings
    if max_len <= 100:
        return NVARCHAR(100)
    if max_len <= 255:
        return NVARCHAR(255)
    if max_len <= 500:
        return NVARCHAR(500)
    if max_len <= 1000:
        return NVARCHAR(1000)
    if max_len <= 2000:
        return NVARCHAR(2000)
    return NVARCHAR(None)   # NVARCHAR(MAX)


def sqlalchemy_to_sql_server_ddl(type_obj) -> str:
    if isinstance(type_obj, Boolean):
        return "BIT"
    if isinstance(type_obj, Integer):
        return "INT"
    if isinstance(type_obj, BigInteger):
        return "BIGINT"
    if isinstance(type_obj, Numeric):
        return f"DECIMAL({type_obj.precision},{type_obj.scale})"
    if isinstance(type_obj, Float):
        return "FLOAT"
    if isinstance(type_obj, Date):
        return "DATE"
    if isinstance(type_obj, DateTime):
        return "DATETIME2"
    if isinstance(type_obj, NVARCHAR):
        if type_obj.length is None:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({type_obj.length})"
    return "NVARCHAR(MAX)"


def infer_dtype_map(df: pd.DataFrame) -> Dict[str, object]:
    return {col: infer_sqlalchemy_type(df[col]) for col in df.columns}


# ============================================================
# SQL HELPERS
# ============================================================
def ensure_schema_exists(engine, schema: str):
    sql = f"""
    IF NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = '{schema}'
    )
    EXEC('CREATE SCHEMA [{schema}]')
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def drop_table_if_exists(engine, schema: str, table: str):
    sql = f"""
    IF OBJECT_ID(N'[{schema}].[{table}]', N'U') IS NOT NULL
        DROP TABLE [{schema}].[{table}];
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def create_table(engine, schema: str, table: str, df: pd.DataFrame, dtype_map: Dict[str, object]):
    column_defs = []
    for col in df.columns:
        sql_type = sqlalchemy_to_sql_server_ddl(dtype_map[col])
        column_defs.append(f"[{col}] {sql_type} NULL")

    ddl = f"""
    CREATE TABLE [{schema}].[{table}] (
        {', '.join(column_defs)}
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def upload_dataframe(engine, schema: str, table: str, df: pd.DataFrame, dtype_map: Dict[str, object]):
    # Convert NaN/NaT to None for cleaner inserts
    upload_df = df.copy()
    upload_df = upload_df.where(pd.notnull(upload_df), None)

    upload_df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        dtype=dtype_map,
        method=None,
        chunksize=1000,
    )


# ============================================================
# MAIN PIPELINE
# ============================================================
def load_one_csv(engine, csv_path: Path, schema: str):
    table = csv_path.stem
    print(f"\\n=== Processing {csv_path.name} -> [{schema}].[{table}] ===")

    df = pd.read_csv(csv_path)
    print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")

    df = parse_date_columns(df)
    dtype_map = infer_dtype_map(df)

    drop_table_if_exists(engine, schema, table)
    print(f"Dropped existing table if present: [{schema}].[{table}]")

    create_table(engine, schema, table, df, dtype_map)
    print(f"Created table: [{schema}].[{table}]")

    upload_dataframe(engine, schema, table, df, dtype_map)
    print(f"Uploaded {len(df):,} rows into [{schema}].[{table}]")


def validate_files(base_dir: Path, files: List[str]):
    missing = [f for f in files if not (base_dir / f).exists()]
    if missing:
        raise FileNotFoundError(
            "These CSV files were not found in CSV_DIR:\\n" + "\\n".join(missing)
        )


def main(csv_dir: Path = None):
    if csv_dir is None:
        csv_dir = latest_export_dir()

    print(f"Using export dir: {csv_dir}")
    print("Connecting to Azure SQL...")
    engine = make_engine()

    ensure_schema_exists(engine, SCHEMA)
    print(f"Schema ready: [{SCHEMA}]")

    validate_files(csv_dir, FILES_IN_ORDER)

    for filename in FILES_IN_ORDER:
        load_one_csv(engine, csv_dir / filename, SCHEMA)

    print("\\nAll tables recreated and uploaded successfully.")


if __name__ == "__main__":
    main()
