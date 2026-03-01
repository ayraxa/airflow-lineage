from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.lineage.entities import Table

DATA_DIR = Path("/opt/airflow/data")

RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
REPORT_DIR = DATA_DIR / "reports"
SUMMARY_DIR = DATA_DIR / "summary"

RAW_CSV_PATH = RAW_DIR / "raw.csv"
CLEAN_PARQUET_PATH = CLEAN_DIR / "clean.parquet"
QUALITY_JSON_PATH = REPORT_DIR / "quality_report.json"
SUMMARY_CSV_PATH = SUMMARY_DIR / "summary.csv"

# IMPORTANT: For OpenLineage manual lineage, use Table entities (File is ignored)
def ds(name: str) -> Table:
    return Table(cluster="pipeline_observatory", database="observatory", name=name)

RAW_TABLE = ds("raw_csv")
CLEAN_TABLE = ds("clean_parquet")
REPORT_TABLE = ds("quality_report_json")
SUMMARY_TABLE = ds("summary_csv")


APPEND_DELIM = " | "  # delimiter when appending comments

# tags we recognize in misc like "ai: ..." or "ai - ..."
PRODUCT_COLS = ["ai", "cleaning", "washing"]
_TAG_PATTERN = re.compile(r"^\s*(ai|cleaning|washing)\s*[:\-]\s*(.+?)\s*$", re.IGNORECASE)


def ingest_raw():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "name": "molly",
            "company": "company123",
            "ai": None,
            "cleaning": None,
            "washing": None,
            # misc has overflow comments, comma-separated
            "misc": "good,bad,great",
        },
        {
            "name": "alice",
            "company": "company123",
            "ai": "already has AI",
            "cleaning": None,
            "washing": None,
            "misc": "cleaning: extra1, washing - extra2",
        },
        {
            "name": "bob",
            "company": "company999",
            "ai": None,
            "cleaning": None,
            "washing": "already has washing",
            "misc": "washing: onlyone",
        },
    ]

    pd.DataFrame(rows).to_csv(RAW_CSV_PATH, index=False)


def _normalize_text(val) -> str | None:
    """Normalize text fields; treat empty/None/'null' as missing."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "" or s.lower() == "null":
        return None
    return s


def _split_misc(misc_val) -> list[str]:
    """Split misc into a list of trimmed non-empty comments."""
    misc = _normalize_text(misc_val)
    if misc is None:
        return []
    parts = [p.strip() for p in misc.split(",")]
    return [p for p in parts if p]


def _parse_tagged_item(item: str) -> tuple[str | None, str]:
    """
    If item matches 'ai: comment' or 'ai - comment' (cleaning/washing too),
    return (col, comment). Otherwise return (None, item).
    """
    s = _normalize_text(item)
    if s is None:
        return (None, "")
    m = _TAG_PATTERN.match(s)
    if not m:
        return (None, s)
    col = m.group(1).lower().strip()
    comment = m.group(2).strip()
    return (col, comment)


def _append_or_set(existing: str | None, new_text: str) -> str:
    """If existing is present, append with delimiter; else set."""
    existing_norm = _normalize_text(existing)
    new_norm = _normalize_text(new_text)
    if new_norm is None:
        return existing_norm or ""
    if existing_norm is None:
        return new_norm
    return f"{existing_norm}{APPEND_DELIM}{new_norm}"


def clean_transform():
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(RAW_CSV_PATH)

    # normalize column names
    df.columns = _normalize_column_names(df.columns)

    # ensure required columns exist (filling missing ones with None)
    df = _ensure_required_columns(df)

    # normalize text fields
    df = _normalize_text_fields(df)

    # fill from misc
    df = df.apply(_fill_from_misc, axis=1)

    # drop duplicates
    df = df.drop_duplicates(subset=["name", "company"], keep="first")

    # save
    df.to_parquet(CLEAN_PARQUET_PATH, index=False)
    df.to_csv(CLEAN_DIR / "clean.csv", index=False)

def _normalize_column_names(columns):
    return [c.strip().lower().replace(" ", "_") for c in columns]

def _ensure_required_columns(df):
    required = ["name", "company", *PRODUCT_COLS, "misc"]
    for col in required:
        if col not in df.columns:
            df[col] = None
    return df

def _normalize_text_fields(df):
    required = ["name", "company", *PRODUCT_COLS, "misc"]
    for col in required:
        df[col] = df[col].map(_normalize_text)
    df["name"] = df["name"].map(lambda x: x.title() if isinstance(x, str) else x)
    df["company"] = df["company"].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def _fill_from_misc(row: pd.Series) -> pd.Series:
    misc_items = _split_misc(row.get("misc"))
    untagged = _route_tagged_items(row, misc_items)
    row = _fill_empty_columns(row, untagged)
    row["misc"] = _update_misc(row, untagged)
    return row

def _route_tagged_items(row, misc_items):
    untagged = []
    for item in misc_items:
        col, comment = _parse_tagged_item(item)
        if col in PRODUCT_COLS:
            row[col] = _append_or_set(row.get(col), comment)
        else:
            if comment:
                untagged.append(comment)
    return untagged

def _fill_empty_columns(row, untagged):
    remaining = untagged[:]
    for col in PRODUCT_COLS:
        if row.get(col) is None and remaining:
            row[col] = remaining.pop(0)
    return row

def _update_misc(row, untagged):
    return ", ".join(untagged) if untagged else None


def quality_report():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(CLEAN_PARQUET_PATH)
    report = {
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "missing_pct": (df.isna().mean() * 100).round(2).to_dict(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    QUALITY_JSON_PATH.write_text(json.dumps(report, indent=2))


def publish_summary():
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(CLEAN_PARQUET_PATH)

    # example summary: rows by company 
    summary = df.groupby("company", dropna=False).size().reset_index(name="rows")
    summary.to_csv(SUMMARY_CSV_PATH, index=False)


with DAG(
    dag_id="pipeline_observatory",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "lineage"],
) as dag:
    t1 = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw,
        outlets=[RAW_TABLE],
    )

    t2 = PythonOperator(
        task_id="clean_transform",
        python_callable=clean_transform,
        inlets=[RAW_TABLE],
        outlets=[CLEAN_TABLE],
    )

    t3 = PythonOperator(
        task_id="quality_report",
        python_callable=quality_report,
        inlets=[CLEAN_TABLE],
        outlets=[REPORT_TABLE],
    )

    t4 = PythonOperator(
        task_id="publish_summary",
        python_callable=publish_summary,
        inlets=[CLEAN_TABLE],
        outlets=[SUMMARY_TABLE],
    )

    t1 >> t2 >> t3 >> t4
