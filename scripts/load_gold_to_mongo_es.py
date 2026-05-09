#!/usr/bin/env python3
# load_gold_to_mongo_es.py
# Import local gold parquet folders into MongoDB collections and Elasticsearch indices.
#
# CMD usage:
# python load_gold_to_mongo_es.py --gold-dir data\mock\gold --mongo-uri "mongodb://admin:admin123@localhost:27017/admin" --mongo-db job_market --es-url "http://localhost:9200"

import argparse
import datetime as dt
import math
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from pymongo import MongoClient, ReplaceOne
from elasticsearch import Elasticsearch, helpers


GOLD_TABLES = [
    "gold_jobs_flat",
    "gold_daily_stats",
    "gold_salary_bucket_stats",
    "gold_language_salary_stats",
    "gold_framework_salary_stats",
    "gold_category_stats",
]


def clean_value(value: Any) -> Any:
    """Convert pandas/numpy/NaN values to MongoDB/Elasticsearch-friendly values."""

    if value is None:
        return None

    # numpy arrays from parquet list columns, e.g. skills/languages/frameworks
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return clean_value(value.tolist())
        except Exception:
            pass

    # lists/tuples/sets
    if isinstance(value, (list, tuple, set)):
        return [clean_value(v) for v in value]

    # dicts
    if isinstance(value, dict):
        return {str(k): clean_value(v) for k, v in value.items()}

    # pandas / numpy scalar -> Python scalar
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except Exception:
            pass

    # datetime/date
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()

    # pandas NA / NaN
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # float nan/inf
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

    return value


def dataframe_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    records = []

    for idx, raw in enumerate(df.to_dict(orient="records")):
        doc = {str(k): clean_value(v) for k, v in raw.items()}

        if "_id" not in doc or doc["_id"] is None:
            key_parts = []
            for k in ["ingest_date", "job_id", "salary_bucket", "language", "framework", "category_level1"]:
                if k in doc:
                    key_parts.append(str(doc.get(k)))
            if not key_parts:
                key_parts = [str(idx)]
            doc["_id"] = "|".join(key_parts)
        else:
            doc["_id"] = str(doc["_id"])

        records.append(doc)

    return records


def read_gold_table(gold_dir: Path, table: str) -> List[Dict[str, Any]]:
    table_dir = gold_dir / table

    if not table_dir.exists():
        print(f"[SKIP] {table}: folder not found: {table_dir}")
        return []

    parquet_files = sorted(table_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"[SKIP] {table}: no parquet files in {table_dir}")
        return []

    df = pd.read_parquet(table_dir)
    records = dataframe_to_records(df)
    print(f"[READ] {table}: {len(records)} records")
    return records


def load_mongodb(mongo_uri: str, mongo_db: str, table: str, records: List[Dict[str, Any]]) -> None:
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[table]

    collection.delete_many({})

    if records:
        operations = [
            ReplaceOne({"_id": record["_id"]}, record, upsert=True)
            for record in records
        ]
        collection.bulk_write(operations, ordered=False)

    collection.create_index("ingest_date")

    if table == "gold_jobs_flat":
        collection.create_index("province")
        collection.create_index("category_level1")
        collection.create_index("salary_bucket")
    elif table == "gold_language_salary_stats":
        collection.create_index("language")
    elif table == "gold_framework_salary_stats":
        collection.create_index("framework")

    print(f"[MONGO] {mongo_db}.{table}: {collection.count_documents({})} docs")


def es_mapping_for_table(table: str) -> Dict[str, Any]:
    keyword_fields = [
        "job_id",
        "source",
        "title_raw",
        "title_normalized",
        "company_name",
        "company_normalized_name",
        "province",
        "category_level1",
        "category_level2",
        "category_level3",
        "salary_bucket",
        "salary_type",
        "work_mode",
        "seniority",
        "experience_bucket",
        "job_type",
        "language",
        "framework",
    ]

    long_fields = [
        "salary_min_vnd",
        "salary_max_vnd",
        "salary_mid_vnd",
        "experience_years_min",
        "total_jobs",
        "active_jobs",
        "salary_disclosed_jobs",
        "remote_jobs",
        "job_count",
        "avg_salary_mid_vnd",
    ]

    double_fields = ["job_pct"]
    boolean_fields = ["is_salary_disclosed", "is_remote", "is_active"]
    date_fields = ["ingest_date", "deadline_date"]

    properties: Dict[str, Any] = {}

    for field in keyword_fields:
        properties[field] = {"type": "keyword"}

    for field in long_fields:
        properties[field] = {"type": "long"}

    for field in double_fields:
        properties[field] = {"type": "double"}

    for field in boolean_fields:
        properties[field] = {"type": "boolean"}

    for field in date_fields:
        properties[field] = {
            "type": "date",
            "format": "yyyy-MM-dd||strict_date_optional_time||epoch_millis",
        }

    for field in ["languages", "frameworks", "skills"]:
        properties[field] = {"type": "keyword"}

    return {"mappings": {"properties": properties}}


def load_elasticsearch(es_url: str, table: str, records: List[Dict[str, Any]]) -> None:
    es = Elasticsearch(es_url)
    index = table.replace("_", "-")

    if es.indices.exists(index=index):
        es.indices.delete(index=index)

    es.indices.create(index=index, body=es_mapping_for_table(table))

    if records:
        actions = []

        for record in records:
            # Elasticsearch dùng _id làm metadata field.
            # Không được để _id nằm trong _source hoặc mapping.
            source = {
                key: value
                for key, value in record.items()
                if key != "_id"
            }

            actions.append(
                {
                    "_index": index,
                    "_id": record["_id"],
                    "_source": source,
                }
            )

        helpers.bulk(es, actions, chunk_size=500)

    es.indices.refresh(index=index)
    count = es.count(index=index)["count"]
    print(f"[ES] {index}: {count} docs")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-dir", required=True, help="Path to data/mock/gold")
    parser.add_argument("--mongo-uri", default="mongodb://admin:admin123@localhost:27017/admin")
    parser.add_argument("--mongo-db", default="job_market")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--skip-mongo", action="store_true")
    parser.add_argument("--skip-es", action="store_true")
    args = parser.parse_args()

    gold_dir = Path(args.gold_dir)

    if not gold_dir.exists():
        raise FileNotFoundError(f"Gold dir not found: {gold_dir}")

    for table in GOLD_TABLES:
        records = read_gold_table(gold_dir, table)

        if not records:
            continue

        if not args.skip_mongo:
            load_mongodb(args.mongo_uri, args.mongo_db, table, records)

        if not args.skip_es:
            load_elasticsearch(args.es_url, table, records)

    print("[DONE] Gold data loaded to requested destinations.")


if __name__ == "__main__":
    main()
