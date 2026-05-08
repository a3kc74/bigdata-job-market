import json
import os
from typing import Iterable, List, Optional, Tuple

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import DataFrame, SparkSession


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name, default)
    if value is None or value == "":
        return default
    return value


def upsert_dataframe(
    df: DataFrame,
    collection,
    id_field: str = "_id",
    batch_size: int = 500,
) -> int:
    """
    Convert Spark rows to JSON-safe Python dicts, then bulk upsert to MongoDB.
    Uses DataFrame.toJSON() so date/timestamp values become serializable strings.
    """
    count = 0
    ops: List[ReplaceOne] = []

    for row_json in df.toJSON().toLocalIterator():
        doc = json.loads(row_json)

        if id_field not in doc or doc[id_field] in (None, ""):
            raise ValueError(f"Document missing required id field '{id_field}': {doc}")

        ops.append(
            ReplaceOne(
                {id_field: doc[id_field]},
                doc,
                upsert=True,
            )
        )

        if len(ops) >= batch_size:
            collection.bulk_write(ops, ordered=False)
            count += len(ops)
            ops = []

    if ops:
        collection.bulk_write(ops, ordered=False)
        count += len(ops)

    return count


def sync_collection(
    spark: SparkSession,
    path: Optional[str],
    mongo_db,
    collection_name: str,
    id_field: str = "_id",
) -> int:
    if not path:
        print(f"[SKIP] {collection_name}: path is empty")
        return 0

    print(f"[READ] {collection_name} <- {path}")
    df = spark.read.parquet(path)

    if id_field not in df.columns:
        raise ValueError(
            f"Collection '{collection_name}' expects field '{id_field}', "
            f"but dataframe columns are: {df.columns}"
        )

    # Optional: coalesce small datasets to reduce driver overhead when reading
    if df.rdd.getNumPartitions() > 8:
        df = df.coalesce(8)

    collection = mongo_db[collection_name]
    written = upsert_dataframe(df, collection, id_field=id_field)
    print(f"[DONE] {collection_name}: upserted {written} documents")
    return written


def main():
    mongo_uri = get_env("MONGODB_URI", "mongodb://admin:admin123@mongodb:27017/admin")
    mongo_db_name = get_env("MONGODB_DATABASE", "job_market")

    gold_jobs_flat_path = get_env("GOLD_JOBS_FLAT_PATH")
    gold_daily_stats_path = get_env("GOLD_DAILY_STATS_PATH")
    gold_salary_bucket_stats_path = get_env("GOLD_SALARY_BUCKET_STATS_PATH")
    gold_language_stats_path = get_env("GOLD_LANGUAGE_STATS_PATH")
    gold_framework_stats_path = get_env("GOLD_FRAMEWORK_STATS_PATH")
    gold_category_stats_path = get_env("GOLD_CATEGORY_STATS_PATH")

    spark = SparkSession.builder.appName("gold-to-mongodb").getOrCreate()
    client = MongoClient(mongo_uri)
    mongo_db = client[mongo_db_name]

    try:
        total = 0

        total += sync_collection(
            spark,
            gold_jobs_flat_path,
            mongo_db,
            "gold_jobs_flat",
            id_field="_id",
        )

        total += sync_collection(
            spark,
            gold_daily_stats_path,
            mongo_db,
            "gold_daily_stats",
            id_field="_id",
        )

        total += sync_collection(
            spark,
            gold_salary_bucket_stats_path,
            mongo_db,
            "gold_salary_bucket_stats",
            id_field="_id",
        )

        total += sync_collection(
            spark,
            gold_language_stats_path,
            mongo_db,
            "gold_language_salary_stats",
            id_field="_id",
        )

        total += sync_collection(
            spark,
            gold_framework_stats_path,
            mongo_db,
            "gold_framework_salary_stats",
            id_field="_id",
        )

        total += sync_collection(
            spark,
            gold_category_stats_path,
            mongo_db,
            "gold_category_stats",
            id_field="_id",
        )

        print(f"[SUCCESS] Total upserted documents: {total}")

    finally:
        spark.stop()
        client.close()


if __name__ == "__main__":
    main()