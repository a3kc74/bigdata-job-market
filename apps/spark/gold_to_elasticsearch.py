import os
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def get_es_client() -> Elasticsearch:
    es_host = os.getenv("ES_HOST", "https://topcv-es-http.elastic-stack.svc:9200")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "changeme")
    es_verify_certs = str_to_bool(os.getenv("ES_VERIFY_CERTS", "false"))

    return Elasticsearch(
        [es_host],
        basic_auth=(es_user, es_password),
        verify_certs=es_verify_certs,
        request_timeout=60,
    )


def read_gold_data(spark: SparkSession) -> DataFrame:
    gold_path = os.getenv("GOLD_JOBS_FLAT_PATH", "s3a://gold/gold_jobs_flat/")
    return spark.read.format("parquet").load(gold_path)


def prepare_for_elasticsearch(df: DataFrame) -> DataFrame:
    return df.withColumn("indexed_at", F.current_timestamp())


def bulk_index_to_elasticsearch(df: DataFrame, es_index: str) -> None:
    es_client = get_es_client()

    actions = []
    for row in df.toJSON().toLocalIterator():
        import json
        doc = json.loads(row)
        doc_id = doc.get("job_id") or doc.get("_id")

        if not doc_id:
            continue

        actions.append({
            "_op_type": "index",
            "_index": es_index,
            "_id": doc_id,
            "_source": doc,
        })

    if actions:
        helpers.bulk(es_client, actions, chunk_size=500, request_timeout=60)
        print(f"Indexed {len(actions)} documents to index '{es_index}'")


def main():
    spark = SparkSession.builder.appName("GoldToElasticsearch").getOrCreate()

    try:
        df = read_gold_data(spark)
        print(f"Read {df.count()} records from gold layer")

        df_prepared = prepare_for_elasticsearch(df)

        es_index = os.getenv("ES_INDEX", "job-market-gold")
        bulk_index_to_elasticsearch(df_prepared, es_index)

        print("Successfully synced gold data to Elasticsearch")

    except Exception as e:
        print(f"Error syncing to Elasticsearch: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

