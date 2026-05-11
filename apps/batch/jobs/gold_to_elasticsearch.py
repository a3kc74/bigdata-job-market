from pyspark.sql import SparkSession
from configs.settings import settings

INDEX = "gold-jobs-flat"

def main():
    spark = (
        SparkSession.builder
        .appName("gold_to_elasticsearch")
        .getOrCreate()
    )

    gold_path = settings.GOLD_PATH

    print(f"Reading Gold from: {gold_path}")

    df = spark.read.parquet(gold_path)

    count = df.count()
    print(f"Gold records read: {count}")

    (
        df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", "elasticsearch.search.svc")
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "false")
        .option("es.resource", INDEX)
        .option("es.mapping.id", "job_id")
        .mode("overwrite")
        .save()
    )

    print(f"Indexed {count} records to Elasticsearch index: {INDEX}")

    spark.stop()

if __name__ == "__main__":
    main()
