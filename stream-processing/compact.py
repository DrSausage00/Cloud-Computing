from pyspark.sql import SparkSession
import os
from datetime import date

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_data_bucket = os.getenv("MINIO_DATA_BUCKET", "mes-data")

table_path = os.getenv("TABLE_PATH", "silver/machine-metrics")
partition_cols = os.getenv("PARTITION_COLS", "machine_type,event_date").split(",")
full_path = f"s3a://{minio_data_bucket}/{table_path}"
event_date = os.getenv("COMPACT_EVENT_DATE") or date.today().isoformat()

spark = (SparkSession.builder
    .appName("SilverCompaction")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

if len(partition_cols) == 1:
    leaf_path = f"{full_path}/{partition_cols[0]}={event_date}"
    df = spark.read.parquet(leaf_path).cache()
    count_before = df.count()
    print(f"Zeilen vor Kompaktierung fuer {table_path} / {event_date}: {count_before}")

    if count_before > 0:
        df.coalesce(1).write.mode("overwrite").parquet(leaf_path)
        print(f"Kompaktierung fertig fuer {table_path} / {event_date}")
    else:
        print(f"Keine Daten fuer {table_path} / {event_date}, ueberspringe.")
    df.unpersist()
else:
    df = spark.read.parquet(full_path).filter(f"event_date = '{event_date}'").cache()
    count_before = df.count()
    print(f"Zeilen vor Kompaktierung fuer {table_path} / {event_date}: {count_before}")

    if count_before > 0:
        (df.coalesce(1)
           .write
           .mode("overwrite")
           .partitionBy(*partition_cols)
           .parquet(full_path))
        print(f"Kompaktierung fertig fuer {table_path} / {event_date}")
    else:
        print(f"Keine Daten fuer {table_path} / {event_date}, ueberspringe.")
    df.unpersist()
