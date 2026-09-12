# ermöglicht Spark mit Python zu verwenden
# Spark ist ein System zur schnellen Verarbeitung von großen Datenmengen
from pyspark.sql import SparkSession
# emöglicht die Verwendung von den Funktionen 
# col (Spalten), 
# lit (konstante Werte), 
# window (Zeitfenster), 
# avg (Durchschnitt), 
# count (Anzahl), 
# min (Minimum), 
# max (Maximum), 
# max_by (Maximum nach Spalte), 
# from_json (JSON in DataFrame konvertieren), 
# to_timestamp (String in Timestamp konvertieren)
# to_date (String in Date konvertieren)
from pyspark.sql.functions import col, lit, window, avg, count, min, max, max_by, from_json, to_timestamp, to_date
# ermöglicht die Verwendung von den Funktionen
from pyspark.sql.types import StructType, StructField, StringType, MapType
# ermöglicht die Verwendung von Betriebssystemfunktionen
import os
import sys

temperature_limit = float(os.getenv("TEMP_LIMIT", "95.0"))

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# ermöglicht die Verwendung von Betriebssystemfunktionen damit die MinIO-Umgebungsvariablen gelesen werden können
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_data_bucket = os.getenv("MINIO_DATA_BUCKET", "mes-data")  
checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/checkpoints")
machine_types = [
    t.strip().upper()
    for t in os.getenv("MACHINE_TYPES", "A,B,C").split(",")
    if t.strip()
]
machine_types_suffix = "-".join(machine_types).lower()
silver_trigger_seconds = int(os.getenv("SILVER_TRIGGER_INTERVAL_SECONDS", "10"))

# erstellt eine SparkSession
spark = (SparkSession.builder
         .appName("MESStreamProcessing")
         .config("spark.sql.shuffle.partitions", "4") # setzt die Anzahl der Partitionen für Shuffle-Operationen auf 4
         .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
         .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
         .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

         .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# definiert das Schema für die Messdaten, die von den Maschinen generiert werden
measurement_schema = MapType(
    StringType(), # der Schlüssel ist ein String (z.B. "temperature", "pressure", "vibration", "status")
    StringType(), # der Wert ist ein String (z.B. "75.0", "1.2", "0.5", "OK")
    True
)

# definiert das Schema für die JSON-Daten, die von der Kafka-Quelle gelesen werden
machine_schema = StructType([StructField("timestamp", StringType(), True),
                             StructField("machine_id", StringType(), True),
                             StructField("machine_type", StringType(), True),
                             StructField("measurements", measurement_schema, True),
                             StructField("schema_version", StringType(), True)
])

# erstellt einen Streaming-DataFrame, der kontinuierlich Daten aus einer Kafka-Quelle liest
stream = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
          .option("subscribe", "machine-events")
          .option("startingOffsets", "latest")
          .load()
          )

# erstellt einen neuen Streaming-DataFrame, der die Spalten "machine_id" und "temperature" enthält
machine_stream = (stream
                  .select(from_json(col("value").cast("string"), machine_schema, {"primitivesAsString": "true"}).alias("data"))
                  .select(col("data.timestamp").alias("timestamp"),
                          col("data.machine_id").alias("machine_id"),
                          col("data.machine_type").alias("machine_type"),
                          col("data.measurements").alias("measurements"),
                          col("data.measurements")["temperature"].cast("double").alias("temperature"),
                          col("data.measurements")["pressure"].cast("double").alias("pressure"),
                          col("data.measurements")["vibration"].cast("double").alias("vibration"),
                          col("data.measurements")["status"].alias("status"),
                          col("data.schema_version").alias("schema_version"))
                  .withColumn("timestamp", to_timestamp(col("timestamp")))
)

machine_stream = machine_stream.filter(col("machine_type").isin(machine_types))

# erstellt einen neuen Streaming-DataFrame für die Bronze-Daten
bronze_stream = (machine_stream
                    .withColumn("event_date", to_date(col("timestamp"))))

# merkt sich den zuletzt bekannten Status jeder Maschine
status_stream = (machine_stream
                 .filter(col("status").isNotNull())
                 .groupBy(col("machine_id"), col("machine_type"))
                 .agg(max_by(col("status"), col("timestamp")).alias("last_status"), 
                      max((col("timestamp"))).alias("last_timestamp"))
                 )

# aggregiert die Daten im Streaming-DataFrame nach einem Zeitfenster von 10 Sekunden und der Maschinen-ID
aggregated_stream = (machine_stream
                     .withWatermark("timestamp", "30 seconds")
                     .groupBy(
                         window(col("timestamp"), "10 seconds"), 
                         col("machine_id"),
                         col("machine_type"))
                     .agg(
                         avg("temperature").alias("avg_temperature"),
                         count("*").alias("event_count"), 
                         min("temperature").alias("min_temperature"),
                         max("temperature").alias("max_temperature"),
                         max_by(col("status"), col("timestamp")).alias("last_status"))
)

# erstellt einen neuen Streaming-DataFrame, der die Spalten "window_start" und "window_end" enthält und die Spalte "window" entfernt
silver_stream = (aggregated_stream
                  .withColumn("window_start", col("window.start"))
                  .withColumn("window_end", col("window.end"))
                  .drop("window")
                  .withColumn("event_date", to_date(col("window_start")))

                  # Grenze ergänzen
                  .withColumn("temperature_limit", lit(temperature_limit))

                  # Prüfen, ob die maximale Termperatur den Grenzwert überschritten hat
                  .withColumn("limit_exceeded", col("max_temperature") > col("temperature_limit"))
)

# Pfad für die aggregierten 10-Sekunden-Maschinenmetriken
silver_table_path = os.getenv("SILVER_TABLE_PATH", "silver/machine-metrics")
silver_path = f"s3a://{minio_data_bucket}/{silver_table_path}"
checkpoint_path = f"{checkpoint_dir}/machine-metrics-{machine_types_suffix}"

# Pfad für den zuletzt bekannten Status jeder Maschine
status_path = f"s3a://{minio_data_bucket}/silver/machine-status"
status_checkpoint_path = f"{checkpoint_dir}/machine-status-{machine_types_suffix}"

# Pfad für die Rohdaten der Maschinenmetriken
bronze_path = f"s3a://{minio_data_bucket}/bronze/machine-events"
bronze_checkpoint_path = f"{checkpoint_dir}/machine-events-{machine_types_suffix}"

# schreibt den aktuellen Status aller Maschinen nach MinIO
def write_status_to_minio(batch_df, batch_id):
    (batch_df.write
     .mode("overwrite")
     .partitionBy("machine_type")
     .parquet(status_path)
    )

# schreibt die Rohdaten der Maschinenmetriken nach MinIO
bronze_query = (bronze_stream.writeStream
                .format("parquet")
                .outputMode("append")
                .option("path", bronze_path)
                .option("checkpointLocation", bronze_checkpoint_path)
                .partitionBy("event_date")
                .start()
                )

def write_silver_to_minio(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Silver batch {batch_id}: leer, wird nicht geschrieben")
        return

    (
        batch_df.write
        .mode("append")
        .partitionBy("machine_type", "event_date") # partitioniert die Daten nach Maschinen-Typ und Datum
        .parquet(silver_path)
    )
    
# gibt die 10-ekunden-Aggregation nach MinIO aus
silver_query = (silver_stream.writeStream
                .foreachBatch(write_silver_to_minio)
                .outputMode("append")
                .trigger(processingTime=f"{silver_trigger_seconds} seconds")
                .option("checkpointLocation", checkpoint_path)
                .start()
                )

# merkt sich dauerhaft den zuletzt bekannten Status jeder Maschine
status_query = (status_stream.writeStream
                .foreachBatch(write_status_to_minio)
                .outputMode("complete")
                .option("checkpointLocation", status_checkpoint_path)
                .start()
                )

# wartet auf die Beendigung der Queries und behandelt Fehler, die während der Ausführung auftreten können
try:
    spark.streams.awaitAnyTermination()
except Exception as exc:
    print(f"Eine Query ist gestorben, Pod wird zum Neustart beendet: {exc}")
    sys.exit(1)

print("Alle Queries sind beendet. Beende SparkSession.")
