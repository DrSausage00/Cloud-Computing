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
from pyspark.sql.functions import col, lit, window, avg, count, min, max, max_by, from_json, to_timestamp
# ermöglicht die Verwendung von den Funktionen
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# ermöglicht die Verwendung von Betriebssystemfunktionen
import os

temperature_limit = float(os.getenv("TEMP_LIMIT", "95.0"))

# erstellt eine SparkSession
spark = (SparkSession.builder
         .appName("MESStreamProcessing")
         .master("local[*]")
         .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# definiert das Schema für die JSON-Daten, die von der Kafka-Quelle gelesen werden
machine_schema = StructType([StructField("timestamp", StringType(), True),
                             StructField("machine_id", StringType(), True),
                             StructField("machine_type", StringType(), True),
                             StructField("temperature", DoubleType(), True),
                             StructField("pressure", DoubleType(), True),
                             StructField("vibration", DoubleType(), True),
                             StructField("status", StringType(), True)])

# erstellt einen Streaming-DataFrame, der kontinuierlich Daten aus einer Kafka-Quelle liest
stream = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "machine-events")
          .option("startingOffsets", "latest")
          .load()
          )

# erstellt einen neuen Streaming-DataFrame, der die Spalten "machine_id" und "temperature" enthält
machine_stream = (stream
                  .select(from_json(col("value").cast("string"), machine_schema).alias("data"))
                  .select("data.*")
                  .withColumn("timestamp", to_timestamp(col("timestamp")))
)

# aggregiert die Daten im Streaming-DataFrame nach einem Zeitfenster von 10 Sekunden und der Maschinen-ID
aggregated_stream = (machine_stream
                     .withWatermark("timestamp", "20 seconds")
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

                  # Grenze ergänzen
                  .withColumn("temperature_limit", lit(temperature_limit))

                  # Prüfen, ob die maximale Termperatur den Grenzwert überschritten hat
                  .withColumn("limit_exceeded", col("max_temperature") > col("temperature_limit"))
)

# gibt die Struktur des Streaming-DataFrames aus
query = (silver_stream.writeStream
         .format("console")
         .outputMode("update")
         .option("truncate", "false")
         .option("checkpointLocation", "./checkpoints/kafka_stream_v2")
         .start()
          )


# wartet darauf, dass der Streaming-Job beendet wird
query.awaitTermination()
