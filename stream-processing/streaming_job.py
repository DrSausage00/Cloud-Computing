# ermöglicht Spark mit Python zu verwenden
# Spark ist ein System zur schnellen Verarbeitung von großen Datenmengen
from pyspark.sql import SparkSession
# emöglicht die Verwendung von den Funktionen 
#               col (Spalten),
#               lit (konstante Werte), 
#               concat (Verkettung von Strings), 
#               window (Zeitfenster), 
#               avg (Durchschnitt), 
#               count (Anzahl), 
#               min (Minimum) 
#               max (Maximum)
#               when (Bedingte Logik)
#               max_by (Maximum nach Bedingung)
#               struct (Struktur)
from pyspark.sql.functions import col, concat, lit, window, avg, count, min, max, when, max_by, struct
# ermöglicht die Verwendung von Betriebssystemfunktionen
import os

temperature_limit = float(os.getenv("TEMP_LIMIT", "95.0"))

# erstellt eine SparkSession
spark = (SparkSession.builder
         .appName("MESStreanProcessing")
         .master("local[*]")
         .getOrCreate()
)

# erstellt einen Streaming-DataFrame, der kontinuierlich Daten aus einer Rate-Quelle liest
stream = (spark.readStream
          .format("rate")
          .option("rowsPerSecond", 1)
          .load()
          )

# erstellt einen neuen Streaming-DataFrame, der die Spalten "machine_id" und "temperature" enthält
machine_stream = (stream
                  .withColumn("machine_id", concat(lit("M"), (col("value") % 3)))
                  .withColumn("temperature", 70 + (col("value") % 30))
                  .withColumn("status", 
                              when((col("value") % 10) == 0, "ERROR")
                              .when((col("value") % 5) == 0, "IDLE")
                              .otherwise("RUNNING"))
)

# aggregiert die Daten im Streaming-DataFrame nach einem Zeitfenster von 10 Sekunden und der Maschinen-ID
aggregated_stream = (machine_stream
                     .withWatermark("timestamp", "20 seconds")
                     .groupBy(
                         window(col("timestamp"), "10 seconds"), 
                         col("machine_id"))
                     .agg(
                         avg("temperature").alias("avg_temperature"),
                         count("temperature").alias("count"), 
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
         .option("checkpointLocation", "./checkpoints/status_stream")
         .start()
          )

# wartet darauf, dass der Streaming-Job beendet wird
query.awaitTermination()
