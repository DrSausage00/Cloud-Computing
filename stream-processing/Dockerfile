FROM apache/spark:4.2.0-python3

USER root

WORKDIR /app

COPY streaming_job.py /app/streaming_job.py

USER spark

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0", "/app/streaming_job.py"]