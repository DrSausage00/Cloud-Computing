FROM apache/spark:4.2.0-python3

USER root

WORKDIR /app

RUN echo "" > /tmp/noop.py \
    && JAVA_TOOL_OPTIONS="-Duser.home=/tmp" /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0 \
      /tmp/noop.py \
    && rm /tmp/noop.py \
    && chmod -R a+rwX /tmp/.ivy2.5.2 /tmp/.m2 2>/dev/null || true

COPY streaming_job.py /app/streaming_job.py
COPY compact.py /app/compact.py

USER spark

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0", "/app/streaming_job.py"]
