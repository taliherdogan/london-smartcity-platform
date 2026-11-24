FROM apache/spark:3.5.3-python3

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download spark-sql-kafka-0-10 and all transitive JARs at build time
# so runtime startup is instantaneous and requires zero Maven downloads.
RUN python3 -c "open('/tmp/prefetch.py', 'w').write('from pyspark.sql import SparkSession\ns = SparkSession.builder.master(\"local[1]\").appName(\"prefetch\").getOrCreate()\ns.stop()\n')" && \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /tmp/prefetch.py && \
  cp /tmp/.ivy2/jars/*.jar /opt/spark/jars/ && \
  rm -rf /tmp/.ivy2 /tmp/prefetch.py

COPY streaming_app.py mssql_sink.py ./

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_MASTER=local[4]
ENV SPARK_DRIVER_MEMORY=1280m

CMD ["sh", "-c", "mkdir -p /tmp/spark-spill/local /tmp/spark-spill/checkpoints && exec /opt/spark/bin/spark-submit --master \"${SPARK_MASTER}\" --driver-memory \"${SPARK_DRIVER_MEMORY}\" --conf \"spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/spark-spill\" /app/streaming_app.py"]
