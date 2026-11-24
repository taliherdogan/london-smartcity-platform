FROM apache/spark:3.5.3-python3

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_benchmarks.py ./

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_MASTER=local[4]
ENV SPARK_DRIVER_MEMORY=3g

CMD ["sh", "-c", "mkdir -p /tmp/benchmark_lake/spark_spill && exec /opt/spark/bin/spark-submit --master \"${SPARK_MASTER}\" --driver-memory \"${SPARK_DRIVER_MEMORY}\" --conf \"spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=35 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/benchmark_lake\" /app/run_benchmarks.py"]
