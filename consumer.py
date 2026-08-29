# We set PYSPARK_PYTHON as an env var at the top, before creating the SparkSession.
# Spark reads this env var when the SparkContext is created and sends that path to
# the executors, so they run the venv python instead of the system one.

import os
os.environ["PYSPARK_PYTHON"] = "/home/ubuntu/pyvenv/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/ubuntu/pyvenv/bin/python3"

from pyspark.sql import SparkSession

# NOTE: Change this to whatever is needed for the actual consumer.

spark = SparkSession.builder \
    .master("spark://master:7077") \
    .appName("QUAX - Kafka FFT streaming") \
    .config("spark.pyspark.python", "/home/ubuntu/pyvenv/bin/python3") \
    .config("spark.executorEnv.PYSPARK_PYTHON", "/home/ubuntu/pyvenv/bin/python3") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()


sc = spark.sparkContext

def check(_):
    import socket, sys, numpy
    return f"{socket.gethostname()} | numpy {numpy.__version__} | {sys.executable}"

for line in sorted(set(sc.parallelize(range(8), 8).map(check).collect())):
    print(line)

spark.stop()