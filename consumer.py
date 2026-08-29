# We set PYSPARK_PYTHON as an env var at the top, before creating the SparkSession.
# Spark reads this env var when the SparkContext is created and sends that path to
# the executors, so they run the venv python instead of the system one.

import os
os.environ["PYSPARK_PYTHON"] = "/home/ubuntu/pyvenv/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/ubuntu/pyvenv/bin/python3"

from pyspark.sql import SparkSession

# NOTE FOR LATER: spark.executor.memory and spark.sql.shuffle.partitions are not
# tuned yet. They don't matter right now because this script only reads and
# prints message metadata, no caching, no groupBy, no join, no shuffle.
#
# Once the real FFT + averaging step is added, revisit both:
# - executor.memory: FFT output arrays get held in executor memory per task,
#   size this based on actual batch size and per-node RAM, not the default.
# - shuffle.partitions: any groupBy/agg for the per-bin average/std triggers a
#   real shuffle. Default is 200, way too many for an 8-core cluster. Set it
#   to match total cores (8), same reasoning used for the Kafka partition count.

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



kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.67.22.111:9092")
    .option("subscribe", "topic_stream")
    .option("startingOffsets", "latest")
    .load()
)

from pyspark.sql.functions import col, length

kafka_df_readable = kafka_df.select(
    col("key").cast("string").alias("scan_id"),
    length("value").alias("value_bytes"),
    "partition", "offset", "timestamp"
)

from pyspark.sql.streaming import StreamingQueryListener

SCANS_PER_MSG = 32     # must match producer.py's SCANS_PER_MSG
SCAN_LEN = 2048         # samples per scan
BYTES_PER_SAMPLE = 4    # float32
MSG_BYTES = SCANS_PER_MSG * SCAN_LEN * BYTES_PER_SAMPLE * 2  # *2 for I and Q

class ProgressPrinter(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        mb_s = (p.inputRowsPerSecond or 0) * MSG_BYTES / 1e6
        print(
            f"batch {p.batchId} | {p.numInputRows} msgs | "
            f"{p.inputRowsPerSecond:.1f} msg/s in | "
            f"{p.processedRowsPerSecond:.1f} msg/s processed | "
            f"{mb_s:6.1f} MB/s | "
            f"{p.durationMs.get('triggerExecution', 0)} ms/batch"
        )

    def onQueryTerminated(self, event):
        pass

spark.streams.addListener(ProgressPrinter())

query = (
    kafka_df_readable.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    # .trigger(processingTime="2 seconds")
    .start()
)

query.awaitTermination()

input("UI at :4040 . This only keeps the script alive so you can see it. press Enter to exit...")

spark.stop()