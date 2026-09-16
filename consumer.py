import os
os.environ["PYSPARK_PYTHON"] = "/home/ubuntu/pyvenv/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/ubuntu/pyvenv/bin/python3"

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, lit, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, ArrayType, DoubleType, LongType
)
from pyspark.sql.streaming.listener import StreamingQueryListener

SCANS_PER_MSG = 32                                    # must match producer.py
SCAN_LEN = 2048
MSG_BYTES = SCANS_PER_MSG * SCAN_LEN * 4 * 2          # float32, I and Q
FS = 2e6                                              # ADC rate, 2 MS/s
FREQ_HZ = np.fft.fftshift(np.fft.fftfreq(SCAN_LEN, 1 / FS)).tolist()   # -1 MHz .. +1 MHz
BOOTSTRAP = "10.67.22.111:9092"

spark = (
    SparkSession.builder
    .master("spark://master:7077")
    .appName("QUAX - Kafka FFT streaming")
    .config("spark.pyspark.python", "/home/ubuntu/pyvenv/bin/python3")
    .config("spark.executorEnv.PYSPARK_PYTHON", "/home/ubuntu/pyvenv/bin/python3")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    # Arrow ships rows from the JVM task thread to the Python worker in batches: message_partials
    # slices over its partition with a window of 64 messages. At 512 KiB each that is
    # 32 MiB of Arrow buffer per task; the default 10 000 would be around 5 GiB and (B)OOM.
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "64")
    # the only shuffle is groupBy(lit(1)): one key, so exactly one reduce task can
    # ever receive rows. The default 200 would schedule 200 tasks per micro-batch,
    # 199 of them empty. Nothing crazy would happen with the default value, but it is nice to keep things in check.
    .config("spark.sql.shuffle.partitions", "1")
    # This is a live monitor: after a restart we want current spectra, not a replay
    # of a stale backlog. So no persistent checkpoint. Spark makes a temporary one
    # per run and this line deletes it on stop. Cost: a restart skips whatever
    # had arrived while we were down.
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# progress reporting
class ProgressPrinter(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        rate_in = p.inputRowsPerSecond or 0.0
        rate_out = p.processedRowsPerSecond or 0.0
        print(
            f"batch {p.batchId} | {p.numInputRows} msgs | "
            f"{rate_in:.1f} msg/s in | {rate_out:.1f} msg/s processed | "
            f"{rate_in * MSG_BYTES / 1e6:6.1f} MB/s | "
            f"{p.durationMs.get('triggerExecution', 0)} ms/batch",
            flush=True,
        )

    def onQueryTerminated(self, event):
        pass


spark.streams.addListener(ProgressPrinter())

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "topic_stream")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 2048)
    .option("failOnDataLoss", "false")
    .option("kafka.max.partition.fetch.bytes", 33554432)   # 32 MiB, ~64 msgs
    .load()
)

## NOTE:
## Assume a micro-batch contains the maximum 2048 Kafka messages.
## With 8 Kafka partitions, ideally we have 256 messages per partition.
##
## message_partials is a vectorized map: it processes the messages in Arrow batches
## of 64 messages at a time. Therefore, each partition has 256 / 64 = 4 Arrow batches.
## Each Arrow batch produces 64 output rows, one output row per input message.
## Each output row contains one array of length 4096:
##   first 2048 values = sum P over the 32 scans
##   second 2048 values = sum P**2 over the 32 scans.
##
## fold_partition then operates separately on each Spark partition.
## It receives the 4 Arrow/Pandas batches belonging to that partition,
## containing 4 × 64 = 256 rows in total.
## It stacks and sums those 256 partial arrays, producing ONE aggregated row
## for that partition.
##
## Therefore, after fold_partition we have:
##   8 partitions × 1 row = 8 rows.
##
## summarize_batch must now combine these 8 partition-level rows into one final result.
## We group all 8 rows under one dummy key using groupBy(lit(1)).
## This causes a shuffle, bringing the 8 rows together.
## applyInPandas then receives those 8 rows as one group.
## summarize_batch stacks and sums their partial arrays again, producing ONE final row
## representing the entire 2048-message micro-batch.
##
## Finally, foreachBatch runs this whole processing pipeline independently
## for each streaming micro-batch.

@pandas_udf(ArrayType(DoubleType()))
def message_partials(values: pd.Series) -> pd.Series:
    out = []
    for v in values:
        if v is None or len(v) != MSG_BYTES:          # poison message: skip, don't crash
            out.append(None)
            continue
        flat = np.frombuffer(v, dtype='<f4')
        half = flat.size // 2
        sig = (flat[:half].reshape(SCANS_PER_MSG, SCAN_LEN).astype(np.float64)
               + 1j * flat[half:].reshape(SCANS_PER_MSG, SCAN_LEN).astype(np.float64))
        power = np.abs(np.fft.fftshift(np.fft.fft(sig, axis=1), axes=1)) ** 2
        out.append(np.concatenate([power.sum(0), (power ** 2).sum(0)]).tolist())
    return pd.Series(out)


partials_df = kafka_df.select(message_partials(col("value")).alias("partials"))


folded_schema = StructType([
    StructField("partials", ArrayType(DoubleType())),
    StructField("n_scans", LongType()),
])


def fold_partition(arrow_batches):
    total, n = None, 0
    for pdf in arrow_batches:
        pdf = pdf.dropna(subset=["partials"])         # drop the skipped ones
        if pdf.empty:
            continue
        chunk = np.stack(pdf["partials"].to_numpy()).sum(axis=0)
        total = chunk if total is None else total + chunk
        n += len(pdf) * SCANS_PER_MSG
    if total is not None:
        yield pd.DataFrame([{"partials": total.tolist(), "n_scans": n}])


folded_df = partials_df.mapInPandas(fold_partition, schema=folded_schema)


# reduce: avg + std per bin, one row per micro-batch.
output_schema = StructType([
    StructField("freq_hz",   ArrayType(DoubleType())),
    StructField("avg_power", ArrayType(DoubleType())),
    StructField("std_power", ArrayType(DoubleType())),
    StructField("n_scans",   LongType()),
])


def summarize_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    total = np.stack(pdf["partials"].to_numpy()).sum(axis=0)
    s1, s2 = total[:SCAN_LEN], total[SCAN_LEN:]
    n = int(pdf["n_scans"].sum())
    mean = s1 / n
    var = np.maximum(s2 / n - mean ** 2, 0.0)          # guard fp noise
    return pd.DataFrame([{
        "freq_hz": FREQ_HZ,
        "avg_power": mean.tolist(),
        "std_power": np.sqrt(var).tolist(),
        "n_scans": n,
    }])


def process_batch(batch_df, batch_id):
    (batch_df
        .groupBy(lit(1).alias("dummy"))
        .applyInPandas(summarize_batch, schema=output_schema)
        .select(to_json(struct(lit(batch_id).alias("batch_id"),
                               "freq_hz", "avg_power", "std_power", "n_scans")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", "topic_results")
        .save())


query = (
    folded_df.writeStream
    .foreachBatch(process_batch)
    .start()
)

query.awaitTermination()