from pyspark.sql import SparkSession
import numpy as np
import json

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

spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext


SCAN_LEN = 2**11          # 2048 samples per FFT scan
SCANS_PER_MSG = 32        # must match producer.py's SCANS_PER_MSG
FS = 2e6                  # ADC readout rate, 2 MS/s (Section 6.2)

KAFKA_BOOTSTRAP = "10.67.22.111:9092"   # same as producer.py's KAFKA_BOOTSTRAP_SERVERS
INPUT_TOPIC = "topic_stream"
OUTPUT_TOPIC = "topic_results"

CHECKPOINT_DIR = "/home/ubuntu/quax_streaming_checkpoint"


def unpack_batch(value):
    """One Kafka message's raw bytes -> list of SCANS_PER_MSG complex scans."""
    flat = np.frombuffer(value, dtype="<f4")
    half = len(flat) // 2
    i_scans = flat[:half].reshape(SCANS_PER_MSG, SCAN_LEN)
    q_scans = flat[half:].reshape(SCANS_PER_MSG, SCAN_LEN)
    complex_scans = i_scans + 1j * q_scans
    return list(complex_scans)


def scan_power_spectrum(scan):
    """One complex scan (2048,) -> its power spectrum (2048,)."""
    fft_vals = np.fft.fft(scan)
    shifted = np.fft.fftshift(fft_vals)
    return np.abs(shifted) ** 2


from kafka import KafkaProducer

results_producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP])

# v3: run-cumulative state. Lives in this driver process for the life of the
# query. Each batch's (n, mean, M2) is folded into the running total with
# Chan's parallel-variance combination formula -- we never re-read old scans,
# only merge small per-bin summary arrays batch by batch.
#
# Known limitation: this state is plain Python memory, NOT covered by
# checkpointLocation. If this script restarts, Kafka offsets correctly
# resume from the checkpoint, but cumulative_state resets to zero -- the
# cumulative spectrum would silently restart from that point on. Acceptable
# for now (documented here + in README_consumer.md); a more robust version
# would periodically persist this dict to disk and reload it on startup.
cumulative_state = {
    "n": 0,
    "mean": np.zeros(SCAN_LEN),
    "M2": np.zeros(SCAN_LEN),
}


def update_cumulative(state, n_batch, mean_batch, M2_batch):
    """Merge one batch's (n, mean, M2) into the running cumulative state.

    M2 here means "sum of squared deviations from the mean" (so
    std = sqrt(M2 / n)) -- this is Chan/Welford's parallel-variance
    combination formula, the standard way to merge two summaries without
    ever touching the raw data again.
    """
    if state["n"] == 0:
        state["n"] = n_batch
        state["mean"] = mean_batch.copy()
        state["M2"] = M2_batch.copy()
        return

    n_cum = state["n"]
    mean_cum = state["mean"]
    M2_cum = state["M2"]

    delta = mean_batch - mean_cum
    n_total = n_cum + n_batch

    mean_new = mean_cum + delta * (n_batch / n_total)
    M2_new = M2_cum + M2_batch + (delta ** 2) * (n_cum * n_batch / n_total)

    state["n"] = n_total
    state["mean"] = mean_new
    state["M2"] = M2_new


def process_batch(batch_df, batch_id):
    n_messages = batch_df.count()
    if n_messages == 0:
        print(f"[batch {batch_id}] empty, skipping")
        return

    # message bytes -> RDD, then unpack each message into its 32 scans
    scansRDD = (
        batch_df.select("value").rdd
        .map(lambda row: row.value)
        .flatMap(unpack_batch)
    )

    # Map phase: FFT + power spectrum, one per scan
    powerRDD = scansRDD.map(scan_power_spectrum)
    powerRDD.persist()

    # Reduce phase, pass 1: this batch's average power spectrum
    n_scans = powerRDD.count()
    summed_power = powerRDD.reduce(lambda a, b: a + b)
    avg_power = summed_power / n_scans

    # Reduce phase, pass 2: this batch's standard deviation per bin
    avg_power_bc = sc.broadcast(avg_power)
    summed_sq_dev = (
        powerRDD
        .map(lambda p: (p - avg_power_bc.value) ** 2)
        .reduce(lambda a, b: a + b)
    )
    std_power = np.sqrt(summed_sq_dev / n_scans)

    powerRDD.unpersist()

    # v3: fold this batch into the run-cumulative stats. summed_sq_dev IS
    # this batch's M2 already (sum of squared deviations), no need to
    # recompute it from std_power.
    update_cumulative(cumulative_state, n_scans, avg_power, summed_sq_dev)
    cumulative_std = np.sqrt(cumulative_state["M2"] / cumulative_state["n"])

    freq_axis = np.fft.fftshift(np.fft.fftfreq(SCAN_LEN, d=1 / FS))

    print(
        f"[batch {batch_id}] {n_messages} Kafka messages -> {n_scans} scans | "
        f"avg power range [{avg_power.min():.4g}, {avg_power.max():.4g}] | "
        f"mean std {std_power.mean():.4g} | "
        f"cumulative n_scans={cumulative_state['n']} cumulative mean std {cumulative_std.mean():.4g}"
    )

    msg_json = {
        "batch_id": batch_id,
        "n_scans": n_scans,
        # v3 note: renamed from "average" (in v2) to "batch" to sit clearly
        # alongside the new "cumulative" section -- if a dashboard/consumer
        # was already built against v2's "average" key, it needs updating.
        "batch": {
            "frequency": freq_axis.tolist(),
            "value": avg_power.tolist(),
            "rms": std_power.tolist(),
        },
        "cumulative": {
            "n_scans_total": cumulative_state["n"],
            "frequency": freq_axis.tolist(),
            "value": cumulative_state["mean"].tolist(),
            "rms": cumulative_std.tolist(),
        },
    }

    results_producer.send(topic=OUTPUT_TOPIC, value=json.dumps(msg_json).encode())
    results_producer.flush()


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


query = (
    kafka_df
    .writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="2 seconds")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

query.awaitTermination()


query.stop()


results_producer.close()
spark.stop()
