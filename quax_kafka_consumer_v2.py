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
OUTPUT_TOPIC = "topic_results"          # new: where processed spectra get published

CHECKPOINT_DIR = "/home/ubuntu/quax_streaming_checkpoint"   # new: persistent, not auto-temp


def unpack_batch(value):
    """One Kafka message's raw bytes -> list of SCANS_PER_MSG complex scans."""
    flat = np.frombuffer(value, dtype="<f4")            # (131072,) flat float32 array
    half = len(flat) // 2                                 # 65536 -- split point between I and Q
    i_scans = flat[:half].reshape(SCANS_PER_MSG, SCAN_LEN)   # (32, 2048)
    q_scans = flat[half:].reshape(SCANS_PER_MSG, SCAN_LEN)   # (32, 2048)
    complex_scans = i_scans + 1j * q_scans                 # (32, 2048), i + jq
    return list(complex_scans)   # 32 separate (2048,) arrays, one per scan


def scan_power_spectrum(scan):
    """One complex scan (2048,) -> its power spectrum (2048,)."""
    fft_vals = np.fft.fft(scan)
    shifted = np.fft.fftshift(fft_vals)
    return np.abs(shifted) ** 2


from kafka import KafkaProducer

results_producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP])


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
    powerRDD.persist()   # <-- v2 fix: compute this chain once, not once per action below

    # Reduce phase, pass 1: average power spectrum
    n_scans = powerRDD.count()
    summed_power = powerRDD.reduce(lambda a, b: a + b)
    avg_power = summed_power / n_scans

    # Reduce phase, pass 2: standard deviation per bin
    avg_power_bc = sc.broadcast(avg_power)
    summed_sq_dev = (
        powerRDD
        .map(lambda p: (p - avg_power_bc.value) ** 2)
        .reduce(lambda a, b: a + b)
    )
    std_power = np.sqrt(summed_sq_dev / n_scans)

    powerRDD.unpersist()   # release executor memory now that this batch is done with it

    freq_axis = np.fft.fftshift(np.fft.fftfreq(SCAN_LEN, d=1 / FS))

    print(
        f"[batch {batch_id}] {n_messages} Kafka messages -> {n_scans} scans | "
        f"avg power range [{avg_power.min():.4g}, {avg_power.max():.4g}] | "
        f"mean std {std_power.mean():.4g}"
    )

    msg_json = {
        "batch_id": batch_id,
        "n_scans": n_scans,
        "average": {
            "frequency": freq_axis.tolist(),
            "value": avg_power.tolist(),
            "rms": std_power.tolist(),
        },
    }

    # v2: actually publish it, instead of leaving it as an unused return value
    results_producer.send(topic=OUTPUT_TOPIC, value=json.dumps(msg_json).encode())
    results_producer.flush()


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")   # only matters on the very first run once a
                                             # checkpoint exists -- see Section 2's note
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

