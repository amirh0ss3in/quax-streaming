# QUAX Kafka Streaming FFT Consumer

## Files

- `quax_kafka_consumer_v2.py` — the current, working Spark Structured Streaming consumer.
  Reads raw I/Q scan batches from Kafka topic `topic_stream`, computes an FFT power
  spectrum per scan, reduces to a running average + standard deviation per batch
  (fully distributed via RDD `persist`/`reduce`, nothing collected to the driver
  except the final small per-bin arrays), and publishes the result as JSON to
  `topic_results`.

  Confirmed working end-to-end on the CloudVeneto cluster (27 Aug 2026): producer.py
  running in tmux session `producer`, this script running in tmux session `consumer`,
  verified via `kafka-console-consumer.sh --topic topic_results` receiving real
  batches (e.g. `batch_id 37, n_scans 3936`) with populated frequency/value/rms arrays.

- `quax_kafka_consumer.py` (older, in this repo already) — **superseded, do not use.**
  Missing two things v2 fixes: (1) no `.persist()` on the per-batch power-spectrum RDD,
  so each micro-batch's read→unpack→FFT chain reran three times per batch instead of
  once; (2) `process_batch` returned its result dict instead of actually publishing
  it — `foreachBatch` ignores return values, so nothing was ever sent to `topic_results`.
  Kept for reference / to show the before/after, but should not be run.

## How to run (on cluster master)

```bash
export PYSPARK_PYTHON=/home/ubuntu/pyvenv/bin/python3
export PYSPARK_DRIVER_PYTHON=/home/ubuntu/pyvenv/bin/python3
python3 quax_kafka_consumer_v2.py
```

Run inside its own tmux session (`tmux new -s consumer`) so it survives an SSH
disconnect. Checkpoint state persists to `/home/ubuntu/quax_streaming_checkpoint`
(gitignored) — deleting that directory resets the stream to start from the latest
Kafka offset again on next run.

## Known open items (not yet done)

- Not yet verified via Spark UI (`localhost:4040`) whether `persist()` measurably
  shortened batch processing time vs. the old version — logically it should
  (eliminates 2 of 3 redundant recomputations per batch) but this hasn't been
  checked with real timing numbers yet.
- `producer.py`'s memory footprint (~2GB RSS preloading all file pairs into RAM)
  on the ~3.8GB master VM is a real risk — not yet addressed.
- `producer.py`'s current send rate (~256 MiB/s stress-test target) doesn't match
  the project's required tunable rates (16/32/8 MB/s) — not yet parametrized.
- No delivery-error handling on `producer.send()` in producer.py.
- Batch-mode exercise (loading one raw file pair directly from S3 into the full
  cluster via `binaryFile`, per Section 6 Task 1) — separate notebook, still has
  unfilled TODO sections, not part of this consumer.
