# QUAX Kafka Streaming FFT Consumer

## Files

- `quax_kafka_consumer_v3.py` — the current, working Spark Structured Streaming consumer.
  Reads raw I/Q scan batches from Kafka topic `topic_stream`, computes an FFT power
  spectrum per scan, and publishes two things per micro-batch to `topic_results`:
  - `"batch"` — this batch's average power spectrum + std, per bin (fully distributed
    via RDD `persist`/`reduce`, nothing collected to the driver except the final small
    per-bin arrays).
  - `"cumulative"` — the running average + std across the whole stream since it
    started, combined batch-by-batch using Chan's parallel-variance formula (no
    re-reading of old data needed, just merges each batch's own summary stats).

  Also sets `.option("failOnDataLoss", "false")` on the Kafka read — `topic_stream`
  has deliberately short retention for stress-testing, so old checkpointed offsets
  can go out of range; without this the query crashes instead of skipping ahead.

  Confirmed working end-to-end on the CloudVeneto cluster (27 Aug 2026): producer.py
  running in tmux session `producer`, this script running in tmux session `consumer`,
  ran cleanly past 100+ micro-batches with `cumulative n_scans` growing correctly
  and `cumulative` std visibly smoothing out relative to the noisier per-batch std,
  verified via `kafka-console-consumer.sh --topic topic_results`.

  **Known limitation:** the cumulative state lives only in this script's own process
  memory, not in `checkpointLocation` — if the script restarts, Kafka offsets resume
  correctly from the checkpoint but the cumulative average/std resets to zero. Not
  yet solved; would need periodically persisting the cumulative state to disk (or
  similar) and reloading it on startup.

- `quax_kafka_consumer.py` (older) — **superseded, do not use.** Missing what v3 has:
  no `.persist()` on the per-batch power-spectrum RDD (so each micro-batch's
  read→unpack→FFT chain reran three times instead of once), `process_batch` returned
  its result instead of publishing it (`foreachBatch` ignores return values, so
  nothing was ever actually sent to `topic_results`), and no cumulative averaging.
  Kept only for reference / to show the before/after; should not be run.

## How to run (on cluster master)

```bash
export PYSPARK_PYTHON=/home/ubuntu/pyvenv/bin/python3
export PYSPARK_DRIVER_PYTHON=/home/ubuntu/pyvenv/bin/python3
python3 quax_kafka_consumer_v3.py
```

Run inside its own tmux session (`tmux new -s consumer`) so it survives an SSH
disconnect. Checkpoint state persists to `/home/ubuntu/quax_streaming_checkpoint`
(gitignored). If the consumer has been stopped for a while and comes back with an
`OffsetOutOfRangeException`, the checkpointed offsets have likely already been
deleted from Kafka by `topic_stream`'s short retention — delete the checkpoint dir
(`rm -rf /home/ubuntu/quax_streaming_checkpoint`) and re-run; nothing of value is
lost since that old data isn't in Kafka anymore either way.

## Known open items (not yet done)

- Cumulative state isn't restart-safe (see limitation above).
- Not yet verified via Spark UI (`localhost:4040`) whether `persist()` measurably
  shortened batch processing time — logically it should, not yet checked with real
  timing numbers.
- `producer.py`'s memory footprint (~2GB RSS preloading all file pairs into RAM) on
  the ~7.8GB master VM is a real risk if a duplicate instance ever runs alongside it
  — already caused one OOM kill of a stray duplicate producer process.
- `producer.py`'s current send rate (~256 MiB/s stress-test target) doesn't match
  the project's required tunable rates (16/32/8 MB/s) — not yet parametrized.
- No delivery-error handling on `producer.send()` in producer.py.
- `topic_stream`'s very short retention (`segment.ms=1000`, 128MB cap) may need
  revisiting now that the consumer is meant to run continuously — worth discussing
  with the team.
- Batch-mode exercise (loading one raw file pair directly from S3 into the full
  cluster via `binaryFile`, per Section 6 Task 1) — separate notebook, still has
  unfilled TODO sections, not part of this consumer.
- Dashboard/consumer reading `topic_results` and displaying both `"batch"` and
  `"cumulative"` sections — not started yet.
