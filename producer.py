KAFKA_BOOTSTRAP_SERVERS = ['10.67.22.111:9092']

import os
import time
import numpy as np

DATA_DIR = 'quax_data'   # our quax data was downloaded here.

from kafka.admin import KafkaAdminClient, NewTopic
kafka_admin = KafkaAdminClient(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
)

print("Print available topics:", kafka_admin.list_topics())

# NOTE: 
# Few things:
# $KAFKA_HOME is just the address of the Kafka folder, exported with `export KAFKA_HOME=/path/to/kafka`.
# Workers reach the broker via advertised.listeners=PLAINTEXT://10.67.22.111:9092 in server.properties (confirmed reachable from a worker VM with 
# python -c "from kafka import KafkaAdminClient; a = KafkaAdminClient(bootstrap_servers=['10.67.22.111:9092']); print(a.describe_cluster()); a.close()")
# topic_stream is created once, out of band:
#
#   $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 10.67.22.111:9092 \
#     --create --topic topic_stream --partitions 8 --replication-factor 1 \
#     --config retention.ms=600000 \
#     --config retention.bytes=134217728 \
#     --config segment.bytes=33554432 \
#     --config segment.ms=1000 \
#     --config file.delete.delay.ms=1000
#
# These settings may seem aggressive, but they ensure we can test the producer with small TIME_INTERVAL (and therefore high throughput) 
# without filling the disk.
# Retention only prunes closed segments, so segment.bytes must be well under
# retention.bytes or the active segment alone blows the budget. Two broker-side
# settings in server.properties (restart required, no per-topic equivalent):
#
#   log.retention.check.interval.ms=5000   # default 300000 is far too coarse at this rate
#   auto.create.topics.enable=false        # else a stray run recreates this as 1 partition, no limits
# 
# It is worth to put the restart procedure here as well:
# $KAFKA_HOME/bin/kafka-server-stop.sh
# $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
# Now confirm the topic exists well:
# $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 10.67.22.111:9092 --describe --topic topic_stream

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

data_i = np.empty((31, 4096, 2048), dtype=np.float32)
data_q = np.empty((31, 4096, 2048), dtype=np.float32)

for file_index in range(31):
    data_i[file_index] = np.fromfile(
        f'{DATA_DIR}/duck_i_{file_index:05d}.dat',
        dtype='<f4'
    ).reshape(4096, 2048)

    data_q[file_index] = np.fromfile(
        f'{DATA_DIR}/duck_q_{file_index:05d}.dat',
        dtype='<f4'
    ).reshape(4096, 2048)

print(data_i.shape, data_q.shape)

# We are going to use for the highest resolution and most accurate timer, as discussed here:
# https://docs.python.org/3/library/time.html#time.perf_counter

# " ...a clock with the highest available resolution to measure a short duration. 
#   It does include time elapsed during sleep. The clock is the same for all processes."
from time import perf_counter

TIME_INTERVAL = 0.1 # seconds

# Each message carries a slice of SCANS_PER_MSG scans instead of a single one.
# Payload layout is: all I scans of the slice, then all Q scans of the slice,
# i.e. the consumer reshapes the bytes as (2, SCANS_PER_MSG, 2048) float32.
SCANS_PER_MSG = 8 # each scan (I and Q together) is 16 KiB, and total size for message is the default which is 1 MiB. So this should be kept under SCANS_PER_MSG = 64.
N_MSGS = 4096 // SCANS_PER_MSG   # messages per file

DT = TIME_INTERVAL / N_MSGS      # now the spacing between messages, not scans

t0 = perf_counter()          # stopwatch start
n = 0                        # messages sent so far
slept = 0

while True:
    for file_index in range(31):
        i_table = data_i[file_index]
        q_table = data_q[file_index]
        t1_file = perf_counter()
        for msg_index in range(N_MSGS):
            lo = msg_index * SCANS_PER_MSG
            hi = lo + SCANS_PER_MSG

            iscan = i_table[lo:hi]
            qscan = q_table[lo:hi]

            scan_id = f'{file_index}_{msg_index}'
            value = iscan.tobytes() + qscan.tobytes()

            producer.send(
                topic='topic_stream',
                key=scan_id.encode(),
                value=value
            )
            
            n += 1
            elapsed = perf_counter() - t0        # how long we've been running
            should_be = n * DT                   # how long we should have been running
            sleep_time = max(0, should_be-elapsed) # ... so we sleep that amount, to get into schedule.
            
            if sleep_time > 0: # Probably the most important line in this code. Extremely important.
                time.sleep(sleep_time)
                slept += 1
        t2_file = perf_counter()
        t_file = t2_file-t1_file
        msg = (
            f"File time: {t_file:4.6f}s, "
            f"error = {(t_file/TIME_INTERVAL-1)*100:+.4f}%, "
            f"paced {slept/n:2.2f}% msgs"
        )
        print(f"\r{msg:<80}", end="", flush=True)
        producer.flush()

        # t0 is our origin, let's say 1 pm. By the time a file is done, n*DT says exactly
        # 4 seconds *should have* passed, but perf_counter() (the current time) usually
        # reads a bit later, because the flush and the sleep overshoots cost us extra.
        # So we move the origin back to 1 pm "with a difference": now minus the 4
        # seconds that were supposed to elapse. That way the time we overran still
        # counts as scheduled time, and the next messages don't sprint to catch up 
        # (this makes the problem of initial burst in the next files fixed!).
        # The max() keeps this one-directional: we can forgive a lost time if there is something to forgive...!
        # remember, we are hoping to get back to 1 pm + extra time cause by flush. That's all.
        # P.S: I'm assuming TIME_INTERVAL = 4 # seconds to explain this
        
        t0 = max(t0, perf_counter() - n * DT)   # forgive time lost to flush