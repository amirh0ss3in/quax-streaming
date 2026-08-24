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

if 'topic_stream' not in kafka_admin.list_topics():
    a_new_topic = NewTopic(name='topic_stream',
                        num_partitions=8, # 3 VMs, 4x Core for master and 2x Core per the two workers = total of 8, So we need *at least* 8 partitions.
                        replication_factor=1)
    kafka_admin.create_topics(new_topics=[a_new_topic])

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

TIME_INTERVAL = 4 # seconds
DT = TIME_INTERVAL / 4096

t0 = perf_counter()          # stopwatch start
n = 0                        # messages sent so far

while True:
    for file_index in range(31):
        i_table = data_i[file_index]
        q_table = data_q[file_index]
        t1_file = perf_counter()
        for scan_index in range(4096):
            iscan = i_table[scan_index]
            qscan = q_table[scan_index]

            scan_id = f'{file_index}_{scan_index}'
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

            time.sleep(sleep_time)
                
        t2_file = perf_counter()
        t_file = t2_file-t1_file
        print(f"\rFile time: {t_file:.6f}s, error = {(t_file/TIME_INTERVAL-1)*100:.4f}%   ", end="", flush=True)
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