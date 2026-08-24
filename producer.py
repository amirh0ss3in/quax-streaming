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
                        num_partitions=6, # 3 VMs, 2x Core per VM, So we need *at least* 6 partitions.
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

while True:
    for file_index in range(31):
        i_table = data_i[file_index]
        q_table = data_q[file_index]

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

        producer.flush()
        time.sleep(4)