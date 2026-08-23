KAFKA_BOOTSTRAP_SERVERS = ['10.67.22.111:9092']

import os
import time
import numpy as np

DATA_DIR = '/home/ubuntu/quax_data'   # پوشه‌ای که داده‌ها اینجا دانلود شدن

from kafka.admin import KafkaAdminClient, NewTopic
kafka_admin = KafkaAdminClient(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
)

a_new_topic = NewTopic(name='topic_stream',   ### سه تا پارتیشن گذاشتم چون سه تا نود داریم
                       num_partitions=3,
                       replication_factor=1)

kafka_admin.create_topics(new_topics=[a_new_topic])

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

while 1:
    for file_index in range(31):
        idata = f'duck_i_{file_index:05d}.dat'
        qdata = f'duck_q_{file_index:05d}.dat'

        with open(f'{DATA_DIR}/{idata}', 'rb') as f: # فایل از روی مستر خوانده میشود
            raw_bytes = f.read()
        flat = np.frombuffer(raw_bytes, dtype='<f4')
        i_table = flat.reshape(4096, 2048)

        # همون کار برای کیو
        with open(f'{DATA_DIR}/{qdata}', 'rb') as f:
            raw_bytes_q = f.read()
        flat_q = np.frombuffer(raw_bytes_q, dtype='<f4')
        q_table = flat_q.reshape(4096, 2048)

        for scan_index in range(4096):
            iscan = i_table[scan_index]
            qscan = q_table[scan_index]
            scan_id = f'{file_index}_{scan_index}'
            value = iscan.tobytes() + qscan.tobytes()
            producer.send(topic='topic_stream',
                          key=scan_id.encode(),
                          value=value)
        producer.flush()
        time.sleep(4)
