# ! pip install kafka-python

KAFKA_BOOTSTRAP_SERVERS = ['10.67.22.111:9092']

import os
import time
import boto3
import numpy as np

s3 = boto3.client(
    's3',
    endpoint_url='https://cloud-areapd.pd.infn.it:5210',
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY']
)

BUCKET = 'quax'

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
    
        response = s3.get_object(Bucket=BUCKET, Key=idata)
        raw_bytes = response['Body'].read()
        flat = np.frombuffer(raw_bytes, dtype='<f4')
        i_table = flat.reshape(4096, 2048)
    
        # همون کار برای کیو
    
        response_q = s3.get_object(Bucket=BUCKET, Key=qdata)
        raw_bytes_q = response_q['Body'].read()
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
