#!/usr/bin/env python
# -*- coding: utf-8 -*-


from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test',
    auto_offset_reset='earliest',
    bootstrap_servers=['10.0.8.206:9092'],
    #api_version=(0, 10, 1),
    consumer_timeout_ms=300
)
while 1:
    for msg in consumer:
        print(msg.value)

if consumer is not None:
    consumer.close()
