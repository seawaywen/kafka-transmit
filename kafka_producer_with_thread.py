#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import time
import random
import string

from kafka import KafkaProducer
from kafka.errors import KafkaError

LETTERS = string.ascii_letters

class MyThread(threading.Thread):
    def __init__(self, thread_id, name, counter):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.counter = counter

    def run(self):
        print(f'starting {self.name}')
        produce_message(self.name, self.counter)
        print(f'exiting {self.name}')

def gen_name():
    s = ''
    for i in range(32):
        randint = random.randint(0, len(LETTERS) - 1)
        s += LETTERS[randint]
    return s

producer = KafkaProducer(
    bootstrap_servers=['10.0.8.206:9092'])

def produce_message(thread_name, counter):
    for i in range(2000):
        name = gen_name()
        _message = '----->[put] [{}]{}'.format(i, name)
        #print(_message)
        message = bytes(f'[{i}]{_message}', encoding='utf-8')
        #message = bytes(f'[{}]message_{i}', encoding='utf-8')
        #print(f'threading:{thread_name}: {message}')
        future = producer.send('test', message)
        try:
            record_metadata = future.get(timeout=3)
            #print(record_metadata)
        except KafkaError as e:
            print(e)



def main(thread_number=2):
    pool = []
    for i in range(thread_number):
        my_thread = MyThread(i, f'thread-{i}', i)
        my_thread.start()
        pool.append(my_thread)

    for my_thread in pool:
        my_thread.join()

print('Exiting main thread')


if __name__ == '__main__':
    s0 = time.time()
    main(thread_number=50)
    print(f'{round(time.time() - s0, 4)}')

    producer.flush()
