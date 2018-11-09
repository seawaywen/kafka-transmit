#!/usr/bin/env python
# -*- coding: utf-8 -*-

import queue
import random
import string
import threading
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

LETTERS = string.ascii_letters
TOPIC = 'replication-test'

KAFKA_SERVERS=[
    '10.0.8.65:9092',
    '10.0.8.206:9092',
    '10.0.8.208:9092',
]

WORKER_NUMBER = 40

MSG_LENGTH = 10000

work_queue = queue.Queue(5000)
queue_lock = threading.Lock()


class WorkerThread(threading.Thread):
    def __init__(self, thread_id, name, q, with_lock=False):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.q = q
        self.with_lock = with_lock
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS)

    def run(self):
        print(f'starting {self.name}')
        self.produce_message(self.name, self.q)
        print(f'exiting {self.name}')

    def produce_message(self, thread_name, q):
        while True:
            if self.with_lock:
                queue_lock.acquire()

            if not work_queue.empty():
                message = q.get()
                if message is None:
                    if self.with_lock:
                        queue_lock.release()
                    break

                #print(f'<--[get][{thread_name}] {message}')
                future = self.producer.send(TOPIC, message)

                if self.with_lock:
                    queue_lock.release()
                try:
                    record_metadata = future.get(timeout=3)
                except KafkaError as e:
                    print(e)
                q.task_done()
            else:
                #print('lock release and sleep 1 sec')

                if self.with_lock:
                    queue_lock.release()
                time.sleep(1)


class DataGeneratorThread(threading.Thread):
    def __init__(self, thread_id, name):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name

    def run(self):
        print(f'starting {self.name}')
        self.gen_data(work_queue)
        print(f'exiting {self.name}')

    def gen_data(self, q):
        #queue_lock.acquire()
        for i in range(MSG_LENGTH):
            name = gen_name()
            #_message = '----->[put] [{}]{}'.format(i, name)
            #print(_message)
            message = bytes(f'[{i}]{name}', encoding='utf-8')
            q.put(message)
        #queue_lock.release()


def gen_name():
    s = ''
    for i in range(1024):
        randint = random.randint(0, len(LETTERS) - 1)
        s += LETTERS[randint]
    return s


threads_pool = []


def start_workers(worker_number=1):

    for i in range(worker_number):
        my_thread = WorkerThread(i, f'worker-thread-{i}', work_queue, with_lock=False)
        my_thread.start()
        threads_pool.append(my_thread)

    d_thread = DataGeneratorThread(0, 'data-generator-thread')
    d_thread.start()
    d_thread.join()


def stop_workers(worker_number=1):
    def worker_stop_thread():
        for i in range(worker_number):
            work_queue.put(None)

    t = threading.Thread(target=worker_stop_thread)
    t.start()
    t.join()

    for worker_thread in threads_pool:
        worker_thread.join()


def main(thread_number=2):

    start_workers(worker_number=WORKER_NUMBER)

    work_queue.join()

    stop_workers(worker_number=WORKER_NUMBER)


print('Exiting main thread')


if __name__ == '__main__':
    s0 = time.time()
    main(thread_number=WORKER_NUMBER)
    print(f'{round(time.time() - s0, 4)}')

    """
without lock: 27.3176
with lock:    59.6886
    """
