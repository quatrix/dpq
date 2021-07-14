from dpq import DPQ
from redis import Redis
import sys
import time
import random
queue_name = 'vova'

def get_queue(name: str):
    return DPQ(redis=Redis(), queue_name=name)

def producer():
    print('starting a producer')

    q = get_queue(queue_name)

    for i in range(10):
        priority = random.randrange(50)
        delay = 10
        print(f'pushing {i=} in queue {queue_name}')

        payload = {
            'body': f'hello world {i=}',
            'priority': priority,
            'delay': delay,
        }

        q.push(payload, priority=priority, delay=delay)


def consumer():
    print('starting a consumer')

    q = get_queue(queue_name)

    while True:
        task, on_success, set_visibility = q.pop()

        if task is None:
            print('queue is empty')
            time.sleep(1)
            continue


        print(f'got task {task=}')

        #on_success()
        delay = random.randrange(50)
        print(f'setting delay for {delay} for {task=}')
        set_visibility(delay)
        


if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'consumer':
        consumer()
    elif cmd == 'producer':
        producer()
    else:
        raise Exception('argv must be consumer or producer')
