from redis import Redis
from dpq import DPQ
import time
import sys


def _now():
    '''Get the current time, as an integer UTC timestamp.'''
    return int(time.mktime(time.gmtime()))


def main():
    redis = Redis()
    queue_name = sys.argv[1]
    dpq = DPQ(redis=redis, queue_name=queue_name)

    print(f'monitoring f{queue_name}')

    while True:
        dpq.enqueue_delayed()
        time.sleep(1)



if __name__ == '__main__':
    main()
