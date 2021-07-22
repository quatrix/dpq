from redis import Redis
from dpq import RpqLua
import time
import sys


def _now():
    '''Get the current time, as an integer UTC timestamp.'''
    return int(time.mktime(time.gmtime()))


def main():
    redis = Redis()
    client = RpqLua(redis)
    queue_name = sys.argv[1]

    print(f'monitoring f{queue_name}')
    while True:
        client.eval('enqueue_delayed', queue_name, _now())
        time.sleep(1)



if __name__ == '__main__':
    main()
