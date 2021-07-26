from dpq import DPQ
from redis import Redis

import pytest
import random
import string
import time

def get_random_string():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))


def create_dpq(**kwargs):
    q_name = get_random_string()
    dpq = DPQ(redis=Redis(), queue_name=q_name, **kwargs)
    assert dpq.get_size() == 0

    return dpq

def test_pop_before_push():
    dpq = create_dpq()
    assert dpq.pop() == None

def test_basic_push_and_pop():
    dpq = create_dpq()

    dpq.push('hey')
    task = dpq.pop() 

    assert task.payload  == 'hey'

    assert dpq.pop() == None

def test_get_size():
    dpq = create_dpq()

    tasks = ['hey', 'ho', 'lets', 'go']

    for t in tasks:
        dpq.push(t)

    assert dpq.get_size() == len(tasks)

def test_get_size_with_delayed():
    dpq = create_dpq()

    tasks = ['hey', 'ho', 'lets', 'go']

    for t in tasks:
        dpq.push(t)

    delayed_tasks = ['hello', 'world']

    for t in delayed_tasks:
        dpq.push(t, delay=10)

    assert dpq.get_size() == len(tasks) + len(delayed_tasks)


def test_push_and_pop_with_delay():
    dpq = create_dpq()
    dpq.push('lol', delay=1)
    dpq.enqueue_delayed()

    assert dpq.get_size() == 1
    assert dpq.pop() == None

    time.sleep(1)

    dpq.enqueue_delayed()
    assert dpq.pop().payload == 'lol'
    assert dpq.pop() == None


def test_push_with_priority():
    dpq = create_dpq()

    dpq.push('go', priority=10)
    dpq.push('ho', priority=30)
    dpq.push('lets', priority=20)
    dpq.push('hey', priority=40)

    assert dpq.get_size() == 4

    tasks = [dpq.pop().payload for _ in range(4)]

    assert tasks == ['hey', 'ho', 'lets', 'go']
    assert dpq.pop() == None


def test_removing_task():
    dpq = create_dpq()

    dpq.push('lol')
    assert dpq.get_size() == 1

    task = dpq.pop()
    assert task.payload == 'lol'

    # after pop() queue size is still 1
    # because a task will stay in the queue
    # until deleted

    assert dpq.get_size() == 1

    task.remove()

    assert dpq.get_size() == 0

def test_task_becomes_visible_if_worker_is_too_slow():
    default_visibility = 1
    dpq = create_dpq(default_visibility=default_visibility)

    dpq.push('lol')
    assert dpq.get_size() == 1

    assert dpq.pop().payload == 'lol'

    # after pop() queue size is still 1
    # because a task will stay in the queue
    # until deleted

    assert dpq.get_size() == 1

    # pop() should return nothing, because
    # the task is invisible

    dpq.enqueue_delayed()
    assert dpq.pop() == None

    time.sleep(default_visibility)
    dpq.enqueue_delayed()

    assert dpq.pop().payload == 'lol'

def test_pop_after_setting_visibility_to_0():
    dpq = create_dpq()

    dpq.push('lol')

    task = dpq.pop()
    assert task.payload  == 'lol'

    dpq.enqueue_delayed()

    assert dpq.pop() == None

    task.set_invisibility(0)
    dpq.enqueue_delayed()

    assert dpq.pop().payload == 'lol'


def test_getting_next_task_when_higher_priority_task_retries_run_out():
    dpq = create_dpq(default_retries=2)

    dpq.push('lol', priority=2)
    dpq.push('heh', priority=1)

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 1

    # so we can pop() it again
    task.set_invisibility(0)
    dpq.enqueue_delayed()

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 2

    # so we can pop() it again
    task.set_invisibility(0)
    dpq.enqueue_delayed()

    task = dpq.pop()
    assert task.payload == 'heh'
    assert task.attempt == 1


def test_task_gets_dropped_after_retries():
    dpq = create_dpq(default_retries=2)

    dpq.push('lol')

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 1

    # so we can pop() it again
    task.set_invisibility(0)
    dpq.enqueue_delayed()

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 2

    # so we can pop() it again
    task.set_invisibility(0)
    dpq.enqueue_delayed()

    assert dpq.pop() == None


def test_setting_retires_per_task_overrides_default():
    dpq = create_dpq(default_retries=5)

    dpq.push('lol', retries=2)

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 1

    # so we can pop() it again
    task.set_invisibility(0)
    dpq.enqueue_delayed()

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 2

    # so we can pop() it again
    task.set_invisibility(0)
    dpq.enqueue_delayed()

    assert dpq.pop() == None


def test_pushing_task_already_enqueued():
    dpq = create_dpq()

    dpq.push('hey', delay=5)

    assert dpq.pop() == None

    # pushing a task that's already
    # exist and already delayed, 
    # should keep it delayed.
    dpq.push('hey')
    assert dpq.pop() == None

def test_pushing_same_task_second_time_restart_retries():
    dpq = create_dpq(default_retries=2)

    dpq.push('lol')

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 1

    task.remove()

    dpq.push('lol')

    task = dpq.pop()
    assert task.payload == 'lol'
    assert task.attempt == 1


def test_extending_visibility():
    dpq = create_dpq(default_visibility=1)

    dpq.push('lol')

    task = dpq.pop()
    assert task.payload == 'lol'

    dpq.enqueue_delayed()
    assert dpq.pop() == None

    # extend by 2 second should 
    # keep the task invisible and 
    # when pop() called it will get nothing
    task.set_invisibility(2)
    time.sleep(1)

    dpq.enqueue_delayed()
    assert dpq.pop() == None

def test_pushing_with_group_id():
    dpq = create_dpq()

    dpq.push('hey', group_id='aaa')
    assert dpq.pop().payload == 'hey'

def test_visibility_for_group_of_tasks():
    dpq = create_dpq()

    dpq.push('hey', group_id='aaa', priority=5)
    dpq.push('ho', group_id='aaa', priority=10)
    dpq.push('vova', priority=1)

    dpq.delay_group('aaa', 1)
    dpq.enqueue_delayed()

    assert dpq.pop().payload == 'vova'

    time.sleep(1)
    dpq.enqueue_delayed()

    assert dpq.pop().payload == 'ho'
    assert dpq.pop().payload == 'hey'
    assert dpq.pop() == None
