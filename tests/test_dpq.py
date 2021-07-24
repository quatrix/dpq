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

def test_basic_push_and_pop():
    dpq = create_dpq()

    dpq.push('hey')
    task, _, _, _ = dpq.pop() 

    assert task == 'hey'

    assert dpq.pop() == (None, None, None, None)

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
    assert dpq.pop() == (None, None, None, None)

    time.sleep(1)

    dpq.enqueue_delayed()
    task, _, _, _ = dpq.pop()

    assert task == 'lol'


def test_push_with_priority():
    dpq = create_dpq()

    dpq.push('go', priority=10)
    dpq.push('ho', priority=30)
    dpq.push('lets', priority=20)
    dpq.push('hey', priority=40)

    assert dpq.get_size() == 4

    tasks = []

    for _ in range(4):
        task, _, _, _ = dpq.pop()
        tasks.append(task)

    assert tasks == ['hey', 'ho', 'lets', 'go']


def test_removing_task():
    dpq = create_dpq()

    dpq.push('lol')
    assert dpq.get_size() == 1

    task, remove_task, _, _ = dpq.pop()

    assert task == 'lol'

    # after pop() queue size is still 1
    # because a task will stay in the queue
    # until deleted

    assert dpq.get_size() == 1

    remove_task()
    assert dpq.get_size() == 0


def test_task_becomes_visible_if_worker_is_too_slow():
    default_visibility = 1
    dpq = create_dpq(default_visibility=default_visibility)

    dpq.push('lol')
    assert dpq.get_size() == 1

    task, remove_task, _, _ = dpq.pop()

    assert task == 'lol'

    # after pop() queue size is still 1
    # because a task will stay in the queue
    # until deleted

    assert dpq.get_size() == 1

    # pop() should return nothing, because
    # the task is invisible

    dpq.enqueue_delayed()
    assert dpq.pop() == (None, None, None, None)

    time.sleep(default_visibility)
    dpq.enqueue_delayed()

    task, _, _, _ = dpq.pop()

    assert task == 'lol'

def test_pop_after_setting_visibility_to_0():
    dpq = create_dpq()

    dpq.push('lol')

    task, _, set_visibility, _ = dpq.pop()
    assert task == 'lol'

    dpq.enqueue_delayed()

    assert dpq.pop() == (None, None, None, None)

    set_visibility(0)
    dpq.enqueue_delayed()

    task, _, set_visibility, _ = dpq.pop()
    assert task == 'lol'

def test_task_gets_dropped_after_retries():
    dpq = create_dpq(default_retries=2)

    dpq.push('lol')

    task, _, set_visibility, remaining_attempts = dpq.pop()
    assert task == 'lol'
    assert remaining_attempts == 1

    # so we can pop() it again
    set_visibility(0)
    dpq.enqueue_delayed()

    task, _, set_visibility, remaining_attempts = dpq.pop()
    assert task == 'lol'
    assert remaining_attempts == 0

    # so we can pop() it again
    set_visibility(0)
    dpq.enqueue_delayed()

    assert dpq.pop() == (None, None, None, None)

@pytest.mark.skip(reason="not implemented")
def test_pushing_task_already_enqueued():
    pass



@pytest.mark.skip(reason="not implemented")
def test_poping_when_rate_limit_still_in_effect():
    pass


@pytest.mark.skip(reason="not implemented")
def test_push_with_priority_and_delay():
    pass



@pytest.mark.skip(reason="not implemented")
def test_pushing_task_already_enqueued():
    pass

@pytest.mark.skip(reason="not implemented")
def test_not_calling_set_success_makes_task_visible_to_other_worker():
    pass

