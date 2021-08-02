# Delayed Priority Queue

![logo](/imgs/logo.png)

## WIP Warning

Wasn't used in produciton yet!

## What?

A Python library that implements a strict arbitrary priority queue with optional delayed task execution over Redis Sorted Sets.

## Docs 

Generated API reference

[Docs!](https://quatrix.github.io/dpq/)

## Features

* Priority can be any 64bit float, anything Redis uses as Sorted Set `SCORE`.
* When pushing a task `delay` specifies seconds until task becomes visibile to workers.
* Task can have limited number of retries after which task will be dropped.
* When poping a task, it will be invisible for a specified amount of time to other workers, so in case of worker failure, eventually task gets picked by another worker.
* (EXPERIMENTAL) A task can have a `group_id` and it's possible to set a delay for an entire group of tasks, this can be useful if a group of tasks accessing a shared resource that becomes unavailable calling `delay_group` will delay all tasks of the same group.

## Why?

I just couldn't find something that has both delayed execution and arbitrary priority.

## Use case?

Scraping an API with a very low rate limit, and the elements you're scraping have a timestamp and recent elements are more important than old once. 

* Task priority can be the timestamp
* When rate limit is reached, task is delayed until rate limit replanishes.

## Example

### Producer
```python
q = DPQ(redis=Redis(), queue_name="my_queue")

for i in range(10):
    priority = random.randrange(50)
    delay = 10

    payload = {
        'body': f'hello world {i=}',
        'priority': priority,
        'delay': delay,
    }

    q.push(payload, priority=priority, delay=delay)
```

### Consumer
```python

q = DPQ(redis=Redis(), queue_name="my_queue")

while True:
    task = q.pop()

    if task is None:
        print('queue is empty')
        time.sleep(1)
        continue

    print(f'got task {task=}')

    try:
        process(task.payload):
        task.remove()
    except RateLimitError:
        delay = random.randrange(50)
        print(f'setting delay for {delay} for {task=}')
        task.set_invisibility(delay)
```


## Delayed Tasks Scheduler

One instance of `delayed_tasks_scheduler` should be running to monitor for
delayed tasks ready to be enqueued.

```bash
poetry run python delayed_tasks_scheduler.py
```


## Installation

```bash
poerty install
```

## Limitations

* Not blocking when queue is empty, so polling is required
* No deadletter yet
* O(log n) when pushing a task on the queue or updating priority/delay
* need to start a `delayed_tasks_scheduler` deamon
* setting a group delay isn't efficient.
