# Delayed Priority Queue

## WORK IN PROGRESS

not tested, not used yet, just for fun


## What?

A Python library that implementes a priority task queue over Redis Sorted Sets with a visibility delay.

Since task order is maintained by using a Sorted Set most operations have a O(log n) complaxity, so probably 
don't put too much tasks in the queue.

When task is popped it doesn't get deleted but becomes invisible for a set duration that can be updated during 
task processing.

When visibility expires task becomes visible again and other workers might take it.

## Why?

I just couldn't find something that has both delay execution and task priority that could be any int

## Use case?

You're scraping an API that has a very low limit of how many requests you can make in a minute and you 
want to make sure you first get the resources that you can about the most.

* You can assign each task an arbitrary priority (for example if you care about the most recent items, 
    you can use a timestamp as a priority)
* When you reach the rate limit, you can put the task back in the queue and set its delay until rate limit replanishes

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
    task, on_success, set_visibility = q.pop()

    if task is None:
        print('queue is empty')
        time.sleep(1)
        continue

    print(f'got task {task=}')

    try:
        process(task):
        on_success()
    except RateLimitError:
        delay = random.randrange(50)
        print(f'setting delay for {delay} for {task=}')
        set_visibility(delay)
```


## Delayed Tasks Scheduler

One instance of `delayed_tasks_scheduler` should be running to monitor for
delayed tasks ready to be enqueued.

```bash
poetry run python delayed_tasks_scheduler.py
```

## Limitations

* Currently there's only one queue per instance
* Not blocking when queue is empty, so polling is required
* Not returning any metadata like number of retries
* No deadletter yet
* O(log n) when pushing a task on the queue or updating priority/visibility
* need to start a `delayed_tasks_scheduler` deamon
* there should probably be some configuration for this deamon, currently some things are hardcoded.

