from typing import Any
from pydantic import BaseModel
from .RpqLua import RpqLua
from typing import Callable
import time
import cloudpickle

def _now():
    '''Get the current time, as an integer UTC timestamp.'''
    return int(time.mktime(time.gmtime()))

class Task(BaseModel):
    payload: str
    attempt: int
    group_id: str
    expires: int
    remove: Callable
    set_invisibility: Callable


class DPQ(BaseModel):
    """
    Delayed Priority Queue
    """
    redis: Any
    queue: Any
    queue_name: str
    default_visibility: int = 10
    default_retries: int = 5

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.queue = RpqLua(self.redis)

    def push(self, task: str, priority: float = 0, delay: int = 0, retries: int = None, group_id: str = None):
        """Push a task on the queue

        Pushes a task on the queue with an optional priority and a delay.
        The queue is a Redis Sorted Set so insert complexity is O(log n)
        where n is numbers of tasks in the queue.

        Since it's a set you get deduplication for free and pushing the same
        task more than once will only update its priority and delay.

        Args:
            task: any object `cloudpickle` can serialize
            priority: float (Redis uses 64bit percision doubles for scores)
            delay: int (number of seconds to wait before task becomes available to workers)
        """
        assert group_id != '0', '0 is reserved to indicate not part of a group'

        if group_id is None:
            group_id = '0'

        if delay > 0:
            delay = _now() + delay

        task = cloudpickle.dumps(task)

        self.queue.eval(
            'push', 
            self.queue_name,
            task, 
            priority, 
            delay, 
            retries or self.default_retries, # FIXME: isn't there a fancy way of doing that?
            group_id
        )

    def get_size(self):
        """Returns size of queue

        Returns total number of tasks, runnable and delayed
        """

        return self.queue.eval('get_size', self.queue_name)

    def enqueue_delayed(self):
        """Enqueue delayed

        Takes tasks in invisible queue ready to be run and moves
        them to runnable queue.
        """

        self.queue.eval('enqueue_delayed', self.queue_name, _now())

    def delay_group(self, group_id: str, delay: int):
        """Set delay for a group_id

        all tasks with same group_id will be delayed 
        for `delay` seconds from when function is called
        """

        # FIXME: maybe/probably better to use absolute time 
        #delay = _now() + delay

        self.queue.eval('delay_group', self.queue_name, group_id, _now() + delay, delay)

    def pop(self) -> Task:
        """Pops the highest priority task from the queue

        Pops the highest priority task from the queue makes it invisible from other
        workers for the defined `default_invisibility` time.

        When invisibilty expires task will become visible again and other 
        workers could process it.
        """

        invisible_until = _now() + self.default_visibility
        task = self.queue.eval('pop', self.queue_name, invisible_until)

        if task is None:
            return

        payload, group_id, priority, attempt = task

        def remove():
            self.queue.eval('remove_from_delayed_queue', self.queue_name, payload, group_id, priority)

        def set_invisibility(seconds: int):
            seconds = _now() + seconds
            self.queue.eval('set_visibility', self.queue_name, payload, group_id, priority, seconds)

        return Task(
            payload=cloudpickle.loads(payload),
            attempt=attempt,
            expires=invisible_until,
            group_id=group_id,
            remove=remove,
            set_invisibility=set_invisibility,
        )
