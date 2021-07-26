from typing import Any
from pydantic import BaseModel
from .RpqLua import RpqLua
import time
import cloudpickle

def _now():
    '''Get the current time, as an integer UTC timestamp.'''
    return int(time.mktime(time.gmtime()))

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

    def push(self, task: str, priority: float = 0, delay: int = 0, group_id: str = None):
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
            self.default_retries, 
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

    def pop(self):
        """Pops the highest priority task from the queue

        Pops the highest priority task from the queue makes it invisible from other
        workers for the defined `default_invisibility` time.

        When invisibilty expires task will become visible again and other 
        workers could process it.

        Limitations:
            current implementation doesn't block when queue is empty
            instead a None task will be returned and user should wait a while
            and try again.

        Returns:
            Task: deserialized task of the highest priority
            on_success(): callback to be called when worker succesfully finishes
                handling the task, it will delete the invisible task
            set_visibility(): callback to update the tasks visibility
                this can be used to make task visible again when worker fails
                so that other workers can take it right away, or for extending
                the visibilty time if worker is still processing and processing
                duration isn't deterministic.
        """

        invisible_until = _now() + self.default_visibility
        task = self.queue.eval('pop', self.queue_name, invisible_until)

        if task is None:
            return

        payload, group_id, priority, attempt = task

        def on_success():
            self.queue.eval('remove_from_delayed_queue', self.queue_name, payload, group_id, priority)

        def set_visibility(seconds: int):
            seconds = _now() + seconds
            self.queue.eval('set_visibility', self.queue_name, payload, group_id, priority, seconds)

        return cloudpickle.loads(payload), on_success, set_visibility, attempt 

