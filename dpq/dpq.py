from typing import Any
from pydantic import BaseModel
from .redis_lua import RedisLua
from typing import Callable, Optional
import time
import cloudpickle

def _now():
    '''Get the current time, as an integer UTC timestamp.'''
    return int(time.mktime(time.gmtime()))

RESERVERD_NIL_GROUP_ID = '0'

class Task(BaseModel):
    """Task

    Attributes:
        payload (str): The body of the task, most likely serialized.
        attempt (int): Number of times this task was popped
        group_id (str): If set when pushed on the queue, the `group id` of the task
        expires (int): When task expected to become visibile again and other workers will take it.
        remove (Callable): When called removes task from the queue (usually called when done).
        set_invisibility (Callable): Called with `seconds` to extend invisibility (useful when task processing time is unknown and needs to be extended)
    """

    payload: str
    attempt: int
    group_id: Optional[str]
    expires: int
    remove: Callable
    set_invisibility: Callable


class DPQ(BaseModel):
    """
    Delayed Priority Queue
    """

    redis: Any 
    queue: Any # name of queue
    queue_name: str
    default_visibility: int = 10
    default_retries: int = 5

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.queue = RedisLua(self.redis)

    def push(self, task: Any, priority: float = 0, delay: int = 0, retries: int = None, group_id: str = None):
        """Push a task on the queue

        Pushes a task on the queue with an optional `priority`, `delay`, `retries` and `group_id`.

        The queue is a Redis Sorted Set so insert complexity is O(log n) (where n is numbers of tasks in the queue).

        Since it's a set you get deduplication for free and pushing the same task more than once will only update its priority and delay.

        Args:
            task (Any): Any object `cloudpickle` can serialize.
            priority (float): (Optional) Payload priority, 64bit float Redis uses as the item score.
            delay (int): (Otional) Number of seconds to wait before task becomes available to workers.
            retries (int): (Optional) Number of attempts before task is dropped.
            group_id (str): (Optional) The group id this task belongs to
        """

        assert group_id != RESERVERD_NIL_GROUP_ID, f'{RESERVERD_NIL_GROUP_ID} is reserved to indicate not part of a group'

        if group_id is None:
            group_id = RESERVERD_NIL_GROUP_ID

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

        Returns total number of tasks both Runnable and Delayed.
        """

        return self.queue.eval('get_size', self.queue_name)

    def enqueue_delayed(self):
        """Enqueue delayed

        Takes tasks in invisible queue ready to be run and moves them to runnable queue.
        """

        self.queue.eval('enqueue_delayed', self.queue_name, _now())

    def delay_group(self, group_id: str, delay: int):
        """Set delay for a `group_id`

        All tasks with same group_id will be delayed for `delay` seconds from when function is called

        Args:
            group_id (str): The group id of tasks to delay.
            deay (int): Number of seconds to delay them by.
        """

        # FIXME: maybe/probably better to use absolute time 
        #delay = _now() + delay

        self.queue.eval('delay_group', self.queue_name, group_id, _now() + delay, delay)

    def pop(self) -> Task:
        """Pops the highest priority task from the runnable queue

        1. Pops the highest priority task from the `runnable queue` 
        2. Makes it invisible from other workers for the defined `default_invisibility` time.
        3. Returns a Task object

        Task object has a `payload` and methods to remove the task from the queue or extand its invisibility.

        When invisibilty expires task will become visible again and other workers could process it.
        """

        # FIXME: pop should take invisibility argument

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
            group_id=None if group_id.decode() == RESERVERD_NIL_GROUP_ID else group_id.decode(),
            remove=remove,
            set_invisibility=set_invisibility,
        )
