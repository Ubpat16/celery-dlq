# tasks.py
from celery import Celery, shared_task
from kombu import Exchange, Queue
from celery.exceptions import Reject, Retry

from celery import bootsteps

default_queue_name = 'default'
default_exchange_name = 'default'
default_routing_key = 'default'
deadletter_suffix = 'deadletter'
deadletter_queue_name = default_queue_name + f'.{deadletter_suffix}'
deadletter_exchange_name = default_exchange_name + f'.{deadletter_suffix}'
deadletter_routing_key = default_routing_key + f'.{deadletter_suffix}'


class DeclareDLXnDLQ(bootsteps.StartStopStep):
    """
    Celery Bootstep to declare the DL exchange and queues before the worker starts
        processing tasks
    """
    requires = {'celery.worker.components:Pool'}

    def start(self, worker):
        app = worker.app

        # Declare DLX and DLQ
        dlx = Exchange(deadletter_exchange_name, type='direct')

        dead_letter_queue = Queue(
            deadletter_queue_name, dlx, routing_key=deadletter_routing_key)

        with worker.app.pool.acquire() as conn:
            dead_letter_queue.bind(conn).declare()


app = Celery(
    'tasks',
    broker='amqp://guest@localhost:5672//',
    backend='redis://localhost:6379/0')

default_exchange = Exchange(default_exchange_name, type='direct')
default_queue = Queue(
    default_queue_name,
    default_exchange,
    routing_key=default_routing_key,
    queue_arguments={
        'x-dead-letter-exchange': deadletter_exchange_name,
        'x-dead-letter-routing-key': deadletter_routing_key
    })

app.conf.task_queues = (default_queue, )

# Add steps to workers that declare DLX and DLQ if they don't exist
app.steps['worker'].add(DeclareDLXnDLQ)

app.conf.task_default_queue = default_queue_name
app.conf.task_default_exchange = default_exchange_name
app.conf.task_default_routing_key = default_routing_key


@app.task
def add(x, y):
    return x + y


@app.task(acks_late=True)
def div(x, y):
    try:
        z = x / y
        return z
    except ZeroDivisionError as exc:
        raise Reject(exc, requeue=False)

@shared_task
def new_task():
    try:
        raise ValueError("new task exception")
    except Exception as exc:
        raise Retry("retrying task")
    
"""

celery -A tasks worker --loglevel=info
# open ipython
from tasks import add, div
add.delay(1,2)
div.delay(2,1)
div.delay(2,0) # throws ZeroDivisionError

"""