from celery import Celery
from celery.exceptions import Reject, TaskPredicate
from functools import wraps
from kombu import Exchange, Queue

from celery import bootsteps


class Config(object):

    APP_NAME = 'main'

    task_default_queue = '%s_celery' % APP_NAME
    task_default_exchange = "%s_celery" % APP_NAME
    task_default_exchange_type = 'direct'
    task_default_routing_key = task_default_queue
    task_create_missing_queues = False
    task_acks_late = True

    # Configuration for DLQ support
    dead_letter_exchange = '%s_dlx' % APP_NAME
    dead_letter_exchange_type = 'direct'
    dead_letter_queue = '%s_dlq' % APP_NAME
    dead_letter_routing_key = dead_letter_queue


class DeclareDLXnDLQ(bootsteps.StartStopStep):
    """
    Celery Bootstep to declare the DL exchange and queues before the worker starts
        processing tasks
    """
    requires = {'celery.worker.components:Pool'}

    def start(self, worker):
        app = worker.app

        # Declare DLX and DLQ
        dlx = Exchange(
            app.conf.dead_letter_exchange,
            type=app.conf.dead_letter_exchange_type)

        dead_letter_queue = Queue(
            app.conf.dead_letter_queue,
            dlx,
            routing_key=app.conf.dead_letter_routing_key)

        with worker.app.pool.acquire() as conn:
            dead_letter_queue.bind(conn).declare()


app = Celery('tasks', broker='pyamqp://guest@localhost//')
app.config_from_object(Config)


# Declare default queues
# We bypass the default mechanism tha creates queues in order to declare special queue arguments for DLX support
default_exchange = Exchange(
    app.conf.task_default_exchange,
    type=app.conf.task_default_exchange_type)
default_queue = Queue(
        app.conf.task_default_queue,
        default_exchange,
        routing_key=app.conf.task_default_routing_key,
        queue_arguments={
            'x-dead-letter-exchange': app.conf.dead_letter_exchange,
            'x-dead-letter-routing-key': app.conf.dead_letter_routing_key
        })

# Inject the default queue in celery application
app.conf.task_queues = (default_queue,)

# Inject extra bootstep that declares DLX and DLQ
app.steps['worker'].add(DeclareDLXnDLQ)


def onfailure_reject(requeue=False):
    """
    When a task has failed it will raise a Reject exception so
    that the message will be requeued or marked for insertation in Dead Letter Exchange
    """

    def _decorator(f):
        @wraps(f)
        def _wrapper(*args, **kwargs):

            try:
                return f(*args, **kwargs)
            except TaskPredicate:
                raise   # Do not handle TaskPredicate like Retry or Reject
            except Exception as e:
                print("Rejecting")
                raise Reject(str(e), requeue=requeue)
        return _wrapper

    return _decorator


@app.task()
@onfailure_reject()
def div(x, y):
    return x / y