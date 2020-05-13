import logging
from pathlib import Path


logger = logging.getLogger(__name__)


class TaskFactory:
    """
    Convenient factory for creating derived, named celery tasks from a single function.

    It is intended that each factory will also have:
        - a dedicated queue
        - a dedicated worker


    Say you have a function
        def ftpsend(self, context, fp):
            context = TaskContext.from_json(context)
            print(f"Sending {fp} to {context.hostname} ...")

    And you want to send files to several destination, you can write:
        ftpsend1 = TaskFactory(
            app=app, task_function=ftpsend,
            context=TaskContext(name="ftpsend_hostname1", hostname="host1")
        )
        ftpsend2 = TaskFactory(
            app=app, task_function=ftpsend,
            context=TaskContext(name="ftpsend_hostname1", hostname="host2")
        )

    And now use the task as follow
        ftpsend1.task_signature.delay(fp)

    """
    app = None
    context = None
    task_function = None
    _celery_worker_args = None
    _task = None

    default_celery_worker_args = {
        "concurrency": 8,
        "autoscale": "100,1",
        "max-memory-per-child": 10000,
    }

    def __init__(self, app, context, task_function=None, celery_worker_args=None):
        self.app = app
        self.context = context
        self.task_function = task_function
        self._celery_worker_args = celery_worker_args

        self.register_task()

    def register_task(self):
        """
        Registering a task into celery application

        :return:
        """
        if self.task is not None:
            logger.debug(f"Registering {self.task} ...")
            self.app.tasks.register(self.task)

    @property
    def task(self):
        """
        Return celery task object, with:
            - function bounded to task object (bind=True)
            - dedicated name
            - specific queue

        :return:
        """
        if self._task is None:
            if self.task_function is not None:
                self._task = self.app.task(
                    self.task_function,
                    bind=True, name=self.context.name, queue=self.queue_name,
                )

        return self._task

    @property
    def task_signature(self):
        """
        Return celery task signature, bounded with TaskContext object

        :return:
        """
        return self.task.s(
            context=self.context.to_json()
        )

    @property
    def queue_name(self):
        """
        Queue name associated to this task
        By default it's the context name

        :return:
        """
        return self.context.name

    @property
    def celery_worker_args(self):
        """
        Add
        :return:
        """
        w_args = self.default_celery_worker_args
        if self._celery_worker_args is not None:
            w_args.update(self._celery_worker_args)
        return w_args


class TaskContext:
    """
    Task execution context, not related to any framework.

    It only provides :
        - convenient properties to work in a separated directory
        - creates all directories
        - json serializer methods

    Example of usage:
        context = TaskContext("ftpsend1", "/data/hub/working")
        assert context.input_dir.exists()

    """
    name = None
    base_data_dir = None

    def __init__(self, name, base_data_dir, **kwargs):
        self.name = name
        self.base_data_dir = str(base_data_dir)
        self._job_kwargs = kwargs

        self.ensure_working_directories()

    @property
    def working_dir(self):
        return Path(self.base_data_dir) / 'working'

    @property
    def input_dir(self):
        return self.working_dir / self.name / 'in'

    @property
    def output_dir(self):
        return self.working_dir / self.name / 'out'

    @property
    def tmp_dir(self):
        return self.working_dir / self.name / 'tmp'

    def ensure_working_directories(self):
        for path in self.input_dir, self.output_dir, self.tmp_dir:
            if not path.exists():
                path.mkdir(parents=True)

    @classmethod
    def from_json(cls, json):
        c = cls(name=json['name'], base_data_dir=json['base_data_dir'])
        c._job_kwargs = json['_job_kwargs']
        return c

    def to_json(self):
        return self.__dict__
