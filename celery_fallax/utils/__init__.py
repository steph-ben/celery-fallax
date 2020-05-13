import logging


class Event(object):
    """
    Simple Event object from watchdog event dict
    """
    def __init__(self, *args, **kwargs):
        self._dest_path = None
        self._src_path = None
        self.__dict__.update(kwargs)

    @property
    def filepath(self):
        """
        Return filepath of existing file
        :return:
        """
        if self._dest_path:
            # watchdog.events.FileSystemMovedEvent
            return self._dest_path
        else:
            # watchdog.events.FileCreatedEvent or FileMovedEvent
            return self._src_path


class TaskLogFormatter(logging.Formatter):
    """
    Logging Formatter that adds:
        - celery specific context (task name and task id)
        - fallax specific context (file currently processed)

    For more details on logging/celery black magic,
    cf. https://www.distributedpython.com/2018/11/06/celery-task-logger-format/
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            from celery._state import get_current_task
            self.get_current_task = get_current_task
        except ImportError:
            self.get_current_task = lambda: None

    def format(self, record):
        task = self.get_current_task()
        if task and task.request:
            record.__dict__.update(task_id=task.request.id,
                                   task_name=task.name)
        else:
            record.__dict__.setdefault('task_name', '')
            record.__dict__.setdefault('task_id', '')

        if not hasattr(record, "fp"):
            record.__dict__.setdefault('fp', '')

        return super().format(record)


def update_logger_context(**kwargs):
    """
    Add extra parameter to logging

    Note that the LogFormatter and the format should be
    updated accordignely

    :param kwargs:
    :return:
    """
    def record_factory(*args, **_kwargs):
        record = logging.LogRecord(*args, **_kwargs)
        for k, v in kwargs.items():
            setattr(record, k, v)
        return record
    logging.setLogRecordFactory(record_factory)

