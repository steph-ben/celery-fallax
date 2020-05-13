import logging

from celery import Celery
from celery.app.log import TaskFormatter
from celery.signals import after_setup_logger

from celery_fallax.cli.main import REDIS_URL, LOG_LEVEL, LOG_LEVEL_CELERY


class Fallax(Celery):
    broker_suffix = "/0"
    backend_suffix = "/1"

    def __init__(self, redis_url, **kwargs):
        super(Fallax, self).__init__(
            main='fallax',
            broker=redis_url + self.broker_suffix,
            backend=redis_url + self.backend_suffix,
            **kwargs
        )

        self.conf.update(
            result_expires=3600,
        )

        # Ensure this app will used when calling celery.current_app ...
        # to avoid having celery default app sometimes
        self.set_default()


app = Fallax(redis_url=REDIS_URL)


@after_setup_logger.connect
def setup_loggers(*args, **kwargs):
    """
    Configure application-wide logger. This is triggered after celery initialisation.

    We setup an application-wide logger that output to stdout, with a custom format (task context, file processed).

    :param args:
    :param kwargs:
    :return:
    """
    logger = logging.getLogger()
    logger.handlers = []
    sh = logging.StreamHandler()
    sh.setFormatter(
        TaskFormatter(
            '%(asctime)s - %(task_id)s - %(task_name)s - %(fp)s - %(name)s - %(levelname)s - %(message)s'
        )
    )
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(sh)

    for logger_name in "celery", "kombu":
        _logger = logging.getLogger(logger_name)
        _logger.setLevel(LOG_LEVEL_CELERY)

