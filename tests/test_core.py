
import celery
from celery import Celery

from dhub.core import TaskContext, TaskFactory


def test_taskcontext(tmp_path):
    name = "ftpsend1"

    context = TaskContext(name=name, base_data_dir=tmp_path)
    assert context.name == name
    assert context.base_data_dir == str(tmp_path)
    assert context._job_kwargs == {}
    assert context.working_dir.exists()
    assert context.input_dir.exists()
    assert context.output_dir.exists()
    assert context.tmp_dir.exists()

    j = context.to_json()
    assert isinstance(j, dict)

    nj = TaskContext.from_json(j)
    assert isinstance(nj, TaskContext)
    assert nj.name == context.name
    assert nj.base_data_dir == context.base_data_dir
    assert nj._job_kwargs == context._job_kwargs


def test_taskfactory(tmp_path):
    def ftpsend(self, context, fp):
        pass

    app = Celery()

    ftpsend1 = TaskFactory(
        app=app, task_function=ftpsend,
        context=TaskContext(name="ftpsend_hostname1", base_data_dir=tmp_path, hostname="host1")
    )
    ftpsend2 = TaskFactory(
        app=app, task_function=ftpsend,
        context=TaskContext(name="ftpsend_hostname1", base_data_dir=tmp_path, hostname="host2")
    )

    for factory in ftpsend1, ftpsend2:
        assert isinstance(factory, TaskFactory)
        assert isinstance(factory.task, celery.Task)

        assert isinstance(factory.task_signature, celery.canvas.Signature)
        assert factory.task_signature.args == ()
        assert 'context' in factory.task_signature.kwargs.keys()

        assert factory.queue_name == factory.context.name
        assert isinstance(factory.celery_worker_args, dict)
        assert set(factory.default_celery_worker_args.keys()).issubset(
            set(factory.celery_worker_args.keys())
        )
