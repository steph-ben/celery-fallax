from pathlib import Path

import dhub
from dhub.cli.main import DhubCli


def test_cli(tmp_path):
    """
    Check that FallaxCli behave correctly

    :return:
    """
    cli = DhubCli(conf=dhub.conf, base_dir=tmp_path)

    assert isinstance(cli.run_dir, Path)
    assert isinstance(cli.log_dir, Path)

    queue_name = "my_queue"
    worker_args = {'arg1': 'value1', 'arg2': 'value2'}
    worker_command = cli.get_worker_command(queue_name, worker_args)
    assert f"celery -A {cli.app_name}" in worker_command
    assert f"--queues {queue_name}" in worker_command
    assert "--arg1 value1" in worker_command
    assert "--arg2 value2" in worker_command

    cli.generate_supervisord_config()
    assert cli.supervisord_config_path.exists()
