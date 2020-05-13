import os
import argparse
from pathlib import Path

import celery_fallax


# Settings TODO
BASE_DIR = "/tmp/opt/datahub"
REDIS_URL = "redis://localhost:6379"
LOG_LEVEL = "DEBUG"
LOG_LEVEL_CELERY = "WARNING"


class FallaxCli:
    """
    Command line client managing required services.
    It uses supervisord for handling:
        - redis
        - sensors
        - celery workers
    """
    app_name = "fallax"
    base_dir = "/opt/datahub"
    supervisord_config = "supervisord.conf"

    conf = None
    args = None
    parser = None

    def __init__(self, conf, base_dir=None):
        self.conf = conf
        if base_dir is not None:
            self.base_dir = base_dir

        self.args = self.parse_args()

        # Ensure directories
        for path in self.run_dir, self.log_dir:
            if not path.exists():
                path.mkdir(parents=True)

        # Run
        print(f"Settings loaded : {self.conf.__name__}\n")
        if not self.args.command:
            self.parser.print_help()
        elif self.args.command == 'start':
            self.generate_supervisord_config()
            self.supervisord_run(self.args.command)
        elif self.args.command == 'restart':
            self.generate_supervisord_config()
            self.supervisord_run('update all')
        elif self.args.command == 'show_only':
            self.generate_supervisord_config()
            print(self.supervisord_config_path)
            with Path(self.supervisord_config_path).open() as fd:
                for l in fd.readlines():
                    if "command" in l:
                        print(l, end="")
            self.generate_supervisord_config()
        else:
            self.supervisord_run(self.args.command)
            self.celery_run(self.args.command)
        print("")

    def parse_args(self):
        parser = argparse.ArgumentParser(description="Datahub cli")
        parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
        parser.add_argument('-d', '--debug', action='store_true', help='Set loglevel to debug')
        parser.add_argument('-s', '--config_module', help='Configuration python module, eg. datahub.settings.laptop')
        parser.add_argument('command', nargs="*", help='Command to run, eg. start/stop/status/show_only')
        self.parser = parser

        args = parser.parse_args()
        args.command = " ".join(args.command)
        return args

    @property
    def run_dir(self):
        return Path(self.base_dir) / "run"

    @property
    def log_dir(self):
        return Path(self.base_dir) / "log"

    @property
    def supervisord_config_path(self):
        return self.run_dir / self.supervisord_config

    def generate_supervisord_config(self):
        """
        Generate supervisord config to settings.BASE_DIR/settings.RUN_DIR/'supervisord.conf'

        By default, it launch:
            - redis service
            - sensors
            - celery beat

        According to

        :return:
        """
        config = f"""
            [unix_http_server] 
            file={self.run_dir}/datahub.sock
            chmod=0777

            [supervisord]
            logfile={self.log_dir}/datahub.log
            pidfile={self.run_dir}/datahub.pid
            childlogdir={self.log_dir}
            loglevel=INFO
            nodaemon=true

            [rpcinterface:supervisor]
            supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

            [supervisorctl]
            serverurl =unix:///{self.run_dir}/datahub.sock

            [program:redis-server]
            command=redis-server --timeout 0
            autostart=true
            autorestart=true
            numprocs=1
            stdout_logfile={self.log_dir}/redis.log
            redirect_stderr=true

            [program:sensors]
            command=sensors
            autostart=true
            autorestart=true
            startsecs=10
            stopasgroup=true
            numprocs=1
            stdout_logfile={self.log_dir}/sensors.log
            redirect_stderr=true

           [program:celery_beat]
            command=celery -A {self.app_name} beat
            autostart=true
            autorestart=true
            startsecs=10
            stopasgroup=true
            numprocs=1
            stdout_logfile={self.log_dir}/celery_beat.log
            redirect_stderr=true

        """

        for queue_name, worker_args in \
                [("celery", celery_fallax.core.TaskFactory.default_celery_worker_args)] + \
                list(map(lambda x: (x.queue_name, x.celery_worker_args), self.conf.job_factory_list)):
            worker_command = self.get_worker_command(
                queue_name,
                worker_args
            )

            config += f"""
                [program:celery_worker_{queue_name}]
                command={worker_command}
                autostart=true
                autorestart=true
                startsecs=10
                stopasgroup=true
                numprocs=1
                stdout_logfile={self.log_dir}/celery_workers.log
                redirect_stderr=true

            """

        with self.supervisord_config_path.open('w') as fd:
            for line in config.splitlines(keepends=True):
                # Strip left blank
                if line != "\n":
                    line = line.lstrip()
                fd.write(line)

    def get_worker_command(self, queue_name, worker_args):
        """
        Generate appropriate celery worker command line

        :param queue_name: str
        :param worker_args: dict
        :return: str
        """
        command = f"celery -A {self.app_name} worker -l {LOG_LEVEL_CELERY} "\
                  f"--queues {queue_name} "\
                  f"--hostname worker_{queue_name}@%%h "
        for k, v in worker_args.items():
            command += f"--{k} {v} "
        return command

    def supervisord_run(self, cmd=""):
        """
        Translate systemctl-like commands to supervisord and run it

        :param cmd: str
        :return:
        """
        print(f"* Running supervisord {cmd} ...")
        if cmd == 'start':
            self.run(f"supervisord -c {self.supervisord_config_path}")
        elif cmd == 'stop':
            self.run(f"supervisorctl -c {self.supervisord_config_path} stop all")
            self.run(f"supervisorctl -c {self.supervisord_config_path} shutdown")
        elif cmd == 'restart':
            self.run(f"supervisorctl -c {self.supervisord_config_path} restart all")
        else:
            self.run(f"timeout 5 supervisorctl -c {self.supervisord_config_path} status")

    def celery_run(self, cmd=""):
        print(f"\n* Running celery {cmd} ...")
        if 'status' in cmd:
            self.run(f"timeout 5 celery -A {self.app_name} inspect registered")

    @staticmethod
    def run(cmd):
        print(cmd)
        os.system(cmd)


def main():
    cli = FallaxCli(base_dir=BASE_DIR, conf=celery_fallax.conf)


if __name__ == "__main__":
    main()
