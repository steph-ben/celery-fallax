"""
A sensor that watch filesystem events

Based on watchdog library https://github.com/gorakhargosh/watchdog
"""
import logging
import shutil
from pathlib import Path

from kombu.exceptions import OperationalError
from watchdog.events import PatternMatchingEventHandler, FileSystemEvent, EVENT_TYPE_MOVED
from watchdog.observers import Observer


logger = logging.getLogger(__name__)


class EventsToWorkflows(PatternMatchingEventHandler):
    def __init__(self, input_dir: Path, workflows: list):
        """
        Subclass watchdog event handler, that will:
            - look for filesystem events in a specific directory
            - copy new files to each workflow input directory
            - run workflow against file
            - unlink original file

        Input arguments are:
            - input_dir is the directory to watch
            - workflows is a list of tuple of (celery task, TaskContext)

        The TaskContext is used to know where to copy the input file

        :param input_dir:
        :param workflows:
        """
        self.input_dir = input_dir
        self.workflows = workflows

        super(EventsToWorkflows, self).__init__(
            patterns='*',
            ignore_patterns=("*.tmp",),
            ignore_directories=True,
            case_sensitive=False
        )

    def on_moved(self, event):
        self.handle_file(event)

    def on_modified(self, event):
        self.handle_file(event)

    def handle_file(self, event: FileSystemEvent):
        fp = Path(event.src_path)
        if event.event_type == EVENT_TYPE_MOVED:
            fp = Path(event.dest_path)

        logger.debug(f"{fp} - File detected")

        meta = {
            'input_dir': str(self.input_dir.absolute()),
            'input_subdir': str(fp.relative_to(self.input_dir).parent),
            'original_filename': fp.name
        }

        for wf, context in self.workflows:
            wf_fp = context.input_dir / fp.name
            logger.debug(f"Copying {fp} to {wf_fp} ...")
            shutil.copy(fp, wf_fp)
            logger.info(f"{fp} - Launching {context.name}({wf_fp}) ...")

            # TODO: if wf is only a function, just call it
            try:
                wf.delay(
                    fp=str(fp),
                    meta=meta
                )
            except OperationalError as e:
                logger.error(f"Unable to send task {context.name} : {str(e)}")

        logger.debug(f"Deleting {fp} ...")
        fp.unlink()


class FilesystemSensor:
    """
    Daemon event handler
    """
    def __init__(self, input_dir: str, workflows: list):
        self.input_dir = Path(input_dir)
        self.observer = Observer()
        self.event_handler = EventsToWorkflows(self.input_dir, workflows)

        if not self.input_dir.exists():
            self.input_dir.mkdir(parents=True)

    def start(self):
        logger.info(f"Start watching {self.input_dir} ...")
        self.observer.schedule(self.event_handler, str(self.input_dir))

        self.observer.start()
        try:
            while self.observer.is_alive():
                self.observer.join()
        except KeyboardInterrupt:
            self.observer.stop()
