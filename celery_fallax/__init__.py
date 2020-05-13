import importlib
import sys
from pathlib import Path

from celery_fallax.celery_app import app


def load_module_from_file(fp):
    fp = Path(fp)
    sys.path.append(fp.absolute().parent)
    module = importlib.import_module(fp.parent.name + "." + fp.stem)

    return module


# Settings TODO
conf = load_module_from_file("/home/steph/Code/data-integration/dhub/config/laptop.py")
