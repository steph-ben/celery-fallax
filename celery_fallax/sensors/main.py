"""
Command line interface for running a set of sensors that trigger tasks on event
"""
import argparse
import logging

from celery_fallax import conf


def parse_args():
    parser = argparse.ArgumentParser(description="Sensors cli")
    parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
    parser.add_argument('-d', '--debug', action='store_true', help='Set loglevel to debug')
    parser.add_argument('-s', '--config_module', help='Configuration python module, eg. datahub.settings.laptop')

    args = parser.parse_args()
    return args


def setup_logger(level=None):
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)8s - %(message)s',
        level=level
    )


def main():
    """
    It does the following:
        - parse command line arguments
        - setup logger with appropriate log levels
        - run sensors according to the configuration

    Parse command line arguments
    :return:
    """
    args = parse_args()

    fallax_logger = logging.getLogger("fallax")
    if args.debug:
        setup_logger(logging.DEBUG)
    elif args.verbose:
        # When user set --verbose, we set "fallax" logger to DEBUG and other to INFO
        setup_logger(logging.INFO)
        fallax_logger.setLevel(logging.DEBUG)
    else:
        setup_logger()
        fallax_logger.setLevel(logging.INFO)

    # TODO: Handle multiple sensors, since start() is currently blocking
    for sensor in conf.sensors:
        sensor.start()


if __name__ == "__main__":
    main()
