import typing
import logging


def set_debug_logger(format: str = None):
    root_logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(format or '%(name)-40s %(message)s')
    handler.setFormatter(formatter)
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)

    logging.getLogger('numba').setLevel(logging.WARNING)
