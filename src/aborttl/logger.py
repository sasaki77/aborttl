from logging import getLogger, StreamHandler, DEBUG, Formatter


def get_default_logger():
    logger = getLogger(__name__)
    logger.setLevel(DEBUG)

    handler = StreamHandler()
    handler.setLevel(DEBUG)

    s = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = Formatter(s)

    logger.addHandler(handler)
    logger.propagate = False

    return logger
