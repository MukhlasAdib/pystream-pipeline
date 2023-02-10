import logging


def create_default_logger(name: str) -> logging.Logger:
    formatter = logging.Formatter(
        fmt="[%(name)s] %(asctime)s %(levelname)s: %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    return logger


LOGGER = create_default_logger("PyStream")


def set_default_logger(logger: logging.Logger) -> None:
    global LOGGER
    LOGGER = logger
