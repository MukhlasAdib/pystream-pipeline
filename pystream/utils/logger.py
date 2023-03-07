import logging


def create_default_logger(name: str) -> logging.Logger:
    formatter = logging.Formatter(
        fmt="[%(name)s] %(asctime)s - %(levelname)-10s: %(message)s"
    )
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


LOGGER = create_default_logger("PyStream")


def set_default_logger(logger: logging.Logger) -> None:
    global LOGGER
    LOGGER = logger
