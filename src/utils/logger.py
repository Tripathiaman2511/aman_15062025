import logging
import sys

def get_logger(name: str = "app"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.ERROR)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger