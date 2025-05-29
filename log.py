import logging


def configure_logger(logger):
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handlers = [
        logging.FileHandler(filename=f'{logger.name}.log'),
        logging.StreamHandler()
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def get_logger(name: str):
    logger = logging.getLogger(name)
    configure_logger(logger)
    return logger
