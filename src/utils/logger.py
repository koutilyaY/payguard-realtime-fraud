import logging
import os

def get_logger(name: str = "payguard") -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
