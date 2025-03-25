import logging
from logging.handlers import TimedRotatingFileHandler
import sys

def setup_logger():
    """
    Returns a logger that prints INFO+ to stdout, and writes DEBUG+ to a daily-rotated file.
    """
    logger = logging.getLogger('labbuild')
    logger.setLevel(logging.DEBUG)  # capture everything; handlers filter

    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler — INFO+
        stream = logging.StreamHandler(sys.stdout)
        stream.setLevel(logging.INFO)
        stream.setFormatter(formatter)
        logger.addHandler(stream)

        # File handler — DEBUG+
        file = TimedRotatingFileHandler(
            'application.log',
            when='midnight',
            interval=1,
            backupCount=7
        )
        file.setLevel(logging.DEBUG)
        file.setFormatter(formatter)
        logger.addHandler(file)

    return logger
