import logging
from logging.handlers import TimedRotatingFileHandler
import sys

def setup_logger(level=logging.INFO):
    """
    Configures and returns a logger with the specified name.
    """
    logger = logging.getLogger('labbuild')
    logger.setLevel(level)  # Set the default logging level

    # Check if handlers are already configured (e.g., when importing this function in multiple modules)
    if not logger.handlers:
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # StreamHandler for stdout
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # FileHandler for logging to a file
        file_handler = TimedRotatingFileHandler('application.log', when='midnight', interval=1, backupCount=7)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger