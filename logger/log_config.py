import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import shutil

# Global list to store error messages
error_logs = []

def setup_logger(level=logging.INFO):
    """
    Configures and returns a logger with the specified name.
    """
    logger = logging.getLogger('labbuild')
    logger.setLevel(level)  # Set the default logging level

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # Stream Handler for stdout
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # File Handler for logging to a file
        file_handler = TimedRotatingFileHandler('application.log', when='midnight', interval=1, backupCount=7)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Custom error handler to collect error logs **without infinite recursion**
        class ErrorLogHandler(logging.Handler):
            def emit(self, record):
                if record.levelno == logging.ERROR:
                    clean_msg = record.getMessage().split(" - ERROR - ")[-1].strip()
                    if clean_msg not in error_logs:  # Avoid duplicates
                        error_logs.append(clean_msg)

        error_handler = ErrorLogHandler()
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

    return logger

# Initialize logger globally
logger = setup_logger()

def print_error_summary():
    """
    Prints a **clean** summary of all collected error messages **without timestamps**.
    """
    if not error_logs:
        return

    # Get terminal width dynamically
    terminal_width = shutil.get_terminal_size((80, 20)).columns
    separator = "=" * terminal_width
    title = " ERROR SUMMARY "

    # **Print directly to stdout instead of logging** to avoid timestamps
    print("\n" + separator)
    print(title.center(terminal_width))
    print(separator)

    for err in error_logs:
        print(f"💥 {err}")  # Print errors clearly

    print(separator + "\n")
