# --- START OF FILE logger/redis_handler.py ---

import logging
import redis
import json
import datetime
import traceback
import os

# --- Configuration ---
REDIS_URL_HANDLER = os.getenv("REDIS_URL", "redis://localhost:6379/0")
print(f"--- DEBUG [RedisHandler]: Using REDIS_URL: {REDIS_URL_HANDLER} ---") 

# --- Setup Logger for this Handler ---
handler_logger = logging.getLogger('labbuild.redis_handler')
handler_logger.setLevel(logging.INFO) # Set level for handler's own logs
if not handler_logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    handler_logger.addHandler(ch)


class RedisStreamHandler(logging.Handler):
    """
    Custom log handler that publishes log records to a Redis channel
    specific to a run_id for real-time streaming.
    """
    def __init__(self, run_id, redis_url=REDIS_URL_HANDLER,
                 level=logging.NOTSET):
        """
        Initializes the handler.

        Args:
            run_id (str): The unique identifier for the current run.
            redis_url (str): The connection URL for the Redis server.
            level (int): The logging level for this handler.
        """
        super().__init__(level)
        self.run_id = run_id
        self.redis_url = redis_url
        # Define a unique channel name for each run
        self.channel_name = f"log_stream::{self.run_id}"
        self.redis_client = None
        self._connect()

    def _connect(self):
        """Establishes connection to Redis."""
        if not self.redis_url:
            handler_logger.error(
                f"({self.run_id}) Redis URL not set. Handler disabled."
            )
            self.redis_client = None
            return
        try:
            # Use from_url for easier configuration and decode responses
            self.redis_client = redis.Redis.from_url(
                self.redis_url, decode_responses=True
            )
            self.redis_client.ping()
            handler_logger.debug(
                f"({self.run_id}) RedisStreamHandler connected to Redis."
            )
        except redis.exceptions.ConnectionError as e:
            handler_logger.error(
                f"({self.run_id}) Failed Redis connection to {self.redis_url}: {e}"
            )
            self.redis_client = None
        except Exception as e:
            handler_logger.error(
                f"({self.run_id}) Unexpected error connecting to Redis: {e}",
                exc_info=True
            )
            self.redis_client = None

    def emit(self, record):
        """Formats the record and publishes it to the Redis channel."""
        if self.redis_client is None:
            # Optional: Could attempt reconnect here, but might block logging
            # handler_logger.debug(f"({self.run_id}) Skipping emit, no Redis connection.")
            return

        try:
            log_entry = self._format_log_entry(record)
            message = json.dumps(log_entry)
            self.redis_client.publish(self.channel_name, message)
        except redis.exceptions.ConnectionError as e:
             handler_logger.error(
                 f"({self.run_id}) Redis connection error during publish: {e}. "
                 "Will attempt reconnect on next emit."
             )
             # Close potentially broken client, _connect will retry next time
             if self.redis_client:
                 try: self.redis_client.close()
                 except Exception: pass
             self.redis_client = None
        except Exception as e:
            handler_logger.error(
                f"({self.run_id}) Failed to emit log record via Redis: {e}"
            )
            self.handleError(record) # Use standard handler error logging

    def _format_log_entry(self, record):
        """Creates a dictionary representation of the log record."""
        entry = {
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z", # ISO UTC
            "level": record.levelname,
            "levelno": record.levelno,
            "message": self.format(record), # Apply formatter if set
            "logger_name": record.name,
            "pathname": record.pathname,
            "filename": record.filename,
            "module": record.module,
            "lineno": record.lineno,
            "funcName": record.funcName,
        }
        if record.exc_info:
            entry['exc_text'] = "".join(
                traceback.format_exception(*record.exc_info)
            )
        return entry

    def close(self):
        """Closes the Redis connection if open."""
        if self.redis_client:
            try:
                self.redis_client.close()
                handler_logger.debug(
                    f"({self.run_id}) RedisStreamHandler closed connection."
                )
            except Exception as e:
                 handler_logger.error(
                     f"({self.run_id}) Error closing Redis connection: {e}"
                 )
            finally:
                 self.redis_client = None
        super().close()

# --- END OF FILE logger/redis_handler.py ---