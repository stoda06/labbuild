from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import atexit
import functools

class TaskScheduler:
    def __init__(self, logger):
        self.executor = ThreadPoolExecutor()
        self.futures = {}
        self.lock = threading.Lock()
        self.logger = logger  # Use the passed logger
        atexit.register(self._shutdown)

    def add_task(self, func, *args, **kwargs):
        """
        Adds a task to be executed asynchronously and returns a future associated with the task.

        :param func: The function to execute.
        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.
        :return: A future object associated with the submitted task.
        """
        task = functools.partial(func, *args, **kwargs)
        task_name = func.__name__

        with self.lock:
            future = self.executor.submit(task)
            # Store the future with its task name for reference
            self.futures[future] = task_name
        return future

    def wait_for_tasks(self):
        """
        Waits for all scheduled tasks to complete and handles their results.
        Prints the function name along with the result or exception.
        """
        for future in as_completed(self.futures):
            task_name = self.futures[future]
            try:
                result = future.result()
                self.logger.info(f"Task '{task_name}' completed with result: {result}")
            except Exception as e:
                self.logger.error(f"Task '{task_name}' raised an exception: {e}")

    def get_result(self, future):
        """
        Retrieves the result of a completed task given its future.

        :param future: The future object associated with the task.
        :return: The result of the task if completed successfully, None otherwise.
        """
        try:
            return future.result()
        except Exception as e:
            self.logger.error(f"Failed to get result for task: {e}")
            return None

    def _shutdown(self):
        """
        Shuts down the executor, ensuring all tasks are completed before exiting.
        """
        self.executor.shutdown(wait=True)