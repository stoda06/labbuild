# utils.py
"""General utility functions."""

import logging
from typing import Dict, List, Any
from concurrent.futures import Future, wait

logger = logging.getLogger('labbuild.utils')

def wait_for_tasks(futures: List[Future], description: str = "tasks") -> List[Dict[str, Any]]:
    """Wait for futures, log errors, and return structured results."""
    results = []
    logger.info(f"Waiting for {len(futures)} {description} to complete...")
    done, not_done = wait(futures)

    for future in done:
        pod_num = getattr(future, "pod_number", None)
        class_num = getattr(future, "class_number", None)
        identifier = str(pod_num) if pod_num is not None else str(class_num) if class_num is not None else "unknown"
        class_identifier = str(class_num) if class_num is not None else None

        try:
            task_result = future.result()
            if isinstance(task_result, tuple) and len(task_result) == 3:
                success, step, error_msg = task_result
                results.append({"identifier": identifier, "class_identifier": class_identifier,
                                "status": "success" if success else "failed", "failed_step": step, "error_message": error_msg})
                logger.debug(f"Task completed (ID: {identifier}, Class: {class_identifier}). Result: {task_result}")
            else:
                 results.append({"identifier": identifier, "class_identifier": class_identifier,
                                "status": "success", "failed_step": None, "error_message": None})
                 logger.debug(f"Task completed (ID: {identifier}, Class: {class_identifier}). Result: {task_result}")
        except Exception as e:
            logger.error(f"Task FAILED (ID: {identifier}, Class: {class_identifier}). Error: {e}", exc_info=True)
            results.append({"identifier": identifier, "class_identifier": class_identifier,
                           "status": "failed", "failed_step": "task_exception", "error_message": str(e)})

    if not_done:
         logger.warning(f"{len(not_done)} {description} did not complete (timed out or cancelled).")
         for future in not_done:
             pod_num = getattr(future, "pod_number", None); class_num = getattr(future, "class_number", None)
             identifier = str(pod_num) if pod_num is not None else str(class_num) if class_num is not None else "unknown"
             class_identifier = str(class_num) if class_num is not None else None
             results.append({"identifier": identifier, "class_identifier": class_identifier,
                            "status": "failed", "failed_step": "timeout_or_cancelled", "error_message": "Task did not complete."})
    return results