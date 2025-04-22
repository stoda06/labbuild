# constants.py
"""Central location for constants used across the application."""

# Database and Collection Names
DB_NAME = "labbuild_db"
ALLOCATION_COLLECTION = "currentallocation"
COURSE_CONFIG_COLLECTION = "courseconfig"
HOST_COLLECTION = "host"
PRTG_COLLECTION = "prtg"
OPERATION_LOG_COLLECTION = "operation_logs" # Operation summary/status logs
LOG_COLLECTION = "logs" # Standard detailed logs from MongoLogHandler
COURSE_MAPPING_RULES_COLLECTION = "course_mapping_rules"

# Add other constants here if needed, e.g.:
# DEFAULT_TAG = "untagged"
# DEFAULT_THREAD_COUNT = 4