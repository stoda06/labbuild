<<<<<<< Updated upstream
<<<<<<< Updated upstream
# labbuild/constants.py
"""
Central location for constants used across the application.
This is a clean, reorganized version to prevent import errors.
"""

# --- Imports (Combined from both files) ---
from enum import Enum
from collections import namedtuple
from openpyxl.styles import PatternFill, Border, Side, Alignment
import logging

=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
# ==============================================================================
# DATABASE AND CORE CONSTANTS
# ==============================================================================
DB_NAME = "labbuild_db"
ALLOCATION_COLLECTION = "currentallocation"
COURSE_CONFIG_COLLECTION = "courseconfig"
HOST_COLLECTION = "host"
PRTG_COLLECTION = "prtg"
OPERATION_LOG_COLLECTION = "operation_logs"
LOG_COLLECTION = "logs"
COURSE_MAPPING_RULES_COLLECTION = "course_mapping_rules"
INTERIM_ALLOCATION_COLLECTION = "interimallocation"
BUILD_RULES_COLLECTION = "build_rules"
TRAINER_EMAIL_COLLECTION = "trainer_emails"
SUBSEQUENT_POD_MEMORY_FACTOR = 0.5

# ==============================================================================
# EXCEL REPORT: GENERAL & LAYOUT CONSTANTS
# ==============================================================================
