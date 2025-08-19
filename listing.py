# In listing.py

import logging
import sys
from typing import Optional, Dict, List, Any, Generator

import pymongo
from pymongo.errors import PyMongoError

# Local Imports
from db_utils import mongo_client
from constants import DB_NAME, ALLOCATION_COLLECTION, COURSE_CONFIG_COLLECTION

logger = logging.getLogger('labbuild.listing')


def list_vendor_courses(vendor: str):
    """List available courses for the given vendor from MongoDB."""
    print(f"Looking for courses for vendor '{vendor}'...")
    try:
        with mongo_client() as client:
            if not client:
                 print("\nError: Could not connect to MongoDB to list courses.")
                 sys.exit(1)

            db = client[DB_NAME]
            collection = db[COURSE_CONFIG_COLLECTION]
            # Case-insensitive query using regex
            courses_cursor = collection.find(
                {"vendor_shortcode": {"$regex": f"^{vendor}$", "$options": "i"}},
                {"course_name": 1, "_id": 0} # Projection
            )
            # Use list comprehension and check for key existence robustly
            courses = sorted([doc["course_name"] for doc in courses_cursor if doc and "course_name" in doc])

            if courses:
                print(f"\nAvailable courses for vendor '{vendor}':")
                for course in courses:
                    print(f"  - {course}")
            else:
                print(f"\nNo courses found for vendor '{vendor}'.")
    except PyMongoError as e:
         print(f"\nError accessing course configurations (PyMongoError): {e}")
    except Exception as e:
        print(f"\nError accessing course configurations: {e}")
    # Exit after listing, regardless of success/failure after attempting
    sys.exit(0)