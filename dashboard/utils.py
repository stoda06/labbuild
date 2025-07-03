# dashboard/utils.py

import re
import shlex
import os
import logging
import datetime
import pytz
from typing import Optional, List, Dict
from flask import current_app
import random
import string
from typing import Union
from pyVmomi import vim # type: ignore
from vcenter_utils import get_vcenter_instance # Import your vCenter connector
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import DB collection from extensions only if needed by utils directly
# Currently only update_power_state_in_db needs it
from .extensions import alloc_collection

logger = logging.getLogger('dashboard.utils')

def build_args_from_form(form_data):
    """Converts form dictionary to a list of CLI arguments for labbuild.py
       in the format: <command> [options...]."""
    try:
        command = form_data.get('command')
        if not command:
            logger.error("Command missing in form data.")
            return None, "Command is required."

        args = [command]  # Command first

        # Map standard arguments applicable to most commands
        arg_map = {
            'vendor': '-v', 'course': '-g', 'host': '--host',
            'start_pod': '-s', 'end_pod': '-e', 'class_number': '-cn',
            'tag': '-t', 'component': '-c', 'operation': '-o',
            'thread': '-th', 'clonefrom': '--clonefrom'
        }
        for key, flag in arg_map.items():
            value = form_data.get(key)
            if value is not None and value != '':
                args.extend([flag, str(value)])
        
        # --- THIS IS THE FIX: Command-specific arguments ---
        if command == 'setup':
            # These arguments are only valid for the 'setup' command
            setup_specific_args = {
                'memory': '-mem',
                'prtg_server': '--prtg-server',
                'datastore': '-ds'
            }
            for key, flag in setup_specific_args.items():
                value = form_data.get(key)
                if value is not None and value != '':
                    args.extend([flag, str(value)])
        # --- END OF FIX ---

        # Map boolean flags (checkboxes)
        bool_flags = {
             're_build': '--re-build', 'full': '--full',
             'monitor_only': '--monitor-only', 'db_only': '--db-only',
             'perm': '--perm', 'verbose': '--verbose'
        }
        for key, flag in bool_flags.items():
            if form_data.get(key) == 'on':
                # Only add flags relevant to the command
                if command == 'setup' and flag in ['--re-build', '--full', '--monitor-only', '--db-only', '--perm']:
                    args.append(flag)
                elif command == 'teardown' and flag in ['--monitor-only', '--db-only']:
                    args.append(flag)
                elif flag == '--verbose': # Verbose is global
                    args.append(flag)
                
        # --- Basic Validation ---
        error_msg = None
        vendor_val = form_data.get('vendor')
        if not vendor_val:
            error_msg = "Vendor (-v) is required."

        elif command == 'manage' and not form_data.get('operation'):
             error_msg = "Manage operation (-o) is required for 'manage' command."

        # Core args validation (only if no error yet)
        elif command in ['setup', 'manage', 'teardown']:
            is_f5 = vendor_val.lower() == 'f5'
            is_special_mode = (
                form_data.get('db_only') == 'on' or
                form_data.get('monitor_only') == 'on' or
                (command == 'setup' and form_data.get('perm') == 'on') or
                form_data.get('component') == '?' or
                form_data.get('course') == '?'
            )
            if not is_special_mode:
                 if not form_data.get('host'):
                     error_msg = "Host (--host) is required for standard operations."
                 elif is_f5 and not form_data.get('class_number'):
                     error_msg = "F5 Class Number (-cn) required for standard F5 operations."
                 elif not is_f5 and (not form_data.get('start_pod') or
                                   not form_data.get('end_pod')):
                     error_msg = "Start Pod (-s) and End Pod (-e) required for standard non-F5 operations."
                 else: # Check range sanity only if previous checks pass
                     start_p = form_data.get('start_pod')
                     end_p = form_data.get('end_pod')
                     if start_p and end_p:
                          try:
                              if int(start_p) > int(end_p):
                                   error_msg = "Start Pod cannot be greater than End Pod."
                          except ValueError:
                               error_msg = "Invalid number for Start/End Pod."

        if error_msg:
             logger.warning(f"Form validation failed: {error_msg}")
             return None, error_msg

        logger.debug(f"Built arguments from form: {args}")
        return args, None # Return args and no error
    except Exception as e:
        logger.error(f"Error building args from form: {e}", exc_info=True)
        return None, "Internal server error processing form data."

def build_args_from_dict(data: dict) -> tuple[Optional[List[str]], Optional[str]]:
    """
    Converts a dictionary (from API JSON) to a list of CLI arguments
    for labbuild.py. Returns args_list, error_message tuple.
    Performs basic validation.
    """
    command = data.get('command')
    if not command:
        logger.error("API Error: 'command' field is missing.")
        return None, "'command' field is required."

    args = [command] # Command first

    # --- Map standard arguments applicable to most commands ---
    arg_map = {
        'vendor': '-v', 'course': '-g', 'host': '--host',
        'start_pod': '-s', 'end_pod': '-e', 'class_number': '-cn',
        'tag': '-t', 'component': '-c', 'operation': '-o',
        'thread': '-th', 'clonefrom': '--clonefrom'
    }
    for key, flag in arg_map.items():
        value = data.get(key)
        if value is not None:
             if key == 'tag' and value == '': continue
             args.extend([flag, str(value)])

    # --- THIS IS THE FIX: Command-specific arguments for API calls ---
    if command == 'setup':
        setup_specific_args = {
            'memory': '-mem',
            'prtg_server': '--prtg-server',
            'datastore': '-ds'
        }
        for key, flag in setup_specific_args.items():
            value = data.get(key)
            if value is not None and value != '':
                args.extend([flag, str(value)])
    # --- END OF FIX ---

    # Map boolean flags
    bool_flags = {
         'rebuild': '--re-build', 'full': '--full',
         'monitor_only': '--monitor-only', 'db_only': '--db-only',
         'perm': '--perm', 'verbose': '--verbose'
    }
    for key, flag in bool_flags.items():
        if data.get(key) is True:
            # Only add flags relevant to the command
            if command == 'setup' and flag in ['--re-build', '--full', '--monitor-only', '--db-only', '--perm']:
                args.append(flag)
            elif command == 'teardown' and flag in ['--monitor-only', '--db-only']:
                args.append(flag)
            elif flag == '--verbose': # Verbose is global
                args.append(flag)

    # Basic Server-Side Validation (unchanged)
    vendor_val = data.get('vendor')
    if not vendor_val:
        return None, "'vendor' field is required."

    if command in ['setup', 'manage', 'teardown']:
        if not data.get('course'):
             return None, "'course' field is required for setup/manage/teardown."
        if command == 'manage' and not data.get('operation'):
             return None, "'operation' field is required for manage command."

        is_f5 = vendor_val.lower() == 'f5'
        is_special_mode = (
            data.get('db_only') is True or
            data.get('monitor_only') is True or
            (command == 'setup' and data.get('perm') is True) or
            data.get('component') == '?' or
            data.get('course') == '?'
        )

        if not is_special_mode:
             if not data.get('host'):
                 return None, "'host' field is required for standard operations."
             if is_f5 and data.get('class_number') is None:
                 return None, "'class_number' field is required for F5 operations."
             if not is_f5 and (data.get('start_pod') is None or data.get('end_pod') is None):
                 return None, "'start_pod' and 'end_pod' fields are required for non-F5 operations."
             start_p = data.get('start_pod'); end_p = data.get('end_pod')
             if start_p is not None and end_p is not None:
                  try:
                      if int(start_p) > int(end_p):
                           return None, "start_pod cannot be greater than end_pod."
                  except (ValueError, TypeError):
                       return None, "Invalid non-integer value for start_pod or end_pod."

    logger.debug(f"Built arguments from API data: {args}")
    return args, None

def build_log_filter_query(request_args):
    """Builds a MongoDB filter dictionary based on request arguments."""
    mongo_filter = {}
    string_filters = {
        'run_id': 'run_id', 'command': 'command', 'status': 'overall_status',
        'vendor': 'args.vendor', 'host': 'args.host', 'course': 'args.course'
    }
    for form_key, mongo_key in string_filters.items():
        filter_form_key = f"filter_{form_key}"
        value = request_args.get(filter_form_key, '').strip()
        if value:
            # Use regex for contains, but exact match for vendor/host? Let's keep contains for now.
            escaped_value = re.escape(value)
            mongo_filter[mongo_key] = {'$regex': escaped_value, '$options': 'i'}

    # Date range filters
    start_after_str = request_args.get('filter_start_after', '').strip()
    start_before_str = request_args.get('filter_start_before', '').strip()
    date_filter = {}
    try:
        # Assume dates from form are naive, convert to UTC for DB query
        if start_after_str:
            dt_start = datetime.datetime.strptime(start_after_str, '%Y-%m-%d')
            # Make timezone-aware (UTC) assuming input is local date start
            date_filter['$gte'] = pytz.utc.localize(dt_start)
        if start_before_str:
            dt_end = datetime.datetime.strptime(start_before_str, '%Y-%m-%d')
            # Make timezone-aware (UTC) for end of day
            date_filter['$lt'] = pytz.utc.localize(
                dt_end + datetime.timedelta(days=1)
            )
        if date_filter:
            mongo_filter['start_time'] = date_filter
            logger.debug(f"Applying date filter (UTC): {date_filter}")
    except ValueError:
        if start_after_str or start_before_str:
             logger.warning(f"Invalid date filter format in query.")
             # Optionally flash a message here if needed, but utils shouldn't flash
    logger.debug(f"Constructed MongoDB filter: {mongo_filter}")
    return mongo_filter

def format_datetime(dt_obj):
    """Formats datetime object as an ISO 8601 string suitable for JavaScript Date."""
    if not dt_obj or not isinstance(dt_obj, datetime.datetime):
        return None
    # Ensure datetime is UTC before formatting
    if dt_obj.tzinfo is None:
        dt_utc = pytz.utc.localize(dt_obj)
    else:
        dt_utc = dt_obj.astimezone(pytz.utc)
    # Format to ISO 8601 with Z for UTC timezone indicator
    return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def parse_command_line(line: str) -> Optional[List[str]]:
    """
    Parses a full command line string (including script path) into
    a list of arguments suitable for labbuild.py.
    Returns None if parsing fails or line is invalid.
    """
    line = line.strip()
    if not line or line.startswith('#'): # Ignore empty lines and comments
        return None

    try:
        # Split using shlex for handling quotes etc.
        parts = shlex.split(line, posix=(os.name != 'nt'))

        # Find the index of 'labbuild.py' or similar entry point
        script_name_endings = ('labbuild.py', 'run.py') # Add other potential entry points
        script_index = -1
        for i, part in enumerate(parts):
            if any(part.endswith(ending) for ending in script_name_endings):
                script_index = i
                break

        if script_index == -1:
            logger.warning(f"Could not find script entry point {script_name_endings} in command line: {line}")
            return None

        # Extract arguments *after* the script
        args = parts[script_index + 1:]

        # Basic validation: Should have at least a command
        if not args:
             logger.warning(f"No arguments found after script in line: {line}")
             return None

        # Ensure command is first (argparse might handle this, but good practice)
        potential_commands = ['setup', 'teardown', 'manage', '-l', '--list-allocations']
        if args[0] not in potential_commands:
             # Try to find command elsewhere and move it
             command_found = None
             command_index = -1
             for i, arg in enumerate(args):
                 if arg in potential_commands:
                     command_found = arg
                     command_index = i
                     break
             if command_found and command_index > 0:
                 args.pop(command_index)
                 args.insert(0, command_found)
             else:
                 # If still no command at start, log warning but proceed
                 logger.warning(f"Could not identify command at start of arguments: {args}")
                 # Consider returning None if strict command-first order is essential

        logger.debug(f"Parsed arguments from line '{line}': {args}")
        return args

    except Exception as e:
        logger.error(f"Error parsing command line '{line}': {e}", exc_info=True)
        return None

def format_job_args(job_args: tuple) -> str:
    """Formats the job arguments tuple into a readable command string."""
    if not job_args or not isinstance(job_args, tuple) or not job_args[0]:
        return "[No Args]"
    try:
        # Expecting args to be like ([arg1, arg2, ...],)
        args_list = job_args[0]
        if isinstance(args_list, list):
            # Quote arguments containing spaces for better readability
            return ' '.join(shlex.quote(str(arg)) for arg in args_list)
        else:
            # Fallback if format is unexpected
            return str(job_args)
    except Exception as e:
        logger.error(f"Error formatting job args '{job_args}': {e}")
        return "[Error Formatting Args]"

def update_power_state_in_db(tag, course_name, host, item_number, item_type, new_state_bool, f5_class_num_context=None):
    """
    Updates the 'poweron' field for a specific pod/class within the nested DB structure.
    Uses the `alloc_collection` imported from `extensions`.
    """
    if alloc_collection is None:
        logger.error("DB update skipped: alloc_collection is None.")
        return

    power_state_str = str(new_state_bool) # Store as string 'True' or 'False'
    target_id = item_number

    # Construct the query to find the specific item
    query = {"tag": tag, "courses.course_name": course_name}
    update = None
    array_filters = [] # Initialize array_filters

    if item_type == 'f5_class':
        query["courses.pod_details"] = {
            "$elemMatch": {"class_number": target_id, "host": host}
        }
        update = {
            "$set": {"courses.$[courseElem].pod_details.$[podElem].poweron": power_state_str}
        }
        array_filters = [
            {"courseElem.course_name": course_name},
            {"podElem.class_number": target_id, "podElem.host": host}
        ]
    elif item_type == 'pod':
        if f5_class_num_context is not None:
            # Update a nested pod within an F5 class entry
            query["courses.pod_details"] = {
                "$elemMatch": {"class_number": f5_class_num_context, "host": host}
            }
            update = {
                 "$set": { "courses.$[courseElem].pod_details.$[classElem].pods.$[podElem].poweron": power_state_str }
            }
            array_filters = [
                {"courseElem.course_name": course_name},
                {"classElem.class_number": f5_class_num_context, "classElem.host": host},
                {"podElem.pod_number": target_id} # Filter the nested pod by number
            ]
        else:
            # Update a non-F5 pod entry
            query["courses.pod_details"] = {
                "$elemMatch": {"pod_number": target_id, "host": host}
            }
            update = {
                "$set": {"courses.$[courseElem].pod_details.$[podElem].poweron": power_state_str}
            }
            array_filters = [
                 {"courseElem.course_name": course_name},
                 {"podElem.pod_number": target_id, "podElem.host": host}
            ]
    else:
        logger.error(f"DB Update Error: Unknown item_type '{item_type}' for tag '{tag}', item {target_id}")
        return

    if update:
        try:
            result = alloc_collection.update_one(
                query,
                update,
                array_filters=array_filters
             )
            # Log update result (unchanged)
            if result.matched_count > 0:
                 if result.modified_count > 0:
                      logger.info(f"DB power state updated for Tag '{tag}', Course '{course_name}', Host '{host}', {item_type} {target_id} to {power_state_str}")
                 else:
                      logger.warning(f"DB power state potentially already '{power_state_str}' for Tag '{tag}', {item_type} {target_id}. No change made.")
            else:
                 logger.error(f"DB power state update FAILED: No matching document found for Tag '{tag}', Course '{course_name}', Host '{host}', {item_type} {target_id} (Class Context: {f5_class_num_context})")
                 logger.debug(f"DB Update Query: {query}") # Log the query on failure
                 logger.debug(f"DB Update Filters: {array_filters}")
        except Exception as e:
             logger.error(f"Exception updating DB power state for Tag '{tag}', {item_type} {target_id}: {e}", exc_info=True)

def get_hosts_available_memory_parallel(host_details_list: List[Dict]) -> Dict[str, float]:
    """
    Connects to vCenter in parallel for specified hosts and returns available memory in GB.
    Uses the 'fqdn' field for vCenter lookup. Requires host details dicts as input.
    WARNING: Direct vCenter calls can still be slow; caching is recommended.

    Args:
        host_details_list: List of dictionaries, where each dictionary contains
                           details for one host (must include 'host_name', 'vcenter', 'fqdn').

    Returns:
        Dictionary mapping host SHORT name to available memory in GB (float).
        Returns None for hosts where data couldn't be fetched.
    """
    host_memory_gb = {} # Maps short_name -> memory_gb
    # Group hosts by vCenter to reuse connections efficiently within the parallel tasks
    hosts_by_vcenter = {}
    for details in host_details_list:
        vcenter_ip = details.get("vcenter")
        if vcenter_ip not in hosts_by_vcenter:
            hosts_by_vcenter[vcenter_ip] = []
        hosts_by_vcenter[vcenter_ip].append(details) # Add full details dict

    max_workers = min(len(host_details_list), 10) # Limit concurrent connections/threads
    logger.info(f"Fetching memory for {len(host_details_list)} hosts across {len(hosts_by_vcenter)} vCenters using {max_workers} workers...")

    # Results will be stored here temporarily
    results_list = []

    # --- Worker Function ---
    def fetch_memory_for_host(host_details):
        host_short_name = host_details.get("host_name")
        vcenter_ip = host_details.get("vcenter")
        host_fqdn_in_vcenter = host_details.get("fqdn")
        si_obj = None
        service_instance = None
        available_gb_result = None # Use None to indicate fetch failure

        if not all([host_short_name, vcenter_ip, host_fqdn_in_vcenter]):
            logger.warning(f"Skipping host details due to missing info: {host_details}")
            return (host_short_name, None) # Return tuple (short_name, memory_gb)

        try:
            # Establish connection (no reuse needed here as each worker handles one host)
            si_obj = get_vcenter_instance(host_details)
            if not si_obj:
                 logger.warning(f"Connection failed for {host_short_name} on {vcenter_ip}")
                 return (host_short_name, None)
            service_instance = si_obj.connection

            content = service_instance.RetrieveContent()
            host_view_obj = None
            container = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
            for host_system in container.view:
                 if host_system.name == host_fqdn_in_vcenter: host_view_obj = host_system; break
            container.Destroy()

            if not host_view_obj:
                logger.warning(f"Host FQDN '{host_fqdn_in_vcenter}' not found in vCenter '{vcenter_ip}'.")
                return (host_short_name, None)

            # Get memory info
            total_memory_bytes = host_view_obj.hardware.memorySize
            memory_usage_mb = host_view_obj.summary.quickStats.overallMemoryUsage

            if total_memory_bytes is None or memory_usage_mb is None:
                 logger.warning(f"Could not retrieve full memory stats for host '{host_short_name}' (FQDN: {host_fqdn_in_vcenter}).")
            else:
                total_memory_gb = total_memory_bytes / (1024**3)
                memory_usage_gb = memory_usage_mb / 1024
                available_gb = total_memory_gb - memory_usage_gb
                available_gb_result = round(max(0, available_gb), 2) # Store the result
                logger.info(f"Fetched memory for '{host_short_name}': ~{available_gb_result:.2f} GB Available.")

        except vim.fault.NotAuthenticated:
             logger.error(f"vCenter auth failed for host '{host_short_name}' ({vcenter_ip}).")
        except Exception as e:
            logger.error(f"Error fetching memory for host '{host_short_name}': {e}", exc_info=False) # Less verbose logging
        finally:
            # Disconnect vCenter instance
            if service_instance:
                try: from pyVim.connect import Disconnect; Disconnect(service_instance)
                except Exception: pass # Ignore disconnect errors

        return (host_short_name, available_gb_result) # Return tuple
    # --- End Worker Function ---

    # --- Submit tasks in parallel ---
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_host = {executor.submit(fetch_memory_for_host, details): details.get("host_name") for details in host_details_list}

        for future in as_completed(future_to_host):
            host_s_name = future_to_host[future]
            try:
                result_tuple = future.result() # Gets the (short_name, memory_gb) tuple
                if result_tuple:
                    host_memory_gb[result_tuple[0]] = result_tuple[1] # Store result using short name key
                else:
                     host_memory_gb[host_s_name] = None # Ensure key exists even on worker error
            except Exception as exc:
                logger.error(f"Worker thread for host '{host_s_name}' generated an exception: {exc}")
                host_memory_gb[host_s_name] = None # Mark as None on thread error

    logger.info(f"Finished parallel memory fetch. Results for {len(host_memory_gb)} hosts.")
    return host_memory_gb

def _generate_random_password(length=8) -> str:
    """Generates a random numeric password of specified length."""
    return "".join(random.choice(string.digits) for _ in range(length))

def _create_contiguous_ranges(pod_numbers: List[Union[int,str]]) -> str:
    """
    Converts a list of pod numbers (can be int or string)
    into a comma-separated string of contiguous ranges.
    Example: [1, 2, 3, '5', 6, 8] -> "1-3,5-6,8"
    """
    if not pod_numbers:
        return ""
    
    processed_pod_numbers: List[int] = []
    for p in pod_numbers:
        try:
            processed_pod_numbers.append(int(p))
        except (ValueError, TypeError):
            logger.warning(f"Could not convert pod number '{p}' to int in _create_contiguous_ranges. Skipping.")
            continue
    
    if not processed_pod_numbers:
        return ""
    
    pod_numbers_sorted_unique = sorted(list(set(processed_pod_numbers)))
    if not pod_numbers_sorted_unique:
        return ""

    ranges = []
    start_range = pod_numbers_sorted_unique[0]
    end_range = pod_numbers_sorted_unique[0]

    for i in range(1, len(pod_numbers_sorted_unique)):
        if pod_numbers_sorted_unique[i] == end_range + 1:
            end_range = pod_numbers_sorted_unique[i]
        else:
            if start_range == end_range:
                ranges.append(str(start_range))
            else:
                ranges.append(f"{start_range}-{end_range}")
            start_range = pod_numbers_sorted_unique[i]
            end_range = pod_numbers_sorted_unique[i]
    
    # Add the last range
    if start_range == end_range:
        ranges.append(str(start_range))
    else:
        ranges.append(f"{start_range}-{end_range}")
        
    return ",".join(ranges)


def get_next_monday_date_str(format_str="%Y%m%d"):
    """
    Calculates the date of the upcoming Monday.
    If today is Monday, it returns today's date.
    Returns the date as a formatted string.
    """
    today = datetime.datetime.today()
    # weekday() returns 0 for Monday, 1 for Tuesday, ..., 6 for Sunday.
    days_until_monday = (0 - today.weekday() + 7) % 7
    next_monday = today + datetime.timedelta(days=days_until_monday)
    return next_monday.strftime(format_str)