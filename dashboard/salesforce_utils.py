# salesforce_utils.py

import os
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
import pytz # For timezone handling if needed

from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceGeneralError
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from the project root .env file
project_root = os.path.dirname(os.path.abspath(__file__)) # Assumes utils is at the root
load_dotenv(os.path.join(project_root, '.env'))

logger = logging.getLogger('labbuild.salesforce') # Use a dedicated logger

# --- Configuration ---
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_SECURITY_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SF_REPORT_ID = os.getenv('SF_REPORT_ID')

# --- Salesforce Connection ---
def get_salesforce_connection():
    """Establishes and returns a Salesforce connection object or None on failure."""
    if not all([SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN]):
        logger.error("Salesforce credentials (USERNAME, PASSWORD, TOKEN) missing in environment variables.")
        return None
    try:
        sf = Salesforce(
            username=SF_USERNAME,
            password=SF_PASSWORD,
            security_token=SF_SECURITY_TOKEN,
            # instance_url='YOUR_SALESFORCE_INSTANCE_URL' # Optional: Usually determined automatically
            version='58.0' # Specify a recent API version
        )
        logger.info(f"Successfully connected to Salesforce as {SF_USERNAME}.")
        return sf
    except SalesforceAuthenticationFailed as e:
        logger.error(f"Salesforce authentication failed: {e}")
        return None
    except SalesforceGeneralError as e:
         logger.error(f"Salesforce general error during connection: {e}")
         return None
    except Exception as e:
        logger.error(f"Unexpected error connecting to Salesforce: {e}", exc_info=True)
        return None

# --- Data Fetching ---
def fetch_salesforce_report_data(sf: Salesforce, report_id: str) -> Optional[pd.DataFrame]:
    """
    Fetches data from a specific Salesforce report (potentially grouped)
    using the simple-salesforce library by calling the Analytics REST API.
    It iterates through relevant factMap keys to collect all detail rows.
    Returns a Pandas DataFrame or None on failure.
    """
    if not sf or not report_id:
        logger.error("Salesforce connection or Report ID is missing.")
        return None

    logger.info(f"Attempting to fetch Salesforce report ID: {report_id} via Analytics API")
    try:
        api_path = f'analytics/reports/{report_id}'
        report_results = sf.restful(api_path, method='GET')

        # Check essential top-level keys first
        if not report_results \
           or 'reportMetadata' not in report_results \
           or 'reportExtendedMetadata' not in report_results \
           or 'factMap' not in report_results:
            logger.error(f"Report data structure missing essential keys (reportMetadata, reportExtendedMetadata, factMap) for report ID {report_id}.")
            logger.debug(f"Received report data structure: {report_results}")
            return None

        # --- Column Extraction (Keep logic from previous correction) ---
        column_api_names = report_results['reportMetadata'].get('detailColumns', [])
        if not isinstance(column_api_names, list):
            logger.error(f"Expected 'detailColumns' to be a list of API names, but got {type(column_api_names)}.")
            logger.debug(f"Detail Columns content: {column_api_names}")
            return None

        extended_meta = report_results['reportExtendedMetadata'].get('detailColumnInfo', {})
        if not isinstance(extended_meta, dict):
             logger.error(f"Expected 'reportExtendedMetadata.detailColumnInfo' to be a dictionary, but got {type(extended_meta)}.")
             logger.debug(f"Extended Metadata content: {extended_meta}")
             return None

        columns = []
        for api_name in column_api_names:
            col_info = extended_meta.get(api_name)
            if isinstance(col_info, dict) and 'label' in col_info:
                columns.append(col_info['label'])
            else:
                logger.warning(f"Label not found for column API name '{api_name}' in extended metadata. Using API name as header.")
                columns.append(api_name)

        if not columns:
            logger.error(f"Could not extract any column labels/API names from report metadata for ID {report_id}.")
            return None
        logger.debug(f"Report columns (labels) found: {columns}")

        # --- CORRECTED Row Extraction (Iterate through relevant factMap keys) ---
        all_rows_data = []
        fact_map = report_results.get('factMap', {})

        # Keys often follow pattern like '0!T', '1!T', etc. for groupings.
        # Iterate through all keys ending in '!T' except the grand total 'T!T'
        for key, group_data in fact_map.items():
            if key.endswith('!T') and key != 'T!T': # Process group keys
                if isinstance(group_data, dict):
                    report_rows = group_data.get('rows', [])
                    logger.debug(f"Processing {len(report_rows)} rows from factMap key '{key}'...")
                    for row in report_rows:
                        data_cells = row.get('dataCells', [])
                        if not isinstance(data_cells, list):
                            logger.warning(f"Expected dataCells to be a list in key '{key}', but got {type(data_cells)}. Skipping row.")
                            continue

                        row_values = []
                        for cell in data_cells:
                            if isinstance(cell, dict):
                                label = cell.get('label')
                                value = cell.get('value')
                                row_values.append(label if label else value)
                            else:
                                row_values.append(None)

                        if len(row_values) == len(columns):
                            all_rows_data.append(row_values)
                        else:
                            expected_cols_str = ", ".join(columns)
                            actual_vals_str = ", ".join(map(str, row_values))
                            logger.warning(
                                f"Row data length mismatch in key '{key}'. Expected {len(columns)} columns ({expected_cols_str}), "
                                f"got {len(row_values)} values ({actual_vals_str})."
                             )
                else:
                     logger.warning(f"Expected dictionary for factMap key '{key}', but got {type(group_data)}.")

        # Check if any data was collected across all groups
        if not all_rows_data:
            logger.warning(f"No data rows extracted from any group key in report ID {report_id}.")
            return pd.DataFrame(columns=columns) # Return empty DataFrame

        # Create DataFrame from all collected rows
        df = pd.DataFrame(all_rows_data, columns=columns)
        logger.info(f"Successfully fetched and parsed report {report_id}. Total rows from all groups: {len(df)}")
        return df

    except SalesforceGeneralError as e:
        logger.error(f"Salesforce API error fetching report {report_id}: {e}")
        logger.debug(f"Salesforce error details: Status={e.status}, Content={e.content}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching/parsing report {report_id}: {e}", exc_info=True)
        return None

# --- Data Processing ---
def process_salesforce_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Processes the raw Salesforce DataFrame. Includes all desired columns.
    Returns the processed DataFrame or None on failure.
    """
    if df is None or df.empty:
        logger.warning("Received empty or None DataFrame for processing.")
        return pd.DataFrame()

    logger.info(f"Processing {len(df)} raw Salesforce rows.")
    processed_df = df.copy()

    # Define the exact original column headers we need for processing/selection
    # Based on your previous debug output
    date_col_hdr = 'Course Start Date'
    end_date_col_hdr = 'Course End Date'
    course_code_hdr = 'Course: Course Job Code'
    course_type_hdr = 'Course Type'
    trainer_col_hdr = 'Trainer'
    cal_desc_col_hdr = 'Cal Desc' # Needed for Pax calculation
    attendees_col_hdr = 'Registered Attendees' # Needed for Pod calculation

    # Basic check if essential processing columns exist
    essential_cols = [date_col_hdr, cal_desc_col_hdr, attendees_col_hdr]
    missing_essentials = [col for col in essential_cols if col not in processed_df.columns]
    if missing_essentials:
         logger.error(f"Essential columns for processing missing: {', '.join(missing_essentials)}. Cannot proceed.")
         return None
    # Warn about others if needed for final display
    optional_display_cols = [end_date_col_hdr, course_code_hdr, course_type_hdr, trainer_col_hdr]
    missing_optionals = [col for col in optional_display_cols if col not in processed_df.columns]
    if missing_optionals:
        logger.warning(f"Optional display columns missing: {', '.join(missing_optionals)}. They will show as N/A.")


    # 1. Convert Date Columns (using dayfirst=True)
    for col in [date_col_hdr, end_date_col_hdr]:
        if col in processed_df.columns:
            processed_df[col] = pd.to_datetime(processed_df[col], format='%d/%m/%Y', errors='coerce', dayfirst=True)

    # Drop rows where Start Date parsing failed (essential)
    original_len = len(processed_df)
    processed_df.dropna(subset=[date_col_hdr], inplace=True)
    if len(processed_df) < original_len:
         logger.warning(f"Dropped {original_len - len(processed_df)} rows due to invalid '{date_col_hdr}' format.")

    # 2. Filter for Upcoming Week (Keep commented out for now if testing)
    try:
        today = datetime.now(pytz.utc).date()
        next_sunday_naive = today + timedelta(days=(6 - today.weekday()) % 7)
        following_saturday_naive = next_sunday_naive + timedelta(days=6)
        start_dates_only = pd.to_datetime(processed_df[date_col_hdr], errors='coerce').dt.date
        processed_df = processed_df[
            (start_dates_only >= next_sunday_naive) &
            (start_dates_only <= following_saturday_naive)
        ].copy()
        logger.info(f"Filtered to {len(processed_df)} rows for the upcoming week ({next_sunday_naive} to {following_saturday_naive}).")
    except Exception as e:
        logger.error(f"Error during date filtering: {e}", exc_info=True)

    if processed_df.empty:
        logger.info("No courses found after initial processing/filtering.")
        return processed_df # Return empty DF

    # 3. Capitalize 'Trainer'
    if trainer_col_hdr in processed_df.columns:
        processed_df[trainer_col_hdr] = processed_df[trainer_col_hdr].apply(
            lambda x: x.title() if isinstance(x, str) else x
        )

    # 4. Extract 'Pax Number' from 'Cal Desc'
    pax_num_col = 'Pax Number' # Internal column name
    if cal_desc_col_hdr in processed_df.columns:
        def extract_pax(desc):
            if not isinstance(desc, str): return 0
            match = re.search(r'(\d+)\s*Pax', desc, re.IGNORECASE)
            return int(match.group(1)) if match else 0
        processed_df[pax_num_col] = processed_df[cal_desc_col_hdr].apply(extract_pax)
    else:
        processed_df[pax_num_col] = 0

    # 5. Ensure 'Registered Attendees' is numeric
    internal_attendees_col = 'Processed Attendees' # Use a distinct internal name
    if attendees_col_hdr in processed_df.columns:
        processed_df[internal_attendees_col] = pd.to_numeric(processed_df[attendees_col_hdr], errors='coerce').fillna(0).astype(int)
    else:
        processed_df[internal_attendees_col] = 0

    # 6. Compute 'required number of pods'
    pods_col = 'Required Pods' # Internal column name
    processed_df[pods_col] = processed_df[[pax_num_col, internal_attendees_col]].max(axis=1)

    # --- 7. Define the columns to KEEP and their desired FINAL names/order ---
    # Map original SF header -> final desired header name
    final_columns_map = {
        date_col_hdr: 'Start Date',
        end_date_col_hdr: 'End Date',
        course_code_hdr: 'Course Code',
        course_type_hdr: 'Course Type',
        trainer_col_hdr: 'Trainer',
        pax_num_col: 'Pax',          # Use calculated Pax
        pods_col: 'Pods Req.'      # Use calculated Pods
    }

    # Create the final DataFrame by selecting and renaming in one go
    # Select only the keys (original headers) from the map that exist in processed_df
    cols_to_select_final = [orig_col for orig_col in final_columns_map.keys() if orig_col in processed_df.columns]

    if not cols_to_select_final:
        logger.error("None of the desired final columns were found in the processed data.")
        return pd.DataFrame()

    final_df = processed_df[cols_to_select_final].copy()

    # Rename the selected columns to their desired display names
    actual_rename_map_final = {orig: final for orig, final in final_columns_map.items() if orig in cols_to_select_final}
    final_df.rename(columns=actual_rename_map_final, inplace=True)

    # --- 8. Final Formatting ---
    # Format date columns (they should be datetime objects now)
    for date_col_display in ['Start Date', 'End Date']:
         if date_col_display in final_df.columns:
            # Dates are already datetime objects, just format them
            final_df[date_col_display] = final_df[date_col_display].dt.strftime('%Y-%m-%d')
            # Fill any NaT values that might have resulted from errors
            final_df.loc[:, date_col_display] = final_df[date_col_display].fillna('Invalid Date')

    # Ensure desired column order
    desired_order = ['Start Date', 'End Date', 'Course Code', 'Course Type', 'Trainer', 'Pax', 'Pods Req.']
    # Filter order list to only include columns that actually exist in final_df
    final_order = [col for col in desired_order if col in final_df.columns]
    final_df = final_df[final_order]

    # Fill any remaining general NaN values
    final_df.fillna('N/A', inplace=True)

    logger.info(f"Processing complete. Returning {len(final_df)} rows with columns: {final_df.columns.tolist()}.")
    return final_df

# --- NEW: Data Processing for CURRENT Week ---
def process_salesforce_data_current_week(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Processes the raw Salesforce DataFrame for the CURRENT week (Sun-Sat).
    Reuses most logic from process_salesforce_data but changes the date filter.
    Returns the processed DataFrame or None on failure.
    """
    if df is None or df.empty:
        logger.warning("Received empty or None DataFrame for processing (current week).")
        return pd.DataFrame()

    logger.info(f"Processing {len(df)} raw Salesforce rows for CURRENT week.")
    processed_df = df.copy()
    date_col_hdr = 'Course Start Date'

    if date_col_hdr not in processed_df.columns:
         logger.error(f"Mandatory column '{date_col_hdr}' not found. Cannot proceed.")
         return None

    # 1. Convert 'Course Start Date'
    processed_df[date_col_hdr] = pd.to_datetime(
        processed_df[date_col_hdr], format='%d/%m/%Y', errors='coerce', dayfirst=True
    )
    original_len = len(processed_df)
    processed_df.dropna(subset=[date_col_hdr], inplace=True)
    if len(processed_df) < original_len:
         logger.warning(f"Dropped {original_len - len(processed_df)} rows due to invalid '{date_col_hdr}' format.")

    # 2. Filter for *Current* Week (Last Sunday to this Saturday)
    try:
        today = datetime.now(pytz.utc).date()
        # Calculate start of the current week (Sunday)
        # today.weekday() is 0 for Mon, 6 for Sun. We want days to subtract to get to last Sun.
        days_since_sunday = (today.weekday() + 1) % 7
        current_sunday_naive = today - timedelta(days=days_since_sunday)
        # Calculate end of the current week (Saturday)
        current_saturday_naive = current_sunday_naive + timedelta(days=6)

        logger.info(f"Filtering for CURRENT week: {current_sunday_naive} to {current_saturday_naive}") # Log the range
        start_dates_only = pd.to_datetime(processed_df[date_col_hdr], errors='coerce').dt.date

        # Apply the filter
        processed_df = processed_df[
            (start_dates_only >= current_sunday_naive) &
            (start_dates_only <= current_saturday_naive)
        ].copy() # Ensure it's a copy
    except Exception as e:
        logger.error(f"Error during current week date filtering: {e}", exc_info=True)

    if processed_df.empty:
        logger.info("No courses found for the current week.")
        # Return empty DF here as subsequent steps might assume data
        # return processed_df

    # --- Reuse steps 3-9 from process_salesforce_data ---
    # 3. Capitalize 'Trainer'
    trainer_col_hdr = next((col for col in processed_df.columns if 'Trainer' in col), None)
    if trainer_col_hdr:
        processed_df[trainer_col_hdr] = processed_df[trainer_col_hdr].apply(
            lambda x: x.title() if isinstance(x, str) else x
        )

    # 4. Extract 'Pax Number'
    cal_desc_col_hdr = next((col for col in processed_df.columns if 'Cal Desc' in col), None)
    pax_num_col = 'Pax Number'
    if cal_desc_col_hdr:
        def extract_pax(desc):
            if not isinstance(desc, str): return 0
            match = re.search(r'(\d+)\s*Pax', desc, re.IGNORECASE)
            return int(match.group(1)) if match else 0
        processed_df[pax_num_col] = processed_df[cal_desc_col_hdr].apply(extract_pax)
    else:
        processed_df[pax_num_col] = 0
        logger.warning(f"Column like 'Cal Desc' not found. 'Pax Number' will be 0.")

    # 5. Ensure 'Registered Attendees' is numeric
    attendees_col_hdr = next((col for col in processed_df.columns if 'Registered Attendees' in col), None)
    internal_attendees_col = 'Processed Attendees'
    if attendees_col_hdr:
        processed_df[internal_attendees_col] = pd.to_numeric(processed_df[attendees_col_hdr], errors='coerce').fillna(0).astype(int)
    else:
        processed_df[internal_attendees_col] = 0
        logger.warning(f"Column like 'Registered Attendees' not found. Attendee count will be 0.")

    # 6. Compute 'required number of pods'
    pods_col = 'Required Pods'
    processed_df[pods_col] = processed_df[[pax_num_col, internal_attendees_col]].max(axis=1)

    # 7/8/9. Select, Rename, Format
    final_columns_map = {
        'Course Start Date': 'Start Date',
        'Course End Date': 'End Date',
        'Course: Course Job Code': 'Course Code',
        'Course Type': 'Course Type',
        'Trainer': 'Trainer',
        pax_num_col: 'Pax',
        pods_col: 'Pods Req.'
    }
    cols_to_select_final = [orig_col for orig_col in final_columns_map.keys() if orig_col in processed_df.columns]

    if not cols_to_select_final:
        logger.warning("None of the desired final columns were found after CURRENT week processing.")
        return pd.DataFrame(columns=list(final_columns_map.values()))

    final_df = processed_df[cols_to_select_final].copy()
    actual_rename_map_final = {orig: final for orig, final in final_columns_map.items() if orig in cols_to_select_final}
    final_df.rename(columns=actual_rename_map_final, inplace=True)

    for date_col_display in ['Start Date', 'End Date']:
        if date_col_display in final_df.columns:
            dates_dt = pd.to_datetime(final_df[date_col_display], errors='coerce', dayfirst=True)
            # ---------------------------------------------------

            # Format the datetime objects
            final_df[date_col_display] = dates_dt.dt.strftime('%Y-%m-%d')

            # Fill NaNs
            final_df.loc[:, date_col_display] = final_df[date_col_display].fillna('Invalid Date')

    desired_order = ['Start Date', 'End Date', 'Course Code', 'Course Type', 'Trainer', 'Pax', 'Pods Req.']
    final_order = [col for col in desired_order if col in final_df.columns]
    final_df = final_df[final_order]
    final_df.fillna('N/A', inplace=True)

    logger.info(f"CURRENT week processing complete. Returning {len(final_df)} rows.")
    return final_df

# --- NEW: Function to Apply Rules on Backend ---
def apply_mapping_rules_to_courses(
    courses_df: pd.DataFrame,
    mapping_rules: list,
    course_configs_list: list,
    hosts_list: list
) -> list:
    """
    Applies mapping rules to a DataFrame of processed course data.

    Args:
        courses_df: DataFrame from process_salesforce_data functions.
        mapping_rules: List of rule documents from MongoDB (sorted by priority).
        course_configs_list: List of available LabBuild course configs ({course_name, vendor_shortcode}).
        hosts_list: List of available host names.

    Returns:
        List of dictionaries, where each dictionary represents a course
        and includes added keys for pre-selections:
        'preselect_labbuild_course', 'preselect_host',
        'preselect_start_pod', 'preselect_end_pod'
    """
    if courses_df is None or courses_df.empty:
        return []

    output_list = []
    # Convert configs to a dict keyed by vendor for faster lookup
    configs_by_vendor = {}
    for cfg in course_configs_list:
        vendor = cfg.get('vendor_shortcode', '').lower()
        if vendor not in configs_by_vendor:
            configs_by_vendor[vendor] = []
        configs_by_vendor[vendor].append(cfg.get('course_name'))

    for index, course_row in courses_df.iterrows():
        # Create a dictionary from the row for easier access
        course_info = course_row.to_dict()

        # --- Prepare data for rule matching ---
        sf_course_code = course_info.get('Course Code', '')
        sf_course_type = course_info.get('Course Type', '') # Make sure this name matches df column
        required_pods = int(course_info.get('Pods Req.', 0)) # Ensure numeric
        derived_vendor = sf_course_code[:2].lower() if sf_course_code else ''

        # --- Initialize Preselections ---
        preselect = {
            "labbuild_course": None,
            "host": None,
            "max_pods": None, # Store intermediate max_pods from rules
            "start_pod": 1,
            "end_pod": required_pods if required_pods > 0 else 1
        }
        extracted_part = None # For regex extraction

        # --- Apply Rules (Logic moved from JS) ---
        for rule in mapping_rules: # Assumes rules are pre-sorted by priority
            match = True
            conditions = rule.get("conditions", {})
            actions = rule.get("actions", {})
            extracted_part = None # Reset for each rule

            # Condition Checks
            if conditions.get("vendor") and conditions["vendor"] != derived_vendor:
                match = False
            if match and conditions.get("sf_course_type_contains"):
                type_lower = sf_course_type.lower()
                if not any(term.lower() in type_lower for term in conditions["sf_course_type_contains"]):
                    match = False
            if match and conditions.get("sf_course_code_contains"):
                code_lower = sf_course_code.lower()
                if not any(term.lower() in code_lower for term in conditions["sf_course_code_contains"]):
                    match = False
            if match and conditions.get("sf_course_type_extract_pattern"):
                try:
                    pattern = conditions["sf_course_type_extract_pattern"]
                    regex_match = re.search(pattern, sf_course_type) # Case sensitive regex search
                    if regex_match and len(regex_match.groups()) > 0:
                        extracted_part = regex_match.group(1)
                    else:
                        match = False
                except Exception as e:
                    logger.error(f"Regex error in rule {rule.get('_id')}: {e}")
                    match = False
            # Add more condition checks here

            # Apply Actions if Match
            if match:
                # Apply LabBuild Course (only if not already set by higher priority)
                course_set_action_applied = False
                if actions.get("set_labbuild_course_contains_extract") and extracted_part and preselect["labbuild_course"] is None:
                    vendor_courses = configs_by_vendor.get(derived_vendor, [])
                    found_course_name = next((name for name in vendor_courses if extracted_part in name), None) # Case sensitive check
                    if found_course_name:
                        preselect["labbuild_course"] = found_course_name
                        course_set_action_applied = True
                    else:
                         logger.warning(f"Rule action for {sf_course_code}: Could not find course containing '{extracted_part}' for vendor '{derived_vendor}'.")

                if not course_set_action_applied and actions.get("set_labbuild_course") and preselect["labbuild_course"] is None:
                    preselect["labbuild_course"] = actions["set_labbuild_course"]

                # Apply Host (only if not already set by higher priority)
                host_set_action_applied = False
                if actions.get("set_host_preference") and preselect["host"] is None:
                    for preferred_host in actions["set_host_preference"]:
                        if preferred_host in hosts_list:
                            preselect["host"] = preferred_host
                            host_set_action_applied = True
                            break
                    if not host_set_action_applied:
                         logger.warning(f"Rule action for {sf_course_code}: Host preference {actions['set_host_preference']} specified, but none found in available hosts.")

                if not host_set_action_applied and actions.get("set_host") and preselect["host"] is None:
                    preselect["host"] = actions["set_host"]

                # Apply Max Pods (overwrites lower priority)
                if actions.get("set_max_pods") is not None:
                    try:
                        max_p = int(actions["set_max_pods"])
                        if max_p >= 1:
                            preselect["max_pods"] = max_p
                        else:
                             logger.warning(f"Rule action for {sf_course_code}: Invalid max_pods value '{actions['set_max_pods']}'.")
                    except (ValueError, TypeError):
                         logger.warning(f"Rule action for {sf_course_code}: Invalid max_pods value '{actions['set_max_pods']}'.")

        # --- Calculate final End Pod ---
        if preselect["max_pods"] is not None:
            effective_required = required_pods if required_pods > 0 else preselect["max_pods"]
            preselect["end_pod"] = min(preselect["max_pods"], effective_required)
            if preselect["start_pod"] > preselect["end_pod"]:
                preselect["start_pod"] = preselect["end_pod"] # Adjust start if needed
        else:
             preselect["end_pod"] = required_pods if required_pods > 0 else 1

        # --- Add preselection info to the original course data ---
        course_info['preselect_labbuild_course'] = preselect["labbuild_course"]
        course_info['preselect_host'] = preselect["host"]
        course_info['preselect_start_pod'] = preselect["start_pod"]
        course_info['preselect_end_pod'] = preselect["end_pod"]

        output_list.append(course_info)

    return output_list

# --- Orchestrator Function ---
def get_upcoming_courses_data(mapping_rules: list, course_configs_list: list, hosts_list: list) -> Optional[list]:
    """
    Connects to Salesforce, fetches report data, processes it,
    and returns it as a list of dictionaries suitable for Flask templates.
    Returns None on critical failure.
    """
    sf_conn = get_salesforce_connection()
    if not sf_conn:
        return None # Error already logged

    if not SF_REPORT_ID:
        logger.error("Salesforce Report ID (SF_REPORT_ID) missing in environment variables.")
        return None

    df_raw = fetch_salesforce_report_data(sf_conn, SF_REPORT_ID)

    if df_raw is None:
        # Error fetching data (already logged)
        return None
    elif df_raw.empty:
         # No raw data found (e.g., report is empty)
         return [] # Return empty list

    df_processed = process_salesforce_data(df_raw)

    if df_processed is None:
        # Error processing data (already logged)
        return None # Indicate processing failure
    elif df_processed.empty:
         # No data after processing (e.g., no courses in the upcoming week)
         return [] # Return empty list
    
    # Apply rules
    augmented_data_list = apply_mapping_rules_to_courses(
        df_processed, mapping_rules, course_configs_list, hosts_list
    )
    return augmented_data_list # Return the list directly

# --- NEW Orchestrator Function for CURRENT Week ---
def get_current_courses_data() -> Optional[list]:
    """
    Connects to Salesforce, fetches report data, processes it for the CURRENT week,
    and returns it as a list of dictionaries suitable for Flask templates.
    Returns None on critical failure.
    """
    sf_conn = get_salesforce_connection()
    if not sf_conn: return None
    if not SF_REPORT_ID: logger.error("SF Report ID missing."); return None

    df_raw = fetch_salesforce_report_data(sf_conn, SF_REPORT_ID)
    if df_raw is None: return None # Error fetching

    # Use the NEW processing function for the current week
    df_processed = process_salesforce_data_current_week(df_raw)

    if df_processed is None: return None # Error processing
    if df_processed.empty: return [] # No current courses

    try:
        data_list = df_processed.to_dict(orient='records')
        return data_list
    except Exception as e:
        logger.error(f"Error converting current week DataFrame to list: {e}", exc_info=True)
        return None