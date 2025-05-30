# salesforce_utils.py

import os
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
import pytz # For timezone handling if needed

from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceGeneralError
from typing import Optional, List, Dict, Any
from collections import defaultdict
from dotenv import load_dotenv
import math

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

def _find_all_matching_rules(rules: list, vendor: str, code: str, type_str: str) -> list[dict]:
    """
    Finds all build rules matching the course criteria.
    Assumes input 'rules' list is already sorted by priority (ascending).
    """
    matching_rules_list = []
    cc_lower, ct_lower, v_lower = code.lower(), type_str.lower(), vendor.lower()

    for rule in rules: 
        conditions = rule.get("conditions", {})
        is_fallback_conditions = not conditions 
        
        match = True 

        rule_vendor_cond = conditions.get("vendor")
        if rule_vendor_cond and rule_vendor_cond.lower() != v_lower:
            match = False
        
        if match and "course_code_contains" in conditions:
            terms = conditions["course_code_contains"]
            terms = [terms] if not isinstance(terms, list) else terms
            if not any(str(t).lower() in cc_lower for t in terms):
                match = False
        
        if match and "course_code_not_contains" in conditions:
            terms = conditions["course_code_not_contains"]
            terms = [terms] if not isinstance(terms, list) else terms
            if any(str(t).lower() in cc_lower for t in terms):
                match = False

        if match and "course_type_contains" in conditions:
            terms = conditions["course_type_contains"]
            terms = [terms] if not isinstance(terms, list) else terms
            if not any(str(t).lower() in ct_lower for t in terms):
                match = False
        
        # A rule matches if all its conditions are met, OR if it's a fallback rule
        # (empty conditions) and any top-level vendor condition (if present) also matches.
        # If a fallback has a vendor condition, that must still match.
        if match:
            matching_rules_list.append(rule)
            
    return matching_rules_list

def apply_build_rules_to_courses(
    courses_df: pd.DataFrame,
    build_rules: List[Dict[str, Any]],
    course_configs_list: List[Dict[str, Any]],
    hosts_list: List[str]
) -> List[Dict[str, Any]]:
    """
    Applies build rules to Salesforce course data, augmenting each course dictionary
    with preselect_* keys. For Maestro courses, it adds a special note and
    stores split configuration for later processing.

    Returns a list of augmented course dictionaries (same length as input DataFrame rows).
    """
    if courses_df is None or courses_df.empty:
        logger.info("apply_build_rules: Received empty DataFrame. Returning empty list.")
        return []

    output_augmented_courses: List[Dict[str, Any]] = []
    available_lab_courses_by_vendor: Dict[str, set] = defaultdict(set)
    for cfg in course_configs_list:
        vendor_cfg = cfg.get('vendor_shortcode', '').lower()
        name_cfg = cfg.get('course_name')
        if vendor_cfg and name_cfg:
            available_lab_courses_by_vendor[vendor_cfg].add(name_cfg)

    for index, course_row_series in courses_df.iterrows():
        # Convert pandas Series to dict for modification
        course_info_dict: Dict[str, Any] = course_row_series.to_dict()

        sf_course_code = str(course_info_dict.get('Course Code', ''))
        sf_course_type = str(course_info_dict.get('Course Type', 'N/A'))
        # These fields are already in course_info_dict from process_salesforce_data
        # trainer_name = str(course_info_dict.get('Trainer', 'N/A'))
        # sf_start_date = str(course_info_dict.get('Start Date', 'N/A'))
        # sf_end_date = str(course_info_dict.get('End Date', 'N/A'))
        initial_required_pods_sf = int(course_info_dict.get('Pods Req.', 0)) or 1
        sf_pax_count = int(course_info_dict.get('Pax', 0)) # Already int from process_sf_data
        sf_registered_attendees = int(course_info_dict.get('Processed Attendees', 0)) # Already int


        derived_vendor = sf_course_code[:2].lower() if sf_course_code and len(sf_course_code) >= 2 else ''
        # Add/update vendor in the dict. 'vendor' is used by _find_all_matching_rules
        course_info_dict['vendor'] = derived_vendor
        # Other sf_ fields are already present from process_salesforce_data

        logger.debug(
            f"SF Utils: Initial for SF Course '{sf_course_code}', Vendor: '{derived_vendor}', "
            f"SF Pods Req: {initial_required_pods_sf}"
        )

        current_required_pods = initial_required_pods_sf # Start with SF calculated pods
        preselect_actions = {
            "labbuild_course": None, "host": None, "host_priority_list": [],
            "allow_spillover": True, "max_pods_constraint": None, "start_pod_number": 1,
            "maestro_split_config": None, # To store split details if applicable
            "preselect_note": None       # For UI notes
        }
        # Flags to ensure each action type is set by only the highest priority rule
        action_applied_flags = {key: False for key in preselect_actions.keys()}
        # Add specific flags that are not directly in preselect_actions keys
        action_applied_flags["calculate_pods_from_pax"] = False


        all_matching_rules = _find_all_matching_rules(
            build_rules, derived_vendor, sf_course_code, sf_course_type
        )

        if not all_matching_rules:
            logger.debug(f"  No build rules matched for SF Course '{sf_course_code}'. Using defaults.")
        else:
            logger.debug(f"  Applying {len(all_matching_rules)} matching rules for SF Course '{sf_course_code}'.")
            for rule in all_matching_rules: # Rules are pre-sorted by priority
                actions_from_rule = rule.get("actions", {})
                rule_name = rule.get('rule_name', str(rule.get('_id')))
                rule_prio = rule.get('priority', 'N/A')

                # Process maestro_split_build first as it's a structural override
                if "maestro_split_build" in actions_from_rule and not action_applied_flags["maestro_split_config"]:
                    split_config = actions_from_rule["maestro_split_build"]
                    if isinstance(split_config, dict) and \
                       all(k in split_config for k in ["main_course", "rack1_course", "rack2_course", "rack_host"]):
                        preselect_actions["maestro_split_config"] = split_config
                        action_applied_flags["maestro_split_config"] = True # Mark as applied
                        logger.info(f"  Rule '{rule_name}' (P{rule_prio}) Matched Maestro Split Build.")
                    else:
                        logger.warning(f"  Rule '{rule_name}' (P{rule_prio}) has incomplete 'maestro_split_build' config.")

                # Process other actions, allowing maestro_split_config to influence defaults
                if not action_applied_flags["labbuild_course"] and "set_labbuild_course" in actions_from_rule:
                    lb_val = actions_from_rule["set_labbuild_course"]
                    if lb_val and isinstance(lb_val, str) and lb_val in available_lab_courses_by_vendor.get(derived_vendor, set()):
                        preselect_actions["labbuild_course"] = lb_val
                        action_applied_flags["labbuild_course"] = True
                        logger.debug(f"  Rule '{rule_name}' (P{rule_prio}) SET LabBuild Course: {lb_val}")
                    elif lb_val is not None:
                        logger.warning(f"  Rule '{rule_name}' (P{rule_prio}) invalid 'set_labbuild_course': {lb_val}")
                
                if not action_applied_flags["host_priority_list"] and "host_priority" in actions_from_rule: # Check flag for list
                    hp_list = actions_from_rule["host_priority"]
                    if isinstance(hp_list, list) and hp_list:
                        chosen_host = next((h for h in hp_list if h in hosts_list), None)
                        if chosen_host:
                            preselect_actions["host"] = chosen_host
                            preselect_actions["host_priority_list"] = hp_list
                            action_applied_flags["host_priority_list"] = True # Mark list as applied
                            action_applied_flags["host"] = True # Mark host as applied
                            logger.debug(f"  Rule '{rule_name}' (P{rule_prio}) SET Host: {chosen_host} (from list)")
                        else:
                            logger.warning(f"  Rule '{rule_name}' (P{rule_prio}) host_priority: no valid hosts.")
                    elif hp_list is not None:
                        logger.warning(f"  Rule '{rule_name}' (P{rule_prio}) invalid 'host_priority': {hp_list}")

                if not action_applied_flags["allow_spillover"] and "allow_spillover" in actions_from_rule:
                    sp_val = actions_from_rule["allow_spillover"]
                    if isinstance(sp_val, bool):
                        preselect_actions["allow_spillover"] = sp_val
                        action_applied_flags["allow_spillover"] = True
                        logger.debug(f"  Rule '{rule_name}' (P{rule_prio}) SET Allow Spillover: {sp_val}")

                if not action_applied_flags["start_pod_number"] and "start_pod_number" in actions_from_rule:
                    try:
                        spn_val = actions_from_rule["start_pod_number"]
                        if spn_val is not None:
                            preselect_actions["start_pod_number"] = max(1, int(spn_val))
                            action_applied_flags["start_pod_number"] = True
                            logger.debug(f"  Rule '{rule_name}' (P{rule_prio}) SET Start Pod: {preselect_actions['start_pod_number']}")
                    except (ValueError, TypeError):
                         logger.warning(f"  Rule '{rule_name}' (P{rule_prio}) invalid 'start_pod_number': {actions_from_rule['start_pod_number']}")
                
                if not action_applied_flags["max_pods_constraint"] and "set_max_pods" in actions_from_rule:
                    try:
                        mpc_val = actions_from_rule["set_max_pods"]
                        if mpc_val is not None:
                            preselect_actions["max_pods_constraint"] = max(1, int(mpc_val))
                            action_applied_flags["max_pods_constraint"] = True
                            logger.debug(f"  Rule '{rule_name}' (P{rule_prio}) SET Max Pods: {preselect_actions['max_pods_constraint']}")
                    except (ValueError, TypeError):
                         logger.warning(f"  Rule '{rule_name}' (P{rule_prio}) invalid 'set_max_pods': {actions_from_rule['set_max_pods']}")

                if not action_applied_flags["calculate_pods_from_pax"] and \
                   "calculate_pods_from_pax" in actions_from_rule and \
                   isinstance(actions_from_rule["calculate_pods_from_pax"], dict):
                    calc_params = actions_from_rule["calculate_pods_from_pax"]
                    divisor = calc_params.get("divisor")
                    min_pods = calc_params.get("min_pods", 1)
                    use_field = calc_params.get("use_field", "pax").lower()
                    if divisor is not None:
                        try:
                            div_int, min_int = int(divisor), int(min_pods)
                            if div_int <= 0 or min_int < 0: raise ValueError("Invalid divisor/min_pods")
                            count = sf_pax_count
                            if use_field == "registered_attendees": count = sf_registered_attendees
                            elif use_field == "max_registered_or_pax": count = max(sf_pax_count, sf_registered_attendees)
                            
                            pods_calc = math.ceil(count / div_int) if count > 0 else 0
                            current_required_pods = max(min_int, int(pods_calc))
                            action_applied_flags["calculate_pods_from_pax"] = True
                            logger.info(f"  Rule '{rule_name}' (P{rule_prio}) 'calc_pods': New Pods Req: {current_required_pods}")
                        except (ValueError, TypeError):
                            logger.warning(f"  Rule '{rule_name}' (P{rule_prio}): Invalid 'calc_pods' params.")
        # End rule application loop

        # --- Finalize based on applied actions ---
        if preselect_actions["maestro_split_config"]:
            msc = preselect_actions["maestro_split_config"]
            course_info_dict['Pods Req.'] = "4 (Split)" # For UI display
            course_info_dict['preselect_labbuild_course'] = msc.get("main_course") # Display main
            course_info_dict['preselect_host'] = preselect_actions.get("host") # Host for main parts
            course_info_dict['preselect_note'] = (
                f"Maestro Split: 2x {msc.get('main_course', 'N/A')}, "
                f"1x {msc.get('rack1_course', 'N/A')} (@{msc.get('rack_host', 'N/A')}), "
                f"1x {msc.get('rack2_course', 'N/A')} (@{msc.get('rack_host', 'N/A')})"
            )
            # Store the split config itself for the next step
            course_info_dict['maestro_split_config_details'] = msc
        else:
            # Apply max_pods_constraint if it was set and no Maestro split
            if preselect_actions["max_pods_constraint"] is not None:
                if current_required_pods > preselect_actions["max_pods_constraint"]:
                    current_required_pods = preselect_actions["max_pods_constraint"]
            
            final_num_pods = max(0, current_required_pods)
            course_info_dict['Pods Req.'] = final_num_pods
            course_info_dict['preselect_labbuild_course'] = preselect_actions["labbuild_course"]
            course_info_dict['preselect_host'] = preselect_actions["host"]
            # start_pod and end_pod for the main UI display on upcoming_courses
            # The actual allocation logic will use these as preferred start points.
            course_info_dict['preselect_start_pod'] = preselect_actions["start_pod_number"]
            course_info_dict['preselect_end_pod'] = (preselect_actions["start_pod_number"] + final_num_pods - 1) \
                                                   if final_num_pods > 0 else (preselect_actions["start_pod_number"] -1)

        # Add all other preselect actions to the course_info_dict for the UI
        course_info_dict['preselect_host_priority_list'] = preselect_actions["host_priority_list"]
        course_info_dict['preselect_allow_spillover'] = preselect_actions["allow_spillover"]
        course_info_dict['preselect_max_pods_applied_constraint'] = preselect_actions["max_pods_constraint"]
        if preselect_actions.get("preselect_note"): # If Maestro set a note
             course_info_dict['preselect_note'] = preselect_actions["preselect_note"]
        
        # Ensure all original SF data columns are still present if they were in course_info_dict
        # course_info_dict will contain original 'Course Code', 'Start Date', etc. plus new 'preselect_*'
        
        output_augmented_courses.append(course_info_dict)
        logger.debug(f"Final augmented course for '{sf_course_code}': {course_info_dict.get('preselect_labbuild_course')}, "
                     f"Pods: {course_info_dict.get('Pods Req.')}, Host: {course_info_dict.get('preselect_host')}")

    return output_augmented_courses


# --- REVISED Orchestrator Function ---
def get_upcoming_courses_data(build_rules: list, course_configs_list: list, hosts_list: list) -> Optional[list]:
    """
    Connects to Salesforce, fetches report data, processes it, applies build rules
    (for course/host/pod preselection), and returns it as a list of dictionaries.

    Args:
        build_rules: List of build rule documents from MongoDB.
        course_configs_list: List of available LabBuild course configs.
        hosts_list: List of available host names.

    Returns:
        List of course dictionaries augmented with preselect_* keys, or None on critical failure.
    """
    sf_conn = get_salesforce_connection()
    if not sf_conn: return None
    from .salesforce_utils import SF_REPORT_ID # Ensure Report ID is accessible
    if not SF_REPORT_ID: logger.error("SF Report ID missing."); return None

    df_raw = fetch_salesforce_report_data(sf_conn, SF_REPORT_ID)
    if df_raw is None: return None
    if df_raw.empty: return []

    df_processed = process_salesforce_data(df_raw) # Basic processing
    if df_processed is None: return None
    if df_processed.empty: return []

    # --- Apply the rules ---
    augmented_data_list = apply_build_rules_to_courses(
        df_processed, build_rules, course_configs_list, hosts_list
    )
    return augmented_data_list

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