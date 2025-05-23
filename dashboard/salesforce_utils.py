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
    course_configs_list: List[Dict[str, Any]], # Used to validate set_labbuild_course
    hosts_list: List[str]  # Used to validate hosts in host_priority
) -> List[Dict[str, Any]]:
    """
    Applies build rules (including LabBuild course selection, host preferences,
    and pod calculation adjustments) to course data from Salesforce.
    Actions from higher-priority rules override those from lower-priority rules
    if they target the same action key (tracked by action_set_flags).

    Args:
        courses_df: DataFrame from process_salesforce_data functions.
        build_rules: List of rule documents from MongoDB build_rules collection,
                     expected to be pre-sorted by priority (ascending).
        course_configs_list: List of available LabBuild course configs (dicts with
                             at least 'vendor_shortcode' and 'course_name').
        hosts_list: List of available host names (strings).

    Returns:
        List of course dictionaries augmented with preselect_* keys and final 'Pods Req.'.
    """
    if courses_df is None or courses_df.empty:
        logger.info("apply_build_rules_to_courses: Received empty DataFrame. Returning empty list.")
        return []

    output_list: List[Dict[str, Any]] = []
    
    # Prepare a lookup for available LabBuild courses by vendor for quick validation
    available_lab_courses_by_vendor: Dict[str, set] = defaultdict(set)
    for cfg in course_configs_list:
        vendor_cfg = cfg.get('vendor_shortcode', '').lower()
        name = cfg.get('course_name')
        if vendor_cfg and name:
            available_lab_courses_by_vendor[vendor_cfg].add(name)

    for index, course_row in courses_df.iterrows():
        course_info: Dict[str, Any] = course_row.to_dict()

        # Extract essential fields from the Salesforce data (DataFrame row)
        sf_course_code = str(course_info.get('Course Code', '')) # Ensure string
        sf_course_type = str(course_info.get('Course Type', 'N/A'))
        trainer_name = str(course_info.get('Trainer', 'N/A'))
        sf_start_date = str(course_info.get('Start Date', 'N/A')) # Assumed YYYY-MM-DD string
        sf_end_date = str(course_info.get('End Date', 'N/A'))   # Assumed YYYY-MM-DD string
        
        # This is the 'Pods Req.' calculated by process_salesforce_data (e.g., max(1, students))
        initial_required_pods = int(course_info.get('Pods Req.', 0)) or 1
        
        sf_pax_count = 0
        try: sf_pax_count = int(course_info.get('Pax', 0)) # From 'Pax Number' renamed to 'Pax'
        except (ValueError, TypeError): pass

        sf_registered_attendees = 0
        try: sf_registered_attendees = int(course_info.get('Processed Attendees', 0))
        except (ValueError, TypeError): pass


        # Derive vendor from SF course code if not already present or to ensure consistency
        derived_vendor = sf_course_code[:2].lower() if sf_course_code and len(sf_course_code) >= 2 else ''
        course_info['vendor'] = derived_vendor # This 'vendor' is used for rule matching

        # Store original SF data and derived vendor in course_info for later use/API output
        course_info['Trainer'] = trainer_name
        course_info['sf_start_date'] = sf_start_date
        course_info['sf_end_date'] = sf_end_date
        course_info['sf_course_type'] = sf_course_type
        course_info['sf_pax'] = sf_pax_count
        course_info['sf_registered_attendees'] = sf_registered_attendees

        logger.debug(
            f"SF Utils: Initial for SF Course '{sf_course_code}', Vendor: '{derived_vendor}', "
            f"Initial Pods Req: {initial_required_pods}, Pax: {sf_pax_count}, Reg: {sf_registered_attendees}"
        )

        # This will hold the final pod requirement after rules are applied
        current_required_pods = initial_required_pods

        # These are the defaults or rule-driven values for UI pre-selection and build execution
        preselect_actions = {
            "labbuild_course": None,
            "host": None, # The single chosen host for pre-selection
            "host_priority_list": [], # The full list from the rule that set the host
            "allow_spillover": True, # Default
            "max_pods_constraint": None, # Max pods constraint from a rule
            "start_pod_number": 1, # Default start_pod
        }
        # Flags to ensure each action type is set by only the highest priority rule
        action_applied_flags = {key: False for key in preselect_actions.keys()}
        action_applied_flags["calculate_pods_from_pax"] = False # Specific flag for pod calculation rule

        all_matching_rules = _find_all_matching_rules(
            build_rules, derived_vendor, sf_course_code, sf_course_type
        )

        if not all_matching_rules:
            logger.debug(f"  SF Utils: No build rules matched for SF Course '{sf_course_code}'.")
        else:
            logger.debug(f"  SF Utils: Applying {len(all_matching_rules)} matching build rules for SF Course '{sf_course_code}'.")
            for rule in all_matching_rules: # Rules are pre-sorted by priority
                actions_from_rule = rule.get("actions", {})
                rule_name_for_log = rule.get('rule_name', str(rule.get('_id')))
                rule_prio_for_log = rule.get('priority', 'N/A')

                # --- Apply "set_labbuild_course" action ---
                if not action_applied_flags["labbuild_course"] and "set_labbuild_course" in actions_from_rule:
                    lb_course_action_val = actions_from_rule["set_labbuild_course"]
                    if lb_course_action_val and isinstance(lb_course_action_val, str):
                        if lb_course_action_val in available_lab_courses_by_vendor.get(derived_vendor, set()):
                            preselect_actions["labbuild_course"] = lb_course_action_val
                            action_applied_flags["labbuild_course"] = True
                            logger.debug(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) SET LabBuild Course: {lb_course_action_val}")
                        else:
                            logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) specified LabBuild course '{lb_course_action_val}' "
                                           f"which is NOT FOUND for vendor '{derived_vendor}'. LabBuild course not set by this rule.")
                    elif lb_course_action_val is not None: # Handles empty string or non-string
                         logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) has invalid 'set_labbuild_course' value: {lb_course_action_val}")

                # --- Apply "host_priority" action ---
                if not action_applied_flags["host"] and "host_priority" in actions_from_rule:
                    rule_hp_list_val = actions_from_rule["host_priority"]
                    if isinstance(rule_hp_list_val, list) and rule_hp_list_val:
                        # Find first valid and available host from the priority list
                        chosen_host_from_rule = next((h for h in rule_hp_list_val if h in hosts_list), None)
                        if chosen_host_from_rule:
                            preselect_actions["host"] = chosen_host_from_rule
                            preselect_actions["host_priority_list"] = rule_hp_list_val # Store the list that led to selection
                            action_applied_flags["host"] = True
                            logger.debug(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) SET Host: {chosen_host_from_rule} (from list: {rule_hp_list_val})")
                        else:
                            logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) specified host_priority list {rule_hp_list_val}, "
                                           "but no hosts in that list are currently available/valid. Host not set by this rule.")
                    elif rule_hp_list_val is not None: # Handles empty list or non-list
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) has invalid 'host_priority' value (must be non-empty list): {rule_hp_list_val}")
                
                # --- Apply "allow_spillover" action ---
                if not action_applied_flags["allow_spillover"] and "allow_spillover" in actions_from_rule:
                    spillover_val = actions_from_rule["allow_spillover"]
                    if isinstance(spillover_val, bool):
                        preselect_actions["allow_spillover"] = spillover_val
                        action_applied_flags["allow_spillover"] = True
                        logger.debug(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) SET Allow Spillover: {spillover_val}")
                    else:
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) has invalid 'allow_spillover' value (must be boolean): {spillover_val}")

                # --- Apply "start_pod_number" action ---
                if not action_applied_flags["start_pod_number"] and "start_pod_number" in actions_from_rule \
                   and actions_from_rule["start_pod_number"] is not None:
                    try:
                        preselect_actions["start_pod_number"] = max(1, int(actions_from_rule["start_pod_number"]))
                        action_applied_flags["start_pod_number"] = True
                        logger.debug(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) SET Start Pod Number: {preselect_actions['start_pod_number']}")
                    except (ValueError, TypeError):
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) has invalid 'start_pod_number' value: {actions_from_rule['start_pod_number']}.")

                # --- Apply "set_max_pods" (constraint) action ---
                if not action_applied_flags["max_pods_constraint"] and "set_max_pods" in actions_from_rule \
                   and actions_from_rule["set_max_pods"] is not None:
                    try:
                        preselect_actions["max_pods_constraint"] = max(1, int(actions_from_rule["set_max_pods"])) # Max pods should be at least 1
                        action_applied_flags["max_pods_constraint"] = True
                        logger.debug(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) SET Max Pods Constraint: {preselect_actions['max_pods_constraint']}")
                    except (ValueError, TypeError):
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) has invalid 'set_max_pods' value: {actions_from_rule['set_max_pods']}.")

                # --- Apply "calculate_pods_from_pax" action ---
                if not action_applied_flags["calculate_pods_from_pax"] and \
                   "calculate_pods_from_pax" in actions_from_rule and \
                   isinstance(actions_from_rule["calculate_pods_from_pax"], dict):
                    
                    calc_params = actions_from_rule["calculate_pods_from_pax"]
                    divisor_param = calc_params.get("divisor")
                    min_pods_param = calc_params.get("min_pods", 1) # Default min_pods to 1
                    use_field_param = calc_params.get("use_field", "pax").lower()

                    if divisor_param is None:
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}): 'calculate_pods_from_pax' missing 'divisor'. Skipping.")
                        continue # Skip this specific action from this rule
                    try:
                        divisor = int(divisor_param)
                        min_p = int(min_pods_param)
                        if divisor <= 0: raise ValueError("Divisor must be positive.")
                        if min_p < 0: raise ValueError("min_pods cannot be negative.") # min_p can be 0
                    except (ValueError, TypeError):
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}): Invalid 'divisor' or 'min_pods' in 'calculate_pods_from_pax'. Skipping.")
                        continue

                    student_count_for_calc = 0
                    if use_field_param == "pax": student_count_for_calc = sf_pax_count
                    elif use_field_param == "registered_attendees": student_count_for_calc = sf_registered_attendees
                    elif use_field_param == "max_registered_or_pax": student_count_for_calc = max(sf_pax_count, sf_registered_attendees)
                    else:
                        logger.warning(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}): Invalid 'use_field' value '{use_field_param}'. Defaulting to 'pax'.")
                        student_count_for_calc = sf_pax_count
                    
                    if student_count_for_calc <= 0:
                        calculated_pods = min_p 
                    else:
                        calculated_pods = math.ceil(student_count_for_calc / divisor)
                        calculated_pods = max(min_p, int(calculated_pods))

                    current_required_pods = calculated_pods # This overrides the previous value
                    action_applied_flags["calculate_pods_from_pax"] = True
                    logger.info(f"  Rule '{rule_name_for_log}' (P{rule_prio_for_log}) Applied 'calculate_pods_from_pax': "
                                f"Students({use_field_param}): {student_count_for_calc}, Divisor: {divisor}, Min: {min_p} "
                                f"=> New Required Pods: {current_required_pods}")
        
        # After all rules for this course, apply max_pods_constraint if set by any rule
        if preselect_actions["max_pods_constraint"] is not None:
            if current_required_pods > preselect_actions["max_pods_constraint"]:
                logger.info(f"  Applying Max Pods constraint for '{sf_course_code}': changing required pods from "
                            f"{current_required_pods} to {preselect_actions['max_pods_constraint']}.")
                current_required_pods = preselect_actions["max_pods_constraint"]

        final_num_pods_to_assign = max(0, current_required_pods) # Ensure it's not negative

        # Calculate final end_pod based on final start_pod and final_num_pods_to_assign
        # If 0 pods are assigned, end_pod will be start_pod - 1, which is fine for range logic.
        final_start_pod = preselect_actions["start_pod_number"]
        final_end_pod = (final_start_pod + final_num_pods_to_assign - 1) if final_num_pods_to_assign > 0 else (final_start_pod -1)


        # Store the final values in course_info. These are used by the API and subsequent dashboard steps.
        course_info['Pods Req.'] = final_num_pods_to_assign # Final calculated pods needed
        course_info['preselect_labbuild_course'] = preselect_actions["labbuild_course"]
        course_info['preselect_host'] = preselect_actions["host"]
        course_info['preselect_start_pod'] = final_start_pod
        course_info['preselect_end_pod'] = final_end_pod # Correctly calculated end_pod
        course_info['preselect_allow_spillover'] = preselect_actions["allow_spillover"]
        course_info['preselect_host_priority_list'] = preselect_actions["host_priority_list"]
        course_info['preselect_max_pods_applied_constraint'] = preselect_actions["max_pods_constraint"]
        
        output_list.append(course_info)
        logger.debug(f"SF Utils: Final processed course_info for '{sf_course_code}': {course_info}")

    return output_list


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