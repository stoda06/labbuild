import io
import logging
import requests
from datetime import datetime
from typing import List, Dict
from collections import defaultdict

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter

# --- Setup basic logging to see the output ---
# In a real app, this would be configured elsewhere.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
ALLOCATION_COLLECTION = "allocation"
HOST_COLLECTION = "host"  # Corrected to singular
INTERIM_ALLOCATION_COLLECTION = "interimallocation"

class ExcelStyle:
    CENTER_ALIGNMENT = Alignment(horizontal='center', vertical='center', wrap_text=True)
    THIN_BORDER = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    CYAN_HEADER_FILL = PatternFill(start_color="00B0F0", end_color="00B0F0", fill_type="solid")
    BLACK_BOLD_FONT = Font(bold=True, color="000000")

TRAINER_SHEET_HEADERS = {
    'Course Name': 'course_name',
    'Pod Number': 'pod_number',
    'Username': 'username',
    'Password': 'password',
    'Version': 'version',
    'RAM': 'ram',
    'Class': 'class',
    'Host': 'host',
    'vCenter': 'vcenter',
    'Taken By': 'taken_by',
    "Don't Delete Until US Courses Complete": 'notes'
}

REPORT_SECTIONS = [
    "F5 COURSE",
    "CHECK POINT",
    "PALO ALTO",
    "Other Vendors" # Fallback section
]

VENDOR_GROUP_MAP = {
    "PA": "PALO ALTO",
    "F5": "F5 COURSE",
    "CP": "CHECK POINT",
}

logger = logging.getLogger(__name__)

# ==============================================================================
# SPECIALIZED DATA FETCHING HELPERS
# ==============================================================================

def _create_host_to_vcenter_map(db: object) -> Dict[str, str]:
    """Helper to create a mapping from host names to their vCenter."""
    host_map = {}
    try:
        collection = db[HOST_COLLECTION]
        for host_doc in collection.find({}):
            host_name = host_doc.get("host_name")
            vcenter = host_doc.get("vcenter")
            if host_name and vcenter:
                clean_host_name = host_name.strip().lower()
                host_map[clean_host_name] = vcenter
    except Exception as e:
        logger.error(f"Could not create host-to-vcenter map: {e}", exc_info=True)
    logger.info(f"Host-to-vCenter Map created. Total entries: {len(host_map)}")
    return host_map


def _fetch_credentials_from_course2() -> Dict[str, Dict[str, str]]:
    """Fetches credentials and stores username, password, and class name."""
    credential_map = {}
    url = 'http://connect:1212/list'
    try:
        logger.info(f"Fetching credentials from course2 service at {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        api_data = response.json()

        for username, details in api_data.items():
            if isinstance(details, dict) and details.get('vpn_auth_course_code') == 'Trainer':
                course_version = details.get('vpn_auth_version')
                password_value = details.get('vpn_auth_class') # As per original logic, this is the password
                class_name = details.get('vpn_auth_class') # This is for the "Class" column

                if course_version and username and password_value:
                    lookup_key = course_version.strip()
                    credential_map[lookup_key] = {
                        "username": username,
                        "password": password_value,
                        "class": class_name  # Store the class name separately
                    }
                    
        logger.info(f"Successfully loaded {len(credential_map)} TRAINER credentials from course2.")
    except requests.exceptions.RequestException as e:
        logger.error(f"FATAL: Could not connect to course2 service. Credentials will be blank. Error: {e}")
    except (ValueError, requests.exceptions.JSONDecodeError):
        logger.error(f"FATAL: Could not parse JSON response from course2 service.")
    return credential_map


def _fetch_from_current_allocations(db: object, host_map: Dict, credential_map: Dict) -> List[Dict]:
    """Data Source 1: Fetches trainer pods from 'allocation'."""
    processed_data = []
    collection = db[ALLOCATION_COLLECTION]
    query = {"$or": [{"tag": "untagged"}, {"tag": {"$regex": "[-_](TP|Trainer-Pods)$", "$options": "i"}}]}
    logger.info(f"Fetching CURRENT week data from '{ALLOCATION_COLLECTION}'...")
    
    for doc in collection.find(query):
        for course_item in doc.get('courses', []):
            if not isinstance(course_item, dict): continue
            
            course_name_from_db = course_item.get('course_name', 'N/A')
            credential_lookup_key = course_name_from_db.strip()
            creds = credential_map.get(credential_lookup_key, {})

            vendor_code = course_item.get('vendor', '').upper()
            vendor_group_name = VENDOR_GROUP_MAP.get(vendor_code, 'Other Vendors')

            # MODIFICATION: The 'Class' column is now always blank for manual entry.
            class_value = ''

            for pod_detail in course_item.get('pod_details', []):
                if not isinstance(pod_detail, dict): continue
                host_name_from_alloc = pod_detail.get('host', 'N/A')
                short_host_name = host_name_from_alloc.strip().split('.')[0]
                lookup_key = short_host_name.lower()
                full_vcenter = host_map.get(lookup_key, 'N/A')
                
                processed_data.append({
                    'course_name': course_name_from_db,
                    'pod_number': pod_detail.get('pod_number'),
                    'username': creds.get('username', ''),
                    'password': creds.get('password', ''),
                    'version': course_name_from_db,
                    'ram': pod_detail.get('memory_gb_one_pod', 'N/A'),
                    'class': class_value,  # This will now always be blank
                    'host': host_name_from_alloc, 
                    'vcenter': full_vcenter.split('.')[0],
                    'taken_by': '', 'notes': '', 'vendor': vendor_group_name
                })
    return processed_data


def _fetch_from_interim_allocations(db: object, host_map: Dict, credential_map: Dict) -> List[Dict]:
    """Data Source 2: Fetches trainer pods from 'interimallocation'."""
    processed_data = []
    collection = db[INTERIM_ALLOCATION_COLLECTION]
    query = {"sf_trainer_name": "Trainer Pods"}
    logger.info(f"Fetching NEXT week data from '{INTERIM_ALLOCATION_COLLECTION}'...")

    for doc in collection.find(query):
        course_version_key_from_db = doc.get('final_labbuild_course')
        credential_lookup_key = course_version_key_from_db.strip() if course_version_key_from_db else None
        creds = credential_map.get(credential_lookup_key, {}) if credential_lookup_key else {}

        vendor_code = doc.get('vendor', '').upper()
        vendor_group_name = VENDOR_GROUP_MAP.get(vendor_code, 'Other Vendors')
        
        # MODIFICATION: The 'Class' column is now always blank for manual entry.
        class_value = ''

        ram_per_pod = doc.get('memory_gb_one_pod', 'N/A')
        
        for assignment in doc.get('assignments', []):
            try:
                start, end = assignment.get('start_pod'), assignment.get('end_pod')
                if start is not None and end is not None:
                    for pod_num in range(int(start), int(end) + 1):
                        host_name_from_alloc = assignment.get('host', 'N/A')
                        short_host_name = host_name_from_alloc.strip().split('.')[0]
                        lookup_key = short_host_name.lower()
                        full_vcenter = host_map.get(lookup_key, 'N/A')
                        
                        processed_data.append({
                            'course_name': doc.get('sf_course_type', 'N/A'),
                            'pod_number': pod_num,
                            'username': creds.get('username', ''),
                            'password': creds.get('password', ''),
                            'version': course_version_key_from_db,
                            'ram': ram_per_pod,
                            'class': class_value,  # This will now always be blank
                            'host': host_name_from_alloc, 
                            'vcenter': full_vcenter.split('.')[0],
                            'taken_by': '', 
                            'notes': doc.get('trainer_assignment_warning', ''),
                            'vendor': vendor_group_name
                        })
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping malformed assignment in doc '{doc.get('_id')}': {e}")
                continue
    return processed_data

# ==============================================================================
# MAIN PUBLIC FUNCTION WITH DECISION LOGIC
# ==============================================================================

def fetch_trainer_pod_data(db: object) -> List[Dict]:
    """Decides data source, fetches and processes trainer pod data."""
    today_weekday = datetime.today().weekday()
    host_map = _create_host_to_vcenter_map(db)
    credential_map = _fetch_credentials_from_course2()
    if today_weekday < 2:
        logger.info("Day is before Wednesday. Using CURRENT week's data source ('allocation').")
        return _fetch_from_current_allocations(db, host_map, credential_map)
    else:
        logger.info("Day is on or after Wednesday. Using NEXT week's data source ('interimallocation').")
        return _fetch_from_interim_allocations(db, host_map, credential_map)

# ==============================================================================
# EXCEL REPORT GENERATION LOGIC
# ==============================================================================

def _apply_style(cell, fill=None, font=None, alignment=None, border=None):
    """Helper to apply multiple styles to a cell."""
    if fill: cell.fill = fill
    if font: cell.font = font
    if alignment: cell.alignment = alignment
    if border: cell.border = border

def create_trainer_report_in_memory(trainer_pods: List[Dict]) -> io.BytesIO:
    """Generates the trainer pod allocation report in memory."""
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Trainer Pod Allocation"

    num_columns = len(TRAINER_SHEET_HEADERS)
    
    # --- Main Title ---
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=num_columns)
    title_cell = sheet["A1"]
    title_cell.value = "TRAINER POD ALLOCATION"
    _apply_style(title_cell, font=Font(bold=True, size=16), alignment=ExcelStyle.CENTER_ALIGNMENT)
    sheet.row_dimensions[1].height = 20

    grouped_by_vendor = defaultdict(list)
    for pod in trainer_pods:
        vendor_name = pod.get("vendor", "Other Vendors")
        grouped_by_vendor[vendor_name].append(pod)

    current_row = 3
    for section_name in REPORT_SECTIONS:
        records_for_this_section = grouped_by_vendor.get(section_name, [])
        if records_for_this_section:
            sheet.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=num_columns)
            cell = sheet.cell(row=current_row, column=1, value=section_name.upper())
            _apply_style(cell, fill=ExcelStyle.CYAN_HEADER_FILL, font=ExcelStyle.BLACK_BOLD_FONT)
            current_row += 1

            headers = list(TRAINER_SHEET_HEADERS.keys())
            for col_idx, header_text in enumerate(headers, 1):
                cell = sheet.cell(row=current_row, column=col_idx, value=header_text)
                _apply_style(cell, fill=ExcelStyle.CYAN_HEADER_FILL, font=ExcelStyle.BLACK_BOLD_FONT,
                             alignment=ExcelStyle.CENTER_ALIGNMENT, border=ExcelStyle.THIN_BORDER)
            current_row += 1

            def sort_key(record):
                course = record.get('course_name', '')
                try: pod_num = int(record.get('pod_number', 0))
                except (ValueError, TypeError): pod_num = 0
                return (course, pod_num)

            for record in sorted(records_for_this_section, key=sort_key):
                for col_idx, header in enumerate(headers, 1):
                    data_key = TRAINER_SHEET_HEADERS[header]
                    value = record.get(data_key)
                    cell = sheet.cell(row=current_row, column=col_idx, value=value)
                    _apply_style(cell, border=ExcelStyle.THIN_BORDER, alignment=ExcelStyle.CENTER_ALIGNMENT)
                current_row += 1
            
            current_row += 1

    # Set Column Widths
    header_to_col_letter = {header: get_column_letter(i) for i, header in enumerate(TRAINER_SHEET_HEADERS.keys(), 1)}
    
    sheet.column_dimensions[header_to_col_letter['Course Name']].width = 35
    sheet.column_dimensions[header_to_col_letter['Pod Number']].width = 12
    sheet.column_dimensions[header_to_col_letter['Username']].width = 15
    sheet.column_dimensions[header_to_col_letter['Password']].width = 15
    sheet.column_dimensions[header_to_col_letter['Version']].width = 25
    sheet.column_dimensions[header_to_col_letter['RAM']].width = 8
    sheet.column_dimensions[header_to_col_letter['Class']].width = 10 # Adjusted width for a blank column
    sheet.column_dimensions[header_to_col_letter['Host']].width = 25
    sheet.column_dimensions[header_to_col_letter['vCenter']].width = 20
    sheet.column_dimensions[header_to_col_letter["Don't Delete Until US Courses Complete"]].width = 40

    in_memory_fp = io.BytesIO()
    wb.save(in_memory_fp)
    in_memory_fp.seek(0)
    
    logger.info("Successfully generated trainer pod report in memory.")
    return in_memory_fp