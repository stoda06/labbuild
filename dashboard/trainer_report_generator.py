import io
import logging
from datetime import datetime
from typing import List, Dict
from collections import defaultdict

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

# Import from your project's main constants file
from constants import (
    ALLOCATION_COLLECTION,
    HOST_COLLECTION,
    INTERIM_ALLOCATION_COLLECTION
)

from excelreport_config import (
    ExcelStyle, TRAINER_SHEET_HEADERS,
    REPORT_SECTIONS, VENDOR_GROUP_MAP
)


logger = logging.getLogger(__name__)

# --- Data Fetching and Processing Logic (from your run_report.py) ---

TRAINER_QUERY = {"tag": {"$regex": "[-_]TP$"}}

def _create_host_to_vcenter_map(db):
    host_map = {}
    collection = db[HOST_COLLECTION]
    for host_doc in collection.find({}):
        host_name = host_doc.get("host_name")
        vcenter = host_doc.get("vcenter")
        if host_name and vcenter:
            host_map[host_name] = vcenter
    return host_map

def _assemble_record(doc, course_info, pod_level_2, final_pod_details, host_map):
    flat_record = {}
    tag = doc.get('tag', '')
    host_name = final_pod_details.get('host')
    flat_record['host'] = host_name
    flat_record['vcenter'] = host_map.get(host_name, 'N/A')
    flat_record['course_name'] = tag
    flat_record['version'] = course_info.get('course_name')
    flat_record['username'] = course_info.get('apm_username')
    flat_record['password'] = course_info.get('apm_password')
    flat_record['pod_number'] = final_pod_details.get('pod_number')
    flat_record['ram'] = final_pod_details.get('ram')
    flat_record['taken_by'] = final_pod_details.get('taken_by')
    flat_record['notes'] = final_pod_details.get('notes')
    if 'class_number' in pod_level_2:
        flat_record['class'] = pod_level_2.get('class_number')
    vendor = 'Other Vendors'
    for prefix, vendor_name in VENDOR_GROUP_MAP.items():
        if tag.upper().startswith(prefix):
            vendor = vendor_name
            break
    flat_record['vendor'] = vendor
    return flat_record

# --- REVISED AND CORRECTED FUNCTION ---
# Replace your previous fetch_trainer_pod_data function with this one.

def fetch_trainer_pod_data(db):
    """
    Fetches and processes trainer pods from the interimallocation collection,
    handling pod number ranges and applying specific formatting rules for the report.
    """
    processed_data = []
    try:
        host_map = _create_host_to_vcenter_map(db)
        collection = db[INTERIM_ALLOCATION_COLLECTION]
        
        logger.info("Querying for trainer data in 'interimallocation' collection.")
        cursor = collection.find({"status": "trainer_confirmed"}) # Or adjust filter as needed

        for doc in cursor:
            # Loop through each assignment object for a given course
            for assignment in doc.get('trainer_assignment', []):
                
                try:
                    start = assignment.get('start_pod')
                    end = assignment.get('end_pod')

                    if start is not None and end is not None:
                        for pod_num in range(int(start), int(end) + 1):
                            
                            flat_record = {}
                            
                            host_name = assignment.get('host', 'N/A')
                            full_vcenter = host_map.get(host_name, 'N/A')
                            
                            flat_record['host'] = host_name
                            flat_record['vcenter'] = full_vcenter.split('.')[0]
                            flat_record['course_name'] = doc.get('sf_course_type', 'N/A')
                            flat_record['version'] = doc.get('final_labbuild_course', 'N/A')
                            flat_record['username'] = doc.get('trainer_apm_username', 'N/A')
                            flat_record['password'] = doc.get('trainer_apm_password', 'N/A')
                            flat_record['pod_number'] = pod_num
                            flat_record['ram'] = doc.get('trainer_memory_gb_one_pod', 'N/A')
                            flat_record['taken_by'] = '' # Kept blank for manual entry
                            flat_record['notes'] = doc.get('trainer_assignment_warning')
                            
                            vendor_code = doc.get('vendor', '').upper()
                            vendor = VENDOR_GROUP_MAP.get(vendor_code, 'Other Vendors')
                            flat_record['vendor'] = vendor

                            # --- THIS IS THE KEY LOGIC FOR THE "CLASS" FIELD ---
                            # It specifically checks if the vendor is F5.
                            # If it is not F5, the 'else' block runs, making the field blank.
                            if vendor_code == 'F5':
                                # Only for F5, try to get the class number.
                                flat_record['class'] = doc.get('sf_class_number', '') 
                            else:
                                # For ALL other vendors (Palo Alto, Check Point, etc.), set to blank.
                                flat_record['class'] = ''

                            processed_data.append(flat_record)

                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping pod range due to invalid data for doc _id {doc.get('_id')}: {e}")
                    continue

        logger.info(f"Successfully processed {len(processed_data)} individual TRAINER pods from 'interimallocation'.")
        return processed_data
    except Exception as e:
        logger.error(f"An error occurred while fetching trainer data from interimallocation: {e}", exc_info=True)
        raise e


# --- Excel Generation Logic (from your report_generator.py) ---

def _apply_style(cell, fill=None, font=None, alignment=None, border=None):
    if fill: cell.fill = fill
    if font: cell.font = font
    if alignment: cell.alignment = alignment
    if border: cell.border = border

# --- REVISED FUNCTION WITH CONDITIONAL SECTION GENERATION ---
# Replace your old create_trainer_report_in_memory function with this one.

# --- REVISED FUNCTION WITH CONDITIONAL HEADER NAME ---
# This version keeps the column structure consistent for all sections.

def create_trainer_report_in_memory(trainer_pods: List[Dict]) -> io.BytesIO:
    """
    Generates the trainer report. The layout is consistent, but the "Class"
    header text is only displayed for the "F5 COURSE" section.
    """
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Trainer Pod Allocation"

    num_columns = len(TRAINER_SHEET_HEADERS)
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=num_columns)
    title_cell = sheet["A1"]
    title_cell.value = "TRAINER POD ALLOCATION"
    _apply_style(title_cell, font=Font(bold=True, size=16), alignment=ExcelStyle.CENTER_ALIGNMENT.value)
    sheet.row_dimensions[1].height = 20

    # Group all fetched pods by their assigned vendor group name
    grouped_by_vendor = defaultdict(list)
    for pod in trainer_pods:
        vendor_name = pod.get("vendor")
        if vendor_name:
            grouped_by_vendor[vendor_name].append(pod)

    current_row = 3
    # Use the REPORT_SECTIONS from the constants file to maintain order
    for section_name in REPORT_SECTIONS:
        
        records_for_this_section = grouped_by_vendor.get(section_name, [])

        if records_for_this_section:
            
            # 1. Write the blue section header (e.g., "PR COURSE")
            sheet.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=num_columns)
            cell = sheet.cell(row=current_row, column=1, value=section_name)
            _apply_style(cell, fill=ExcelStyle.LIGHT_BLUE_FILL.value, font=Font(bold=True, color="000000", size=14))
            current_row += 1

            # 2. Write the column headers (Course Name, Version, etc.)
            headers = list(TRAINER_SHEET_HEADERS.keys())
            for col_idx, header_text in enumerate(headers, 1):
                
                # --- KEY CHANGE: Conditionally blank out the 'Class' header ---
                # The header text is set to blank if it is 'Class' AND the section is not 'F5 COURSE'
                value_to_write = header_text
                if header_text == 'Class' and section_name != 'F5 COURSE':
                    value_to_write = '' # This makes the header cell blank

                cell = sheet.cell(row=current_row, column=col_idx, value=value_to_write)
                _apply_style(cell, fill=ExcelStyle.LIGHT_BLUE_FILL.value, font=Font(bold=True),
                             alignment=ExcelStyle.CENTER_ALIGNMENT.value, border=ExcelStyle.THIN_BORDER.value)
            current_row += 1

            # 3. Sort and write the actual data rows for the pods
            def sort_key(record):
                course = record.get('course_name', '')
                pod_num = int(record.get('pod_number', 0))
                return (course, pod_num)

            sorted_records = sorted(records_for_this_section, key=sort_key)

            for record in sorted_records:
                for col_idx, header in enumerate(headers, 1):
                    data_key = TRAINER_SHEET_HEADERS[header]
                    value = record.get(data_key)
                    cell = sheet.cell(row=current_row, column=col_idx, value=value)
                    _apply_style(cell, border=ExcelStyle.THIN_BORDER.value, alignment=ExcelStyle.CENTER_ALIGNMENT.value)
                current_row += 1
            
            # 4. Add a blank row after the section is complete
            current_row += 1

    # Adjust column widths (this part remains the same)
    # The columns are now fixed, so we can use the original letter-based widths.
    # Make sure the 'Class' column (likely G or H) has a width defined here.
    column_widths = {
        'A': 35, 'B': 12, 'C': 15, 'D': 15, 'E': 20, 'F': 8, 'G': 8, 'H': 15, 
        'I': 20, 'J': 15, 'K': 35
    } # Assuming 'Class' is column 'G'. Adjust if your header order is different.
    
    for col_letter, width in column_widths.items():
        if col_letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:num_columns]:
            sheet.column_dimensions[col_letter].width = width

    # Save to memory
    in_memory_fp = io.BytesIO()
    wb.save(in_memory_fp)
    in_memory_fp.seek(0)
    
    logger.info("Successfully generated trainer pod report in memory.")
    return in_memory_fp