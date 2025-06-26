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
    ExcelStyle, TRAINER_SHEET_HEADERS, REPORT_SECTIONS, VENDOR_GROUP_MAP,
    ALLOCATION_COLLECTION, HOST_COLLECTION
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

def fetch_trainer_pod_data(db):
    """Fetches and processes ONLY trainer pods for the report."""
    processed_data = []
    try:
        host_map = _create_host_to_vcenter_map(db)
        collection = db[ALLOCATION_COLLECTION]
        logger.info(f"Querying for TRAINER PODS with filter: {TRAINER_QUERY}")
        cursor = collection.find(TRAINER_QUERY)
        
        for doc in cursor:
            for course_info in doc.get('courses', []):
                for pod_level_2 in course_info.get('pod_details', []):
                    if 'pods' in pod_level_2 and pod_level_2.get('pods'):
                        for final_pod_details in pod_level_2.get('pods', []):
                            flat_record = _assemble_record(doc, course_info, pod_level_2, final_pod_details, host_map)
                            processed_data.append(flat_record)
                    else:
                        final_pod_details = pod_level_2 
                        flat_record = _assemble_record(doc, course_info, pod_level_2, final_pod_details, host_map)
                        processed_data.append(flat_record)

        logger.info(f"Successfully processed {len(processed_data)} individual TRAINER pods.")
        return processed_data
    except Exception as e:
        logger.error(f"An error occurred while fetching trainer data: {e}", exc_info=True)
        raise e


# --- Excel Generation Logic (from your report_generator.py) ---

def _apply_style(cell, fill=None, font=None, alignment=None, border=None):
    if fill: cell.fill = fill
    if font: cell.font = font
    if alignment: cell.alignment = alignment
    if border: cell.border = border

def create_trainer_report_in_memory(trainer_pods: List[Dict]) -> io.BytesIO:
    """Generates the trainer report and saves it to an in-memory stream."""
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Trainer Pod Allocation"

    num_columns = len(TRAINER_SHEET_HEADERS)
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=num_columns)
    title_cell = sheet["A1"]
    title_cell.value = "TRAINER POD ALLOCATION"
    _apply_style(title_cell, font=Font(bold=True, size=16), alignment=ExcelStyle.CENTER_ALIGNMENT.value)
    sheet.row_dimensions[1].height = 20

    grouped_by_vendor = defaultdict(list)
    for pod in trainer_pods:
        vendor_name = pod.get("vendor")
        if vendor_name and vendor_name != "Other Vendors":
            grouped_by_vendor[vendor_name].append(pod)

    current_row = 3
    # Use the REPORT_SECTIONS from the constants file
    for section_name in REPORT_SECTIONS:
        sheet.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=num_columns)
        cell = sheet.cell(row=current_row, column=1, value=section_name)
        # Use styles from the main ExcelStyle class
        _apply_style(cell, fill=ExcelStyle.LIGHT_BLUE_FILL.value, font=Font(bold=True, color="000000", size=14))
        current_row += 1

        headers = list(TRAINER_SHEET_HEADERS.keys())
        for col_idx, header_text in enumerate(headers, 1):
            cell = sheet.cell(row=current_row, column=col_idx, value=header_text)
            _apply_style(cell, fill=ExcelStyle.LIGHT_BLUE_FILL.value, font=Font(bold=True),
                         alignment=ExcelStyle.CENTER_ALIGNMENT.value, border=ExcelStyle.THIN_BORDER.value)
        current_row += 1

        records_for_this_section = grouped_by_vendor.get(section_name, [])

        if records_for_this_section:
            def sort_key(record):
                course = record.get('course_name', '')
                pod_num_str = str(record.get('pod_number', '0'))
                pod_num = int(pod_num_str) if pod_num_str.isdigit() else 0
                return (course, pod_num)

            sorted_records = sorted(records_for_this_section, key=sort_key)

            for record in sorted_records:
                for col_idx, header in enumerate(headers, 1):
                    data_key = TRAINER_SHEET_HEADERS[header]
                    value = record.get(data_key)
                    cell = sheet.cell(row=current_row, column=col_idx, value=value)
                    _apply_style(cell, border=ExcelStyle.THIN_BORDER.value, alignment=ExcelStyle.CENTER_ALIGNMENT.value)
                current_row += 1
        current_row += 1

    column_widths = {'A': 35, 'B': 12, 'C': 15, 'D': 15, 'E': 20, 'F': 8, 'G': 8, 'H': 15, 'I': 20, 'J': 15, 'K': 35}
    for col_letter, width in column_widths.items():
        if col_letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:num_columns]:
            sheet.column_dimensions[col_letter].width = width

    # Save to memory
    in_memory_fp = io.BytesIO()
    wb.save(in_memory_fp)
    in_memory_fp.seek(0)
    
    logger.info("Successfully generated trainer pod report in memory.")
    return in_memory_fp