# labbuild/dashboard/upcoming_report_generator.py
# This file generates the "Upcoming Lab Report" which uses data from the
# interimallocation collection and also includes extended courses from the
# currentallocation collection.

import io
import logging
import re
import requests
import pymongo
from flask import current_app
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter

# Note: We now import all relevant collection constants
from constants import (
    INTERIM_ALLOCATION_COLLECTION,
    ALLOCATION_COLLECTION,
    HOST_COLLECTION,
    COURSE_CONFIG_COLLECTION
)

from excelreport_config import (
    ExcelStyle,
    AU_HOST_NAMES, US_HOST_NAMES,
    AVAILABLE_RAM_GB, SUMMARY_ENV_ORDER,
    RAM_SUMMARY_START_COL, EXCEL_GROUP_ORDER, EXCEL_COLUMN_WIDTHS
)

# --- Logging Setup ---
logger = logging.getLogger(__name__)

# ==============================================================================
# ALL THE HELPER AND DATA PROCESSING FUNCTIONS
# ==============================================================================

def calculate_ram_summary(all_allocations: List[Dict], host_map: Dict) -> Dict[str, float]:
    allocated_ram_by_env = {env_key: 0 for env_key in host_map.keys()}
    for allocation in all_allocations:
        ram = convert_to_numeric(allocation.get("ram"))
        if not ram or ram <= 0: continue
        virtual_hosts_str = allocation.get("virtual_hosts", "").lower() if allocation.get("virtual_hosts") else ""
        if not virtual_hosts_str: continue
        for env_key, host_prefix in host_map.items():
            if host_prefix.lower() in virtual_hosts_str:
                total_pods_for_course = convert_to_numeric(allocation.get("vendor_pods")) or 1
                if total_pods_for_course <= 1:
                    allocated_ram_by_env[env_key] += ram
                else:
                    allocated_ram_by_env[env_key] += ram + (total_pods_for_course - 1) * ram / 2
    return allocated_ram_by_env

def convert_to_numeric(val):
    if val is None: return None
    if isinstance(val, (int, float)): return val
    if isinstance(val, str):
        val = val.strip()
        if not val: return None
        try: return int(val)
        except ValueError:
            try: return float(val)
            except ValueError: return val
    return val

def apply_style(cell, trainer=False, use_green_fill=False, is_summary=False):
    if cell is None: return
    cell.border = ExcelStyle.THIN_BORDER.value
    cell.alignment = ExcelStyle.CENTER_ALIGNMENT.value
    if trainer: cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value
    elif use_green_fill: cell.fill = ExcelStyle.GREEN_FILL.value
    elif is_summary: cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value

def write_cell(sheet, row, col, value, trainer=False, use_green_fill=False, number_format=None, is_summary=False):
    cell = sheet.cell(row=row, column=col)
    cell.value = convert_to_numeric(value) if not (isinstance(value, str) and value.startswith("=")) else value
    apply_style(cell, trainer, use_green_fill, is_summary)
    if number_format: cell.number_format = number_format

def write_group_title(sheet, row, title):
    # This function now only sets the font, not borders or fill.
    cell = sheet.cell(row=row, column=1, value=title)
    cell.font = Font(bold=True, size=14)
    return row + 1

def write_merged_header(sheet, row, col, header_text):
    sheet.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + 2)
    for c in range(col, col + 3):
        cell = sheet.cell(row=row, column=c)
        cell.font = Font(bold=True)
        cell.border = ExcelStyle.THIN_BORDER.value
        cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value
        cell.alignment = ExcelStyle.CENTER_ALIGNMENT.value
    sheet.cell(row=row, column=col, value=header_text)

def write_summary_section(sheet, row_offset):
    start_col = RAM_SUMMARY_START_COL
    env_keys = SUMMARY_ENV_ORDER
    headers = ["RAM Summary", "Total", *env_keys]
    for i, header in enumerate(headers):
        col = start_col + i
        cell = sheet.cell(row=row_offset, column=col, value=header)
        cell.font = Font(bold=True)
        apply_style(cell, is_summary=True)
    available_ram_row, allocated_ram_row = row_offset + 1, row_offset + 2
    allocated_pct_row, remaining_ram_row = row_offset + 3, row_offset + 4
    sheet.cell(row=available_ram_row, column=start_col, value="Available RAM (GB)").font = Font(bold=True)
    sheet.cell(row=allocated_ram_row, column=start_col, value="Allocated RAM (GB)").font = Font(bold=True)
    sheet.cell(row=allocated_pct_row, column=start_col, value="Allocated RAM (%)").font = Font(bold=True)
    sheet.cell(row=remaining_ram_row, column=start_col, value="Remaining RAM (GB)").font = Font(bold=True)
    total_col_letter = get_column_letter(start_col + 1)
    
    first_data_row = 13
    last_data_row = 500

    for i, env_key in enumerate(env_keys):
        col, col_letter = start_col + 2 + i, get_column_letter(start_col + 2 + i)
        available_ram = AVAILABLE_RAM_GB.get(env_key, 0)
        write_cell(sheet, available_ram_row, col, available_ram, is_summary=False, number_format='0')
        
        formula_ram = f"=SUM({col_letter}{first_data_row}:{col_letter}{last_data_row})"
        write_cell(sheet, allocated_ram_row, col, formula_ram, is_summary=False, number_format='0.0')
        
        formula_pct = f"=IF({col_letter}{available_ram_row}>0, {col_letter}{allocated_ram_row}/{col_letter}{available_ram_row}, 0)"
        write_cell(sheet, allocated_pct_row, col, formula_pct, is_summary=False, number_format="0%")
        formula_rem = f"={col_letter}{available_ram_row}-{col_letter}{allocated_ram_row}"
        write_cell(sheet, remaining_ram_row, col, formula_rem, is_summary=False, number_format='0.0')
        
    first_env, last_env = get_column_letter(start_col + 2), get_column_letter(start_col + 1 + len(env_keys))
    write_cell(sheet, available_ram_row, start_col + 1, f"=SUM({first_env}{available_ram_row}:{last_env}{available_ram_row})", number_format='0')
    write_cell(sheet, allocated_ram_row, start_col + 1, f"=SUM({first_env}{allocated_ram_row}:{last_env}{allocated_ram_row})", number_format='0.0')
    write_cell(sheet, allocated_pct_row, start_col + 1, f"=IF({total_col_letter}{available_ram_row}>0, {total_col_letter}{allocated_ram_row}/{total_col_letter}{available_ram_row}, 0)", number_format="0%")
    write_cell(sheet, remaining_ram_row, start_col + 1, f"={total_col_letter}{available_ram_row}-{total_col_letter}{allocated_ram_row}", number_format='0.0')
    for r in [allocated_ram_row, allocated_pct_row, remaining_ram_row]:
        for c in range(start_col + 1, start_col + 2 + len(env_keys)):
            cell = sheet.cell(row=r, column=c)
            cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
            cell.font = Font(bold=True, color="FF0000")

def _write_data_row(sheet, row, entry, headers, header_keys, header_pos, is_trainer, use_green_fill, host_map: Dict):
    trainer_flag, green_flag = (True, False) if is_trainer else (False, use_green_fill)
    virtual_hosts_str = entry.get("virtual_hosts", "").lower() if entry.get("virtual_hosts") else ""
    col = 1
    
    for h in header_keys:
        if h == "Start/End Pod":
            pod = entry.get("start_end_pod", "")
            parts = pod.replace("→", "->").replace("–", "-").split("-")
            left, right = (parts[0].strip(), parts[1].strip()) if len(parts) > 1 else (parts[0].strip() if parts else "", "")
            right = right if right else left
            write_cell(sheet, row, col, left, trainer=trainer_flag, use_green_fill=green_flag); col += 1
            write_cell(sheet, row, col, "->", trainer=trainer_flag, use_green_fill=green_flag); col += 1
            write_cell(sheet, row, col, right, trainer=trainer_flag, use_green_fill=green_flag); col += 1
        elif h in host_map:
            host_name_for_this_column = host_map[h].lower()
            hosts_list = [h.strip().lower() for h in virtual_hosts_str.split(',') if h.strip()]
            if host_name_for_this_column in virtual_hosts_str:
                ram_col, pods_col = get_column_letter(header_pos["RAM"]), get_column_letter(header_pos["Vendor Pods"])
                formula = f"=IF({pods_col}{row}<=1, {ram_col}{row}, {ram_col}{row} + ({pods_col}{row}-1)*{ram_col}{row}/2)"
                write_cell(sheet, row, col, formula, trainer_flag, green_flag, number_format='0.0')
            else:
                write_cell(sheet, row, col, 0, trainer_flag, green_flag, number_format='0.0')
            col += 1
        else:
            data_key = headers.get(h)
            val_to_write = entry.get(data_key) if data_key else ""
            num_format = '0.0' if h == "RAM" else None
            write_cell(sheet, row, col, val_to_write, trainer_flag, green_flag, number_format=num_format)
            col += 1

def write_group_summary_boxes(sheet, start_row, header_pos, group_pod_total_or_formula, group_host_ram_totals, group_name, group_end_col, data_start_row, data_end_row):
    vendor_col = header_pos.get("Vendor Pods")
    students_col = header_pos.get("Students")
    has_pod_summary = vendor_col and students_col and (group_pod_total_or_formula is not None)
    has_ram_summary = sum(group_host_ram_totals.values()) > 0
    if not (has_pod_summary or has_ram_summary): return start_row
    
    label_start, label_end = start_row, start_row + 1
    value_start, value_end = start_row + 2, start_row + 3
    summary_end = value_end
    thin, no_side = Side(style='thin'), Side(style=None)
    border_top, border_bottom = Border(left=thin, right=thin, top=thin, bottom=no_side), Border(left=thin, right=thin, top=no_side, bottom=thin)
    for r in range(start_row, summary_end + 1):
        for c in range(1, group_end_col + 1):
            cell = sheet.cell(row=r, column=c)
            cell.alignment = ExcelStyle.CENTER_ALIGNMENT.value
            cell.border = border_top if r in [label_start, value_start] else border_bottom
    
    if has_pod_summary:
        merge_start = students_col
        merge_end = vendor_col
        
        sheet.merge_cells(start_row=label_start, start_column=merge_start, end_row=label_end, end_column=merge_end)
        label_cell = sheet.cell(row=label_start, column=merge_start, value="Total Pods")
        label_cell.font = Font(bold=True, size=20)
        label_cell.alignment = Alignment(horizontal='center', vertical='center')
        label_cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value

        sheet.merge_cells(start_row=value_start, start_column=merge_start, end_row=value_end, end_column=merge_end)
        
        value_cell = sheet.cell(row=value_start, column=merge_start, value=group_pod_total_or_formula)
        value_cell.font = Font(bold=True, size=20)
        value_cell.alignment = Alignment(horizontal='center', vertical='center')
        value_cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value

    if has_ram_summary:
        for host_key, host_ram in group_host_ram_totals.items():
            if host_col := header_pos.get(host_key):
                cell_top, cell_bottom = sheet.cell(row=value_start, column=host_col), sheet.cell(row=value_end, column=host_col)
                if data_start_row <= data_end_row:
                    host_col_letter = get_column_letter(host_col)
                    formula = f"=SUM({host_col_letter}{data_start_row}:{host_col_letter}{data_end_row})"
                    cell_bottom.value = formula
                else:
                    cell_bottom.value = 0
                
                cell_bottom.number_format = '0.0'
                cell_bottom.font = Font(bold=True, color="FFFF00")
                cell_top.fill = ExcelStyle.LIGHT_BLUE_FILL.value
                cell_bottom.fill = ExcelStyle.LIGHT_BLUE_FILL.value
    return start_row + 4

def apply_outer_border(sheet, start_row, end_row, start_col, end_col):
    """
    Applies an outer border to a range of cells.
    The top border is thin, while the other sides are medium.
    This version correctly handles existing borders in cells.
    """
    thin_side = Side(style='thin')
    medium_side = Side(style='medium')

    # Helper to copy and modify a border safely
    def set_border_side(cell, top=None, left=None, right=None, bottom=None):
        border = cell.border.copy()
        if top: border.top = top
        if left: border.left = left
        if right: border.right = right
        if bottom: border.bottom = bottom
        cell.border = border

    # Apply borders to the four edges of the range
    for col in range(start_col, end_col + 1):
        set_border_side(sheet.cell(row=start_row, column=col), top=thin_side)
        set_border_side(sheet.cell(row=end_row, column=col), bottom=medium_side)
    
    for row in range(start_row, end_row + 1):
        set_border_side(sheet.cell(row=row, column=start_col), left=medium_side)
        set_border_side(sheet.cell(row=row, column=end_col), right=medium_side)

def determine_us_au_location(virtual_hosts_str: str) -> str:
    if not virtual_hosts_str: return ""
    hosts_lower = virtual_hosts_str.lower()
    if any(au in hosts_lower for au in AU_HOST_NAMES): return "AU"
    if any(us in hosts_lower for us in US_HOST_NAMES): return "US"
    return ""

def format_date(date_str):
    if not date_str: return ""
    try: return datetime.strptime(date_str, '%Y-%m-%d').strftime('%A, %d/%m')
    except Exception: return date_str

def get_vendor_prefix_map():
    return {gn.split(' ')[0].lower(): gn for gn in ExcelStyle.DEFAULT_COURSE_GROUPS.value.keys()}

def find_location_from_code(course_code: str, location_map: dict) -> str:
    if not course_code: return ""
    for loc_code in sorted(location_map.keys(), key=len, reverse=True):
        if loc_code in course_code: return location_map[loc_code]
    return ""

def _find_field(doc, keys):
    course_details = doc.get('courses', [{}])[0]
    for key in keys:
        if doc.get(key) is not None: return doc.get(key)
        if course_details.get(key) is not None: return course_details.get(key)
    return None

def _find_value_across_docs(docs: list, key_priority_list: list):
    for key in key_priority_list:
        for doc in docs:
            value = doc.get(key)
            if value is not None and value != '':
                return value
    return None

def _extract_apm_command_value(doc: Dict) -> str:
    try:
        apm_commands = doc.get("apm_commands")
        if not apm_commands or not isinstance(apm_commands, list) or not apm_commands[0]:
            return ""
        
        command_string = apm_commands[0]
        if not isinstance(command_string, str):
            return ""

        quoted_parts = re.findall(r'"(.*?)"', command_string)
        
        if len(quoted_parts) >= 4:
            return quoted_parts[-4]
        else:
            logger.warning(f"APM command string has fewer than 4 quoted parts: {command_string}")
            return ""
            
    except (IndexError, TypeError) as e:
        logger.debug(f"Could not extract APM command value from doc: {e}")
        return ""
    return ""

def _find_apm_command_across_docs(docs: list) -> str:
    for doc in docs:
        value = _extract_apm_command_value(doc)
        if value:
            return value
    return ""

# ==============================================================================
# CREDENTIAL HANDLING
# ==============================================================================
def fetch_apm_credentials():
    try:
        url = "http://connect:1212/list"
        response = requests.get(url, timeout=70)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch APM credentials: {e}")
        return {}

def build_apm_lookup(apm_data):
    lookup_by_code = {}
    if not isinstance(apm_data, dict):
        logger.warning("APM credential data is not valid. Cannot build lookup.")
        return {}
    for username, entry in apm_data.items():
        course_code = entry.get("vpn_auth_course_code")
        if course_code and course_code.lower() != "trainer":
            lookup_by_code[course_code] = {
                "username": username,
                "password": entry.get("vpn_auth_class")
            }
    return lookup_by_code

# ==============================================================================
# DATA UNPACKING FUNCTIONS
# ==============================================================================
def unpack_interim_allocations(documents, vendor_map, location_map, apm_lookup_by_code, ram_lookup_map, host_vcenter_map):
    logger.info("Starting unpack_interim_allocations process...")
    trainer_data_map = {}
    standard_docs_raw = []
    trainer_docs_raw = []
    
    for doc in documents:
        course_code_value = doc.get('sf_course_code')
        if course_code_value and '-trainer pod' in str(course_code_value).lower():
            trainer_docs_raw.append(doc)
        else:
            standard_docs_raw.append(doc)
    logger.info(f"Separated documents: {len(standard_docs_raw)} standard, {len(trainer_docs_raw)} trainer.")

    processed_trainer_pods = []
    for doc in trainer_docs_raw:
        related_courses = doc.get('related_student_courses', [])
        grouping_course_code = related_courses[0] if related_courses else doc.get('sf_course_code')
        shared_ram = doc.get('memory_gb_one_pod')
        if not shared_ram:
            course_version = doc.get('final_labbuild_course')
            if course_version in ram_lookup_map:
                shared_ram = ram_lookup_map.get(course_version)
        shared_username = doc.get('student_apm_username') or doc.get('apm_username')
        shared_password = doc.get('student_apm_password') or doc.get('apm_password')
        trainer_hosts = sorted(set(a.get('host') for a in doc.get('assignments', []) if a.get('host')))
        shared_host_str = ", ".join(trainer_hosts)
        trainer_vcenters = sorted(list(set(host_vcenter_map.get(h.lower(), '') for h in trainer_hosts if h)))
        shared_vcenter_str = ", ".join(filter(None, trainer_vcenters))
        if shared_host_str and not shared_vcenter_str:
            logger.warning(f"vCenter lookup failed for trainer pod '{doc.get('sf_course_code')}' with hosts '{shared_host_str}'.")
        if not related_courses:
             logger.warning(f"Trainer pod doc {doc.get('sf_course_code')} has no related_student_courses. Cannot link data reliably.")
        for course_code in related_courses:
            trainer_data_map[course_code] = { 'username': shared_username, 'password': shared_password, 'ram': shared_ram, 'virtual_hosts': shared_host_str }
        pod_ranges = [f"{a.get('start_pod')}-{a.get('end_pod')}" for a in doc.get('assignments', []) if a.get('start_pod')]
        us_au_loc = determine_us_au_location(shared_host_str)
        trainer_pod_entry = {
            'course_code': grouping_course_code, 'location': "", 'us_au_location': us_au_loc, 'course_start_date': "", 'last_day': "",
            'trainer_name': "", 'course_name': doc.get('sf_course_type', ''), 'start_end_pod': ", ".join(pod_ranges),
            'username': shared_username, 'password': shared_password, 'class_number': "", 'students': len(related_courses),
            'vendor_pods': doc.get('effective_pods_req', len(pod_ranges) or 0), 'ram': shared_ram, 'virtual_hosts': shared_host_str,
            'vcenter': shared_vcenter_str, 'pod_type': 'trainer', 'version': doc.get('final_labbuild_course'), 'course_version': doc.get('final_labbuild_course'),
            'apm_command_value': _extract_apm_command_value(doc),
        }
        processed_trainer_pods.append(trainer_pod_entry)
    logger.info(f"Built trainer_data_map for {len(trainer_data_map)} student courses.")

    grouped_courses = defaultdict(lambda: {'docs': [], 'assignments': []})
    for doc in standard_docs_raw:
        if code := doc.get('sf_course_code'):
            grouped_courses[code]['docs'].append(doc)
            grouped_courses[code]['assignments'].extend(doc.get('assignments', []))
    processed_standard_courses = []
    logger.info(f"Processing {len(grouped_courses)} unique standard course codes.")
    for code, data in grouped_courses.items():
        base_doc = data['docs'][0]
        info_from_trainer = trainer_data_map.get(code, {})
        apm_creds = apm_lookup_by_code.get(code, {})
        final_username = info_from_trainer.get('username') or apm_creds.get('username')
        final_password = info_from_trainer.get('password') or apm_creds.get('password')
        final_ram = info_from_trainer.get('ram') or _find_value_across_docs(data['docs'], ['memory_gb_one_pod', 'ram'])
        if not final_ram:
            course_version = base_doc.get('final_labbuild_course')
            if course_version in ram_lookup_map:
                final_ram = ram_lookup_map.get(course_version)
        
        # --- START OF VENDOR-SPECIFIC FIX ---
        # 1. Start with the host string from a linked trainer pod, if available.
        final_host_str = info_from_trainer.get('virtual_hosts')

        # 2. Determine if the course is a "Check Point" course.
        course_type = base_doc.get('sf_course_type', '').lower()
        is_cp_course = 'check point' in course_type

        # 3. For CP courses, ALWAYS overwrite the host string with data from assignments,
        #    as the trainer data can be unreliable. For other courses, only use assignments
        #    if the trainer data was missing.
        if is_cp_course or not final_host_str:
            hosts_from_assignments = sorted(set(a.get('host') for a in data['assignments'] if a.get('host')))
            if hosts_from_assignments:
                final_host_str = ", ".join(hosts_from_assignments)

        # 4. As a final fallback for any course, search for a 'virtual_hosts' field
        #    on the course documents themselves if no host string has been found yet.
        if not final_host_str:
            final_host_str = _find_value_across_docs(data['docs'], ['virtual_hosts']) or ""
        # --- END OF VENDOR-SPECIFIC FIX ---

        final_hosts_list = [h.strip() for h in final_host_str.split(',') if h.strip()]
        vcenters = sorted(list(set(host_vcenter_map.get(h.lower(), '') for h in final_hosts_list if h)))
        final_vcenter_str = ", ".join(filter(None, vcenters))
        
        if final_host_str and not final_vcenter_str:
            logger.warning(
                f"VCENTER LOOKUP FAILED for course '{code}'. "
                f"Hosts found: '{final_host_str}'. "
                f"Failed to find a vCenter for these hosts in the map. "
                f"Please check if these host identifiers exist in the 'hosts' collection."
            )
            
        pod_ranges = [f"{a.get('start_pod')}-{a.get('end_pod')}" for a in data['assignments'] if a.get('start_pod')]
        us_au_loc = determine_us_au_location(final_host_str)
        if not us_au_loc and code:
            code_lower = code.lower()
            if code_lower.startswith('au-') or '-au-' in code_lower: us_au_loc = "AU"
            elif code_lower.startswith('us-') or '-us-' in code_lower: us_au_loc = "US"
        course = {
            'course_code': code, 'location': find_location_from_code(code, location_map), 'us_au_location': us_au_loc,
            'course_start_date': format_date(base_doc.get('sf_start_date')), 'last_day': format_date(base_doc.get('sf_end_date')),
            'trainer_name': base_doc.get('sf_trainer_name'), 'course_name': base_doc.get('sf_course_type'), 'start_end_pod': ", ".join(pod_ranges),
            'username': final_username, 'password': final_password, 'class_number': base_doc.get('f5_class_number'), 'students': base_doc.get('sf_pax_count', 0),
            'vendor_pods': base_doc.get('effective_pods_req', len(pod_ranges) or 0), 'ram': final_ram, 'virtual_hosts': final_host_str,
            'vcenter': final_vcenter_str, 'pod_type': 'default', 'version': base_doc.get('final_labbuild_course'), 'course_version': base_doc.get('final_labbuild_course'),
            'apm_command_value': _find_apm_command_across_docs(data['docs']),
        }
        processed_standard_courses.append(course)
    logger.info(f"Finished processing. Returning {len(processed_standard_courses)} standard courses and {len(processed_trainer_pods)} trainer pods.")
    return processed_standard_courses, processed_trainer_pods

def unpack_extended_allocations(documents: List[Dict], location_map: Dict, ram_lookup_map: Dict, host_vcenter_map: Dict) -> List[Dict]:
    extended_courses = []
    logger.info(f"Unpacking {len(documents)} extended allocations from currentallocation.")
    for doc in documents:
        course_details = doc.get('courses', [{}])[0]
        if not course_details:
            logger.warning(f"Skipping extended allocation doc with _id {doc.get('_id')} due to missing 'courses' data.")
            continue
        pod_details_list = doc.get('pod_details') or course_details.get('pod_details', [])
        pod_numbers = sorted([p.get('pod_number') for p in pod_details_list if p.get('pod_number') is not None])
        hosts = sorted(list(set(p.get('host') for p in pod_details_list if p.get('host'))))
        vcenters = sorted(list(set(host_vcenter_map.get(h.lower(), '') for h in hosts if h)))
        final_vcenter_str = ", ".join(filter(None, vcenters))
        if hosts and not final_vcenter_str:
            logger.warning(f"vCenter lookup failed for extended course '{doc.get('tag', '')}' with hosts '{', '.join(hosts)}'.")
        start_end_pod = ""
        if pod_numbers:
            start_end_pod = f"{pod_numbers[0]}-{pod_numbers[-1]}" if len(pod_numbers) > 1 else str(pod_numbers[0])
        ram_value = course_details.get('memory_gb_one_pod') or course_details.get('ram')
        if not ram_value:
            course_version = course_details.get('course_name')
            if course_version in ram_lookup_map:
                ram_value = ram_lookup_map.get(course_version)
        course_code = doc.get('tag', '')
        course = {
            'course_code': course_code, 'location': find_location_from_code(course_code, location_map), 'us_au_location': determine_us_au_location(", ".join(hosts)),
            'course_start_date': format_date(course_details.get('start_date')), 'last_day': format_date(course_details.get('end_date')),
            'trainer_name': course_details.get('trainer_name'), 'course_name': course_details.get('course_name'), 'start_end_pod': start_end_pod,
            'username': course_details.get('apm_username'), 'password': course_details.get('apm_password'), 'class_number': None, 'students': len(pod_numbers),
            'vendor_pods': len(pod_numbers), 'ram': ram_value, 'virtual_hosts': ", ".join(hosts), 'vcenter': final_vcenter_str,
            'pod_type': 'extended', 'version': course_details.get('course_name'), 'course_version': course_details.get('course_name'),
            'apm_command_value': '',
        }
        extended_courses.append(course)
    return extended_courses

# ==============================================================================
# EXCEL GENERATION & MAIN ORCHESTRATION
# ==============================================================================
def generate_excel_in_memory(course_allocations: List[Dict], trainer_pods: List[Dict], extended_pods: List[Dict], host_map: Dict) -> io.BytesIO:
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Upcoming Labs"
    
    all_data = course_allocations + trainer_pods + extended_pods
    
    grouped = {g: [] for g in ExcelStyle.DEFAULT_COURSE_GROUPS.value}
    for entry in all_data:
        for gname, fn in ExcelStyle.DEFAULT_COURSE_GROUPS.value.items():
            if fn(entry.get("course_code", "")):
                grouped[gname].append(entry)
                break
                
    group_order, current_row = EXCEL_GROUP_ORDER, 12
    for group_name in group_order:
        if not (records := grouped.get(group_name, [])): continue
        
        current_row += 1 
        
        group_start_row = current_row
        
        logger.info(f"Writing group: {group_name} with {len(records)} records")
        current_row = write_group_title(sheet, current_row, group_name)
        
        headers = ExcelStyle.DEFAULT_HEADER_COURSE_MAPPING.value[group_name].copy()
        
        # For ANY group that has a "Course Version" column, replace it with a "vCenter" column.
        if "Course Version" in headers:
            new_headers = {}
            for header, data_key in headers.items():
                if header == "Course Version":
                    new_headers["vCenter"] = "vcenter"
                else:
                    new_headers[header] = data_key
            headers = new_headers
        
        if "Group" in headers:
            headers["Group"] = "apm_command_value"

        header_keys, col_idx, header_pos, group_end_col = list(headers), 1, {}, 0
        for h in header_keys:
            header_pos[h], width = col_idx, 3 if h == "Start/End Pod" else 1
            col_idx += width
            group_end_col += width
        col = 1
        for h in header_keys:
            if h == "Start/End Pod": write_merged_header(sheet, current_row, col, h); col += 3
            else:
                header_text = "APM Commands" if h == "Group" else h
                cell = sheet.cell(row=current_row, column=col, value=header_text)
                cell.font = Font(bold=True)
                apply_style(cell, is_summary=True)
                col += 1
        current_row += 1
        data_start_row = current_row
        group_host_ram_totals = {k: 0 for k in host_map}
        non_trainer_pods = [e for e in records if e.get("pod_type") != "trainer"]
        trainer_pods_in_group = [e for e in records if e.get("pod_type") == "trainer"]
        
        for entry in non_trainer_pods:
            _write_data_row(sheet, current_row, entry, headers, header_keys, header_pos, False, entry.get("pod_type") == "extended", host_map)
            if (entry_ram := convert_to_numeric(entry.get("ram")) or 0) > 0:
                for host_key, host_name in host_map.items():
                    if host_name.lower() in (entry.get("virtual_hosts", "") or "").lower():
                        total_pods_for_entry = convert_to_numeric(entry.get("vendor_pods")) or 1
                        if total_pods_for_entry <= 1: group_host_ram_totals[host_key] += entry_ram
                        else: group_host_ram_totals[host_key] += entry_ram + (total_pods_for_entry - 1) * entry_ram / 2
            current_row += 1
        if non_trainer_pods and trainer_pods_in_group:
            for c in range(1, group_end_col + 1): sheet.cell(row=current_row, column=c).border = ExcelStyle.THIN_BORDER.value
            current_row += 1
        for entry in trainer_pods_in_group:
            _write_data_row(sheet, current_row, entry, headers, header_keys, header_pos, True, False, host_map)
            if (entry_ram := convert_to_numeric(entry.get("ram")) or 0) > 0:
                for host_key, host_name in host_map.items():
                    if host_name.lower() in (entry.get("virtual_hosts", "") or "").lower():
                        total_pods_for_entry = convert_to_numeric(entry.get("vendor_pods")) or 1
                        if total_pods_for_entry <= 1: group_host_ram_totals[host_key] += entry_ram
                        else: group_host_ram_totals[host_key] += entry_ram + (total_pods_for_entry - 1) * entry_ram / 2
            current_row += 1
        data_end_row = current_row - 1
        
        pod_total_formula = 0
        vendor_pods_col_num = header_pos.get("Vendor Pods")
        if vendor_pods_col_num and data_start_row <= data_end_row:
            vendor_pods_col_letter = get_column_letter(vendor_pods_col_num)
            pod_total_formula = f"=SUM({vendor_pods_col_letter}{data_start_row}:{vendor_pods_col_letter}{data_end_row})"

        summary_start_row = current_row
        current_row = write_group_summary_boxes(sheet, summary_start_row, header_pos, pod_total_formula, group_host_ram_totals, group_name, group_end_col, data_start_row, data_end_row)
        end_row = current_row - 1 if summary_start_row < current_row else summary_start_row - 1
        apply_outer_border(sheet, group_start_row, end_row, 1, group_end_col)
        current_row += 1
        
    write_summary_section(sheet, 2)
    
    for col_letter, width in EXCEL_COLUMN_WIDTHS.items():
        sheet.column_dimensions[col_letter].width = width
    in_memory_fp = io.BytesIO()
    wb.save(in_memory_fp)
    in_memory_fp.seek(0)
    return in_memory_fp

def get_upcoming_report_data(db):
    try:
        if db is None: raise ConnectionError("A valid database connection was not provided.")
        
        logger.info("Fetching data for upcoming report from INTERIM_ALLOCATION_COLLECTION...")
        interim_docs = list(db[INTERIM_ALLOCATION_COLLECTION].find({}))
        
        logger.info("Fetching extended courses from CURRENT_ALLOCATION_COLLECTION...")
        extended_docs = list(db[ALLOCATION_COLLECTION].find({"extend": "true"}))
        logger.info(f"Found {len(extended_docs)} allocations marked for extension.")

        logger.info("Fetching course configs for RAM fallback...")
        course_configs = list(db[COURSE_CONFIG_COLLECTION].find({}, {"course_name": 1, "memory": 1}))
        ram_lookup_map = {
            doc['course_name']: doc['memory']
            for doc in course_configs if 'course_name' in doc and 'memory' in doc
        }
        logger.info(f"Built RAM lookup map with {len(ram_lookup_map)} entries.")

        locations_data = list(db["locations"].find({}))
        host_docs = list(db[HOST_COLLECTION].find({}))

        host_map_for_summary = {doc['host_shortcode'].capitalize(): doc['host_shortcode'] for doc in host_docs if 'host_shortcode' in doc and str(doc.get('include_for_build')).lower() == 'true'}
        location_map = {loc['code']: loc['name'] for loc in locations_data if 'code' in loc and 'name' in loc}
        
        host_vcenter_map = {}
        logger.info("Building robust host-to-vCenter lookup map...")
        for doc in host_docs:
            vcenter_fqdn = doc.get('vcenter') or doc.get('vcentre')
            if not (vcenter_fqdn and isinstance(vcenter_fqdn, str)):
                continue
            short_vcenter = vcenter_fqdn.split('.')[0]
            for key in ['host_name', 'fqdn', 'host_shortcode']:
                identifier = doc.get(key)
                if identifier and isinstance(identifier, str):
                    host_vcenter_map[identifier.lower()] = short_vcenter
        
        logger.info(f"Built vCenter map with {len(host_vcenter_map)} keys. Keys are: {list(host_vcenter_map.keys())}")
        if not host_vcenter_map:
            logger.warning("The host-to-vCenter map is EMPTY. The 'vCenter' column in the report will be blank.")
            
        vendor_map = get_vendor_prefix_map()
        apm_data = fetch_apm_credentials()
        apm_lookup_by_code = build_apm_lookup(apm_data)
        
        course_allocs, trainer_pods = unpack_interim_allocations(interim_docs, vendor_map, location_map, apm_lookup_by_code, ram_lookup_map, host_vcenter_map)
        extended_pods = unpack_extended_allocations(extended_docs, location_map, ram_lookup_map, host_vcenter_map)
        
        logger.info(
            f"Processed for Upcoming Report: {len(course_allocs)} Standard Courses | "
            f"{len(trainer_pods)} Trainer Pods | {len(extended_pods)} Extended Pods"
        )
        return course_allocs, trainer_pods, extended_pods, host_map_for_summary

    except Exception as e:
        logger.error(f"FATAL: An error occurred during upcoming report data fetching: {e}", exc_info=True)
        raise e