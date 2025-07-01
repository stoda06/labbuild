# labbuild/dashboard/report_generator.py

import io
import logging
import requests
import pymongo
from flask import current_app
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any, Optional

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# --- UPDATED IMPORTS ---
# Import the new configuration constants alongside the existing ones.
from constants import (
    ExcelStyle, LOG_LEVEL_GENERATE_EXCEL,
    AU_HOST_NAMES, US_HOST_NAMES,
    INTERIM_ALLOCATION_COLLECTION, ALLOCATION_COLLECTION, HOST_COLLECTION,
    AVAILABLE_RAM_GB, SUMMARY_ENV_ORDER,
    
    # --- NEWLY ADDED CONSTANTS FOR BETTER MAINTAINABILITY ---
    RAM_SUMMARY_START_COL, EXCEL_GROUP_ORDER, EXCEL_COLUMN_WIDTHS
)

# --- Logging Setup ---
logger = logging.getLogger(__name__)

# ==============================================================================
# ALL THE HELPER AND DATA PROCESSING FUNCTIONS
# ==============================================================================

def calculate_ram_summary(all_allocations: List[Dict], host_map: Dict) -> Dict[str, float]:
    """
    Calculates the total allocated RAM for each environment based on matching host prefixes.
    """
    allocated_ram_by_env = {env_key: 0 for env_key in host_map.keys()}

    for allocation in all_allocations:
        ram = convert_to_numeric(allocation.get("ram"))
        if not ram or ram <= 0:
            continue

        virtual_hosts_str = allocation.get("virtual_hosts", "").lower()
        if not virtual_hosts_str:
            continue

        # Check which environment this allocation belongs to
        for env_key, host_prefix in host_map.items():
            if host_prefix.lower() in virtual_hosts_str:
                # Add the RAM to the corresponding environment's total
                allocated_ram_by_env[env_key] += ram
    
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
    if trainer:
        cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value
    elif use_green_fill:
        cell.fill = ExcelStyle.GREEN_FILL.value
    elif is_summary:
        cell.fill = ExcelStyle.LIGHT_BLUE_FILL.value

def write_cell(sheet, row, col, value, trainer=False, use_green_fill=False, number_format=None, is_summary=False):
    cell = sheet.cell(row=row, column=col)
    cell.value = convert_to_numeric(value) if not (isinstance(value, str) and value.startswith("=")) else value
    apply_style(cell, trainer, use_green_fill, is_summary)
    if number_format:
        cell.number_format = number_format

def write_group_title(sheet, row, title):
    sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=34)
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

def write_summary_section(sheet, row_offset, allocated_ram_by_env: Dict[str, float]):
    """
    Writes the dynamic RAM summary section to the Excel sheet using configuration from constants.
    """
    start_col = RAM_SUMMARY_START_COL
    env_keys = SUMMARY_ENV_ORDER
    headers = ["RAM Summary", "Total", *env_keys]

    for i, header in enumerate(headers):
        col = start_col + i
        cell = sheet.cell(row=row_offset, column=col, value=header)
        cell.font = Font(bold=True)
        apply_style(cell, is_summary=True)

    available_ram_row = row_offset + 1
    allocated_ram_row = row_offset + 2
    allocated_pct_row = row_offset + 3
    remaining_ram_row = row_offset + 4

    sheet.cell(row=available_ram_row, column=start_col, value="Available RAM (GB)").font = Font(bold=True)
    sheet.cell(row=allocated_ram_row, column=start_col, value="Allocated RAM (GB)").font = Font(bold=True)
    sheet.cell(row=allocated_pct_row, column=start_col, value="Allocated RAM (%)").font = Font(bold=True)
    sheet.cell(row=remaining_ram_row, column=start_col, value="Remaining RAM (GB)").font = Font(bold=True)

    total_col_letter = get_column_letter(start_col + 1)
    
    for i, env_key in enumerate(env_keys):
        col = start_col + 2 + i
        col_letter = get_column_letter(col)
        available_ram = AVAILABLE_RAM_GB.get(env_key, 0)
        write_cell(sheet, available_ram_row, col, available_ram, is_summary=False, number_format='0')
        allocated_ram = allocated_ram_by_env.get(env_key, 0)
        write_cell(sheet, allocated_ram_row, col, allocated_ram, is_summary=False, number_format='0.0')
        formula_pct = f"=IF({col_letter}{available_ram_row}>0, {col_letter}{allocated_ram_row}/{col_letter}{available_ram_row}, 0)"
        write_cell(sheet, allocated_pct_row, col, formula_pct, is_summary=False, number_format="0%")
        formula_rem = f"={col_letter}{available_ram_row}-{col_letter}{allocated_ram_row}"
        write_cell(sheet, remaining_ram_row, col, formula_rem, is_summary=False, number_format='0.0')

    first_env_col_letter = get_column_letter(start_col + 2)
    last_env_col_letter = get_column_letter(start_col + 1 + len(env_keys))
    
    write_cell(sheet, available_ram_row, start_col + 1, f"=SUM({first_env_col_letter}{available_ram_row}:{last_env_col_letter}{available_ram_row})", number_format='0')
    write_cell(sheet, allocated_ram_row, start_col + 1, f"=SUM({first_env_col_letter}{allocated_ram_row}:{last_env_col_letter}{allocated_ram_row})", number_format='0.0')
    write_cell(sheet, allocated_pct_row, start_col + 1, f"=IF({total_col_letter}{available_ram_row}>0, {total_col_letter}{allocated_ram_row}/{total_col_letter}{available_ram_row}, 0)", number_format="0%")
    write_cell(sheet, remaining_ram_row, start_col + 1, f"={total_col_letter}{available_ram_row}-{total_col_letter}{allocated_ram_row}", number_format='0.0')
    
    for r in [allocated_ram_row, allocated_pct_row, remaining_ram_row]:
        for c in range(start_col + 1, start_col + 2 + len(env_keys)):
            cell = sheet.cell(row=r, column=c)
            cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
            cell.font = Font(bold=True, color="FF0000")

def _write_data_row(sheet, row, entry, headers, header_keys, header_pos, is_trainer, use_green_fill, host_map: Dict):
    trainer_flag, green_flag = (True, False) if is_trainer else (False, use_green_fill)
    virtual_hosts_str = entry.get("virtual_hosts", "").lower()
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
            if host_name_for_this_column in virtual_hosts_str:
                ram_col_letter, pods_col_letter = get_column_letter(header_pos["RAM"]), get_column_letter(header_pos["Vendor Pods"])
                formula = f"=IF({pods_col_letter}{row}<=1, {ram_col_letter}{row}, {ram_col_letter}{row} + ({pods_col_letter}{row}-1)*{ram_col_letter}{row}/2)"
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

def write_group_summary_boxes(sheet, start_row, header_pos, group_pod_total, group_host_ram_totals, group_name):
    vendor_col, ram_col = header_pos.get("Vendor Pods"), header_pos.get("RAM")
    if not vendor_col and not ram_col: return start_row
    label_row, value_row = start_row, start_row + 1
    if vendor_col and group_pod_total > 0:
        sheet.merge_cells(start_row=label_row, start_column=vendor_col - 1, end_row=label_row, end_column=vendor_col + 1)
        label_cell = sheet.cell(row=label_row, column=vendor_col - 1, value="Total Pods")
        label_cell.font = Font(bold=True, size=14)
        apply_style(cell=label_cell, is_summary=True)
        for offset in [-1, 0, 1]:
            cell = sheet.cell(row=value_row, column=vendor_col + offset, value=group_pod_total if offset == 0 else "")
            cell.font = Font(bold=True, size=12)
            apply_style(cell=cell, is_summary=True)
    if ram_col and sum(group_host_ram_totals.values()) > 0:
        # Correctly iterates over the keys from the dynamically calculated totals
        for host_key in group_host_ram_totals.keys():
            host_col = header_pos.get(host_key)
            if not host_col: continue
            host_ram = group_host_ram_totals.get(host_key, 0)
            cell = sheet.cell(row=value_row, column=host_col)
            cell.value = host_ram if host_ram > 0 else 0
            cell.number_format = '0'
            apply_style(cell, is_summary=True)
            cell.font = Font(bold=True, color="FFFF00")
    return start_row + 3

def apply_outer_border(sheet, start_row, end_row, start_col, end_col):
    medium_border = ExcelStyle.MEDIUM_OUTER_BORDER.value
    for col in range(start_col, end_col + 1):
        top_cell = sheet.cell(row=start_row, column=col)
        top_cell.border = Border(top=medium_border.top, left=top_cell.border.left, right=top_cell.border.right, bottom=top_cell.border.bottom)
        bottom_cell = sheet.cell(row=end_row, column=col)
        bottom_cell.border = Border(top=bottom_cell.border.top, left=bottom_cell.border.left, right=bottom_cell.border.right, bottom=medium_border.bottom)
    for row in range(start_row, end_row + 1):
        left_cell = sheet.cell(row=row, column=start_col)
        left_cell.border = Border(top=left_cell.border.top, left=medium_border.left, right=left_cell.border.right, bottom=left_cell.border.bottom)
        right_cell = sheet.cell(row=row, column=end_col)
        right_cell.border = Border(top=right_cell.border.top, left=right_cell.border.left, right=medium_border.right, bottom=right_cell.border.bottom)

def determine_us_au_location(virtual_hosts_str: str) -> str:
    if not virtual_hosts_str: return ""
    hosts_lower = virtual_hosts_str.lower()
    if any(au_host in hosts_lower for au_host in AU_HOST_NAMES): return "AU"
    if any(us_host in hosts_lower for us_host in US_HOST_NAMES): return "US"
    return ""

def format_date(date_str):
    if not date_str: return ""
    try: return datetime.strptime(date_str, '%Y-%m-%d').strftime('%A, %d/%m')
    except Exception: return date_str

def fetch_apm_credentials():
    try:
        url = "http://connect:1212/list?us=true"
        response = requests.get(url, timeout=70)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch APM credentials: {e}")
        return []

def build_apm_lookup(apm_data):
    lookup = {}
    for username, entry in apm_data.items():
        course_code = entry.get("vpn_auth_course_code")
        if not course_code: continue
        trainer_name = entry.get("vpn_auth_courses", "").split(" - ")[0].strip()
        lookup[course_code] = {"username": username, "password": entry.get("vpn_auth_class"), "trainer_name": trainer_name}
    return lookup

def get_vendor_prefix_map():
    return {gn.split(' ')[0].lower(): gn for gn in ExcelStyle.DEFAULT_COURSE_GROUPS.value.keys()}

def find_location_from_code(course_code: str, location_map: dict) -> str:
    if not course_code: return ""
    for loc_code in sorted(location_map.keys(), key=len, reverse=True):
        if loc_code in course_code: return location_map[loc_code]
    return ""

def unpack_interim_allocations(documents, vendor_map, location_map):
    grouped_courses = defaultdict(lambda: {'assignments': [], 'trainer_assignments': [], 'docs': []})
    for doc in documents:
        code = doc.get('sf_course_code')
        if code:
            grouped_courses[code]['docs'].append(doc)
            if doc.get('assignments'): grouped_courses[code]['assignments'].extend(doc['assignments'])
            if isinstance(doc.get('trainer_assignment'), list): grouped_courses[code]['trainer_assignments'].extend(doc['trainer_assignment'])

    standard_courses, trainer_pods = [], []
    for code, data in grouped_courses.items():
        base_doc = next((d for d in data['docs'] if d.get('assignments')), data['docs'][0])
        student_virtual_hosts_str, resolved_location = "", find_location_from_code(code, location_map)
        if data['assignments']:
            meta_doc = next((d for d in data['docs'] if d.get('memory_gb_one_pod') is not None), base_doc)
            pod_ranges = [f"{a.get('start_pod')}-{a.get('end_pod')}" for a in data['assignments'] if a.get('start_pod')]
            hosts = sorted(set(a.get('host') for a in data['assignments'] if a.get('host')))
            student_virtual_hosts_str = ", ".join(hosts)
            
            found_version = base_doc.get('final_labbuild_course')
            
            course = {
                'course_code': code, 'us_au_location': determine_us_au_location(student_virtual_hosts_str),
                'course_start_date': format_date(base_doc.get('sf_start_date')), 'last_day': format_date(base_doc.get('sf_end_date')),
                'location': resolved_location, 'trainer_name': base_doc.get('sf_trainer_name'),
                'course_name': base_doc.get('sf_course_type'), 'start_end_pod': ", ".join(pod_ranges),
                'username': base_doc.get('student_apm_username'), 'password': base_doc.get('student_apm_password'),
                'class_number': base_doc.get('f5_class_number'), 'students': base_doc.get('sf_pax_count', 0),
                'vendor_pods': base_doc.get('effective_pods_req', len(pod_ranges)),
                'ram': meta_doc.get('memory_gb_one_pod'), 'virtual_hosts': student_virtual_hosts_str, 'pod_type': 'default',
                'version': found_version,
                'course_version': found_version,
            }
            standard_courses.append(course)
            
        if data['trainer_assignments']:
            meta_doc = next((d for d in data['docs'] if d.get('trainer_memory_gb_one_pod') is not None), base_doc)
            pod_ranges = [f"{a.get('start_pod')}-{a.get('end_pod')}" for a in data['trainer_assignments'] if a.get('start_pod')]
            final_trainer_hosts = student_virtual_hosts_str or ", ".join(sorted(set(a.get('host') for a in data['trainer_assignments'] if a.get('host'))))
            
            found_trainer_version = meta_doc.get('trainer_labbuild_course')
            
            trainer = {
                'course_code': f"{code}-TP", 'us_au_location': determine_us_au_location(final_trainer_hosts),
                'course_name': meta_doc.get('trainer_labbuild_course') or base_doc.get('sf_course_type'), 'start_end_pod': ", ".join(pod_ranges),
                'pod_type': 'trainer', 'course_start_date': format_date(base_doc.get('sf_start_date')), 'last_day': format_date(base_doc.get('sf_end_date')),
                'location': resolved_location, 'trainer_name': base_doc.get('sf_trainer_name'),
                'username': meta_doc.get('trainer_apm_username') or base_doc.get('apm_username'), 'password': meta_doc.get('trainer_apm_password') or base_doc.get('apm_password'),
                'vendor_pods': len(pod_ranges),
                'ram': meta_doc.get('trainer_memory_gb_one_pod'), 'virtual_hosts': final_trainer_hosts,
                'version': found_trainer_version,
                'course_version': found_trainer_version,
            }
            trainer_pods.append(trainer)
            
    return standard_courses, trainer_pods

def _find_field(doc, keys):
    course_details = doc.get('courses', [{}])[0]
    for key in keys:
        if doc.get(key) is not None: return doc.get(key)
        if course_details.get(key) is not None: return course_details.get(key)
    return None

def unpack_current_allocations(documents, vendor_map, apm_credentials, location_map, host_to_vcenter_map):
    grouped = defaultdict(lambda: {'pod_numbers': set(), 'hosts': set(), 'doc': None})
    for doc in documents:
        if doc.get('extend') != 'true':
            continue
        key = doc.get('tag')
        if not key:
            continue
        if not grouped[key]['doc']:
            grouped[key]['doc'] = doc
        for c in doc.get('courses', []):
            for pd in c.get('pod_details', []):
                for p in pd.get("pods", [pd]):
                    if p.get("pod_number") is not None:
                        grouped[key]['pod_numbers'].add(int(p.get("pod_number")))
                    if p.get("host"):
                        grouped[key]['hosts'].add(p.get("host"))

    extended_pods = []
    for course_code, val in grouped.items():
        if not val['pod_numbers']:
            continue
        
        doc = val['doc']
        creds = apm_credentials.get(course_code, {})
        
        username = _find_field(doc, ['apm_username', 'student_apm_username']) or creds.get('username')
        password = _find_field(doc, ['apm_password', 'student_apm_password']) or creds.get('password')
        trainer_name = _find_field(doc, ['trainer_name', 'sf_trainer_name']) or creds.get('trainer_name')
        
        pod_nums = sorted(val['pod_numbers'])
        pod_count = len(pod_nums)
        pod_range = f"{pod_nums[0]}-{pod_nums[-1]}" if pod_count > 1 else str(pod_nums[0])
        
        course_hosts = sorted(val['hosts'])
        vcenters = {host_to_vcenter_map.get(host) for host in course_hosts if host_to_vcenter_map.get(host)}
        vcenter_str = ", ".join(sorted(vcenters))
        
        found_version = _find_field(doc, ['course_name', 'version']) or 'N/A'
        
        pod = {
            'course_code': course_code,
            'us_au_location': determine_us_au_location(", ".join(course_hosts)),
            'pod_type': 'extended',
            'start_end_pod': pod_range,
            'location': find_location_from_code(course_code, location_map),
            'virtual_hosts': ", ".join(course_hosts),
            'course_start_date': format_date(_find_field(doc, ['start_date', 'sf_start_date'])),
            'last_day': format_date(_find_field(doc, ['end_date', 'sf_end_date'])),
            'username': username,
            'password': password,
            'trainer_name': trainer_name,
            'ram': _find_field(doc, ['ram', 'memory_gb_one_pod']),
            'vendor_pods': pod_count,
            'students': pod_count,
            'vcenter_name': vcenter_str,
            'version': found_version,
            'course_version': found_version,
        }
        extended_pods.append(pod)
        
    return extended_pods

def generate_excel_in_memory(course_allocations: List[Dict], trainer_pods: List[Dict], extended_pods: List[Dict], host_map: Dict) -> io.BytesIO:
    """
    Main function to generate the entire Excel file.
    Uses configuration from constants for layout and ordering.
    """
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Labbuild"

    all_data = course_allocations + trainer_pods + extended_pods
    allocated_ram_data = calculate_ram_summary(all_data, host_map)

    expanded_data = []
    for entry in all_data:
        hosts = entry.get("virtual_hosts", "")
        host_list = [h.strip() for h in hosts.split(",") if h.strip()]
        if len(host_list) <= 1:
            expanded_data.append(entry)
        else:
            for host in host_list:
                new_entry = entry.copy()
                new_entry["virtual_hosts"] = host
                expanded_data.append(new_entry)
    all_data = expanded_data

    grouped = {g: [] for g in ExcelStyle.DEFAULT_COURSE_GROUPS.value}
    for entry in all_data:
        if "pod_type" not in entry: entry["pod_type"] = "default"
        for gname, fn in ExcelStyle.DEFAULT_COURSE_GROUPS.value.items():
            if fn(entry.get("course_code", "")):
                grouped[gname].append(entry)
                break

    # Uses the new constant for group order
    group_order = EXCEL_GROUP_ORDER
    current_row = 12
    for group_name in group_order:
        records = grouped.get(group_name, [])
        if not records: continue

        group_start_row = current_row
        logger.info(f"Writing group: {group_name} with {len(records)} records")
        current_row = write_group_title(sheet, current_row, group_name)
        headers = ExcelStyle.DEFAULT_HEADER_COURSE_MAPPING.value[group_name]
        header_keys, col_idx, header_pos, group_end_col = list(headers), 1, {}, 0
        for h in header_keys:
            header_pos[h] = col_idx
            width = 3 if h == "Start/End Pod" else 1
            col_idx += width
            group_end_col += width

        col = 1
        for h in header_keys:
            if h == "Start/End Pod":
                write_merged_header(sheet, current_row, col, h); col += 3
            else:
                cell = sheet.cell(row=current_row, column=col, value=h)
                cell.font = Font(bold=True)
                apply_style(cell, is_summary=True); col += 1
        current_row += 1
        
        group_host_ram_totals, group_pod_total = {k: 0 for k in host_map}, 0
        non_trainer_pods = [e for e in records if e.get("pod_type") != "trainer"]
        trainer_pods_in_group = [e for e in records if e.get("pod_type") == "trainer"]

        for entry in non_trainer_pods:
            is_extended = entry.get("pod_type") == "extended"
            _write_data_row(sheet, current_row, entry, headers, header_keys, header_pos, False, is_extended, host_map)
            group_pod_total += convert_to_numeric(entry.get("vendor_pods")) or 0
            entry_ram = convert_to_numeric(entry.get("ram")) or 0
            if entry_ram > 0:
                for host_key, host_name in host_map.items():
                    if host_name.lower() in entry.get("virtual_hosts", "").lower():
                        group_host_ram_totals[host_key] += entry_ram
            current_row += 1

        if non_trainer_pods and trainer_pods_in_group: current_row += 1

        for entry in trainer_pods_in_group:
            _write_data_row(sheet, current_row, entry, headers, header_keys, header_pos, True, False, host_map)
            group_pod_total += convert_to_numeric(entry.get("vendor_pods")) or 0
            entry_ram = convert_to_numeric(entry.get("ram")) or 0
            if entry_ram > 0:
                for host_key, host_name in host_map.items():
                    if host_name.lower() in entry.get("virtual_hosts", "").lower():
                        group_host_ram_totals[host_key] += entry_ram
            current_row += 1

        current_row = write_group_summary_boxes(sheet, current_row, header_pos, group_pod_total, group_host_ram_totals, group_name)
        apply_outer_border(sheet, group_start_row, current_row - 1, 1, group_end_col)

    write_summary_section(sheet, 2, allocated_ram_data)
    
    # Uses the new constant for column widths
    for col_letter, width in EXCEL_COLUMN_WIDTHS.items():
        sheet.column_dimensions[col_letter].width = width

    in_memory_fp = io.BytesIO()
    wb.save(in_memory_fp)
    in_memory_fp.seek(0)
    return in_memory_fp

def get_full_report_data(db):
    """
    Main data orchestrator. Fetches all data and builds lookup maps
    by filtering hosts based on the 'include_for_build' flag.
    """
    try:
        if db is None:
            raise ConnectionError("A valid database connection was not provided.")

        logger.info("Fetching data using provided DB connection...")

        interim_docs = list(db[INTERIM_ALLOCATION_COLLECTION].find({}))
        current_docs = list(db[ALLOCATION_COLLECTION].find({}))
        locations_data = list(db["locations"].find({}))
        host_docs = list(db[HOST_COLLECTION].find({}))

        # Dynamically build the map, filtering by 'include_for_build'
        host_map_for_summary = {
            doc['host_shortcode'].capitalize(): doc['host_shortcode']
            for doc in host_docs if 'host_shortcode' in doc and doc.get('include_for_build') == 'true'
        }
        logger.info(f"Dynamically built HOST_MAP for summary: {host_map_for_summary}")

        host_to_vcenter_map = {
            doc['host_name']: doc['vcenter']
            for doc in host_docs if 'host_name' in doc and 'vcenter' in doc}
            
        location_map = {loc['code']: loc['name'] for loc in locations_data if 'code' in loc and 'name' in loc}
        
        logger.info("Successfully fetched data from MongoDB.")

        vendor_map = get_vendor_prefix_map()
        apm_data = fetch_apm_credentials()
        apm_lookup = build_apm_lookup(apm_data)

        course_allocs, trainer_pods = unpack_interim_allocations(interim_docs, vendor_map, location_map)
        extended_pods = unpack_current_allocations(current_docs, vendor_map, apm_lookup, location_map, host_to_vcenter_map)
        
        logger.info(f"Processed: {len(course_allocs)} Standard | {len(extended_pods)} Extended | {len(trainer_pods)} Trainer pods")
        
        return course_allocs, trainer_pods, extended_pods, host_map_for_summary

    except Exception as e:
        logger.error(f"FATAL: An error occurred during data fetching: {e}", exc_info=True)
        raise e