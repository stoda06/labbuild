# FILE: labs/test/avaya.py
#!/usr/bin/env python3.10

import argparse
import threading
from pymongo import MongoClient
from tabulate import tabulate
import subprocess
import shlex
import re
from db_utils import get_vcenter_by_host  # kept for compatibility if used elsewhere

# --- Global Settings ---
VERBOSE = False
RED = '\033[91m'
ENDC = '\033[0m'

# --- Helper Functions ---

def log(msg, print_lock):
    if VERBOSE:
        with print_lock:
            print(f"[DEBUG] {msg}", flush=True)

def get_course_components(course_name, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result for '{course_name}': {doc}", print_lock)
        if not doc or "components" not in doc:
            with print_lock:
                print(f"❌ No components found for course: {course_name}", flush=True)
            return []
        return doc["components"]
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB error while fetching components for '{course_name}': {e}", flush=True)
        return []

def expand_plus_x_ip(ip: str, pod: int) -> str:
    m = re.match(r'^(\d+\.\d+\.\d+)\.(\d+)\+X$', ip)
    if not m:
        return ip
    base, last = m.group(1), int(m.group(2))
    new_last = max(0, min(255, last + pod))
    return f"{base}.{new_last}"

def resolve_component_ip(component: dict, pod: int, print_lock):
    candidate_keys = ["podip", "ip", "ip_address", "addr", "hostip"]
    raw = None
    for k in candidate_keys:
        if k in component and component[k]:
            raw = str(component[k]).strip()
            break
    if not raw:
        return None, None
    resolved = raw
    if "{X}" in resolved:
        resolved = resolved.replace("{X}", str(pod))
    if resolved.endswith("+X"):
        resolved = expand_plus_x_ip(resolved, pod)
    log(f"Resolved IP for '{component.get('component_name')}': raw='{raw}', resolved='{resolved}'", print_lock)
    return resolved, raw

def target_vr_host(pod: int, host_selector: str) -> str:
    return f"avvr{pod}.us" if host_selector.lower() in ["hotshot", "trypticon"] else f"avvr{pod}"

def run_nmap_local(target: str, port: int | str, print_lock):
    cmd = ["nmap", "-Pn", "-n", "-p", str(port), target]
    log(f"Running LOCAL nmap: {' '.join(cmd)}", print_lock)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        output = proc.stdout
        log(f"LOCAL nmap output for {target}:{port}:\n{output}", print_lock)
        return output, None
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT"
    except Exception as e:
        return "", f"FAILED ({e})"

def run_nmap_via_vr(vr_host: str, target_ip: str, port: int | str, print_lock):
    remote_cmd = f"nmap -Pn -n -p {shlex.quote(str(port))} {shlex.quote(str(target_ip))}"
    ssh_cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=15",
        vr_host,
        remote_cmd
    ]
    log(f"Running VR-SSH nmap: {' '.join(ssh_cmd)}", print_lock)
    try:
        proc = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=180)
        output = proc.stdout
        log(f"VR-SSH nmap output from {vr_host} for {target_ip}:{port}:\n{output}", print_lock)
        return output, None
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT (VR-SSH)"
    except Exception as e:
        return "", f"FAILED (VR-SSH: {e})"

def parse_nmap_status(output: str, port: int | str) -> str:
    out_lower = (output or "").lower()
    port = str(port)
    if f"{port}/tcp open" in out_lower or f"{port}/udp open" in out_lower:
        return "UP"
    if "filtered" in out_lower:
        return "FILTERED"
    if not output:
        return "DOWN"
    return "DOWN"

def run_checks(pod, components_to_test, host_selector, print_lock):
    vr_host = target_vr_host(pod, host_selector)
    results = []

    with print_lock:
        print(f"\n🧪 Running checks for Pod {pod} (VR: {vr_host})", flush=True)

    for component in components_to_test:
        clone_name = component.get("clone_name", "").replace('{X}', str(pod))
        port = component.get("podport")
        if not clone_name or not port:
            log(f"Skipping malformed component (missing clone_name/port): {component}", print_lock)
            continue
        if isinstance(port, str) and port.lower() == "arping":
            results.append({
                'pod': pod, 'component': clone_name, 'target': vr_host, 'port': port,
                'status': 'SKIPPED (ARP not used)', 'via': 'N/A'
            })
            continue

        ip_resolved, ip_raw = resolve_component_ip(component, pod, print_lock)
        raw_has_X = bool(ip_raw) and ("{X}" in ip_raw or ip_raw.endswith("+X"))

        # Primary routing rules
        if ip_resolved:
            if ip_resolved.startswith("172.26."):
                via = "LOCAL"
                target = ip_resolved
                output, err = run_nmap_local(target, port, print_lock)
            else:
                via = "VR-SSH"
                target = ip_resolved
                output, err = run_nmap_via_vr(vr_host, target, port, print_lock)
        else:
            via = "LOCAL"
            target = vr_host
            output, err = run_nmap_local(target, port, print_lock)

        status = err if err else parse_nmap_status(output, port)

        # Fallback: if LOCAL + FILTERED + raw IP had 'X' + 172.26.* => VR retry
        if (
            status == "FILTERED"
            and via == "LOCAL"
            and ip_resolved
            and ip_resolved.startswith("172.26.")
            and raw_has_X
        ):
            log(f"Filtered locally for {clone_name}; raw IP had X → retrying via VR-SSH", print_lock)
            output2, err2 = run_nmap_via_vr(vr_host, ip_resolved, port, print_lock)
            status = err2 if err2 else parse_nmap_status(output2, port)
            via = "VR-SSH (retry)"
            target = ip_resolved

        results.append({
            'pod': pod,
            'component': clone_name,
            'target': target,
            'port': port,
            'status': status,
            'via': via
        })

    return results

def main(argv=None, print_lock=None):
    if print_lock is None:
        print_lock = threading.Lock()
    global VERBOSE

    parser = argparse.ArgumentParser(description="AV Pod Network Checker (LOCAL vs VR-SSH)")
    parser.add_argument("-g", "--course", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    parser.add_argument("-v", "--verbose", "--ver", action="store_true", help="Verbose output")
    parser.add_argument("-c", "--component")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose

    with print_lock:
        print("🚀 Starting AV Pod Network Checker (LOCAL vs VR-SSH)", flush=True)
        print(f"\n📘 Fetching components for course: {args.course}", flush=True)

    all_components = get_course_components(args.course, print_lock)
    if not all_components:
        return []

    testable_components = [c for c in all_components if c.get("podport")]
    components_to_test = (
        [c for c in testable_components if c.get("component_name") in [n.strip() for n in args.component.split(',')]]
        if args.component
        else testable_components
    )
    if not components_to_test:
        with print_lock:
            print("✅ No testable components with a port defined for this course.", flush=True)
        return []

    all_results = []
    for pod in range(args.start, args.end + 1):
        pod_results = run_checks(pod, components_to_test, args.host, print_lock)
        with print_lock:
            print(f"\n📊 Network Test Summary for Pod {pod}", flush=True)
            headers = ["Component", "Target", "Pod", "Port", "Status", "Via"]
            table_data = []
            for r in pod_results:
                status_display = f"{RED}{r['status']}{ENDC}" if r['status'] != "UP" else r['status']
                table_data.append([r['component'], r['target'], r['pod'], r['port'], status_display, r['via']])
            print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"), flush=True)
        all_results.extend(pod_results)

    return all_results

if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(f"❌ Exited: {e}", flush=True)
        raise
    except Exception as e:
        print(f"❌ Unhandled exception: {e}", flush=True)
        raise
