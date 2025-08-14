# FILE: labs/test/avaya.py
#!/usr/bin/env python3.10

import argparse
import threading
from pymongo import MongoClient
from tabulate import tabulate
import subprocess
import os
from db_utils import get_vcenter_by_host  # kept for compatibility if used elsewhere

# --- Global Settings ---
VERBOSE = False
RED = '\033[91m'
ENDC = '\033[0m'

# --- Helper Functions ---

def log(msg, print_lock):
    """Prints a message if verbose mode is enabled."""
    if VERBOSE:
        with print_lock:
            print(f"[DEBUG] {msg}")

def get_course_components(course_name, print_lock):
    """Fetches all components for a course from the database."""
    try:
        client = MongoClient("mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result for '{course_name}': {doc}", print_lock)

        if not doc or "components" not in doc:
            with print_lock:
                print(f"❌ No components found for course: {course_name}")
            return []

        return doc["components"]
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB error while fetching components for '{course_name}': {e}")
        return []

def run_local_checks(pod, components_to_test, host, print_lock):
    """
    Run checks from localhost:
      - For each component, read its port from MongoDB.
      - Always scan the single host avvrX(.us) with that port via local nmap.
      - Skip ARP checks (they require L2 adjacency).
    """
    host_fqdn = f"avvr{pod}.us" if host.lower() in ["hotshot", "trypticon"] else f"avvr{pod}"
    results = []

    with print_lock:
        print(f"\n🖥️  Running checks LOCALLY for Pod {pod} (target host: {host_fqdn})")

    for component in components_to_test:
        clone_name = component.get("clone_name", "").replace('{X}', str(pod))
        port = component.get("podport")
        status = "UNKNOWN"

        if not clone_name or not port:
            log(f"Skipping malformed component (missing clone_name/port): {component}", print_lock)
            continue

        # If Mongo lists "arping" as a 'port', we skip: we only nmap the host/port per your requirement.
        if isinstance(port, str) and port.lower() == "arping":
            results.append({
                'pod': pod, 'component': clone_name, 'target': host_fqdn, 'port': port,
                'status': 'SKIPPED (ARP not used in host-only mode)', 'host': host_fqdn
            })
            continue

        # Build local nmap command: single host + single port
        # -Pn: skip ping discovery; -n: no DNS; -p: the specific port
        cmd = ["nmap", "-Pn", "-n", "-p", str(port), host_fqdn]
        log(f"   -> Running locally: {' '.join(cmd)}", print_lock)

        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            output = proc.stdout
            out_lower = output.lower()
            log(f"   <- Output:\n{output.strip()}", print_lock)

            if f"{port}/tcp open" in out_lower or f"{port}/udp open" in out_lower:
                status = "UP"
            elif "filtered" in out_lower:
                status = "FILTERED"
            else:
                status = "DOWN"

        except subprocess.TimeoutExpired:
            status = "TIMEOUT"
        except Exception as e:
            status = f"FAILED ({e})"

        results.append({
            'pod': pod,
            'component': clone_name,
            'target': host_fqdn,
            'port': port,
            'status': status,
            'host': host_fqdn
        })

    return results

def main(argv=None, print_lock=None):
    if print_lock is None:
        print_lock = threading.Lock()
    global VERBOSE

    parser = argparse.ArgumentParser(description="AV Pod Network Checker (host-only nmap)")
    parser.add_argument("-g", "--course", required=True, help="Course name in MongoDB")
    parser.add_argument("--host", required=True, help="Host selector (affects .us suffix for avvrX)")
    parser.add_argument("-s", "--start", type=int, required=True, help="Start pod number")
    parser.add_argument("-e", "--end", type=int, required=True, help="End pod number")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("-c", "--component", help="Test only these component_name(s), comma-separated")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose

    with print_lock:
        print("🚀 Starting AV Pod Network Checker (local nmap, host-only)")
        print(f"\n📘 Fetching components for course: {args.course}")

    all_components = get_course_components(args.course, print_lock)
    if not all_components:
        return []

    # Only components that have a port defined; IPs are ignored in host-only mode
    testable_components = [c for c in all_components if c.get("podport")]

    if args.component:
        selected_names = [c.strip() for c in args.component.split(',')]
        components_to_test = [c for c in testable_components if c.get("component_name") in selected_names]
    else:
        components_to_test = testable_components

    if not components_to_test:
        with print_lock:
            print("✅ No testable components with a port defined for this course.")
        return []

    all_results = []
    for pod in range(args.start, args.end + 1):
        pod_results = run_local_checks(pod, components_to_test, args.host, print_lock)

        with print_lock:
            print(f"\n📊 Network Test Summary for Pod {pod}")
            headers = ["Component", "Target Host", "Pod ID", "Port", "Status"]

            table_data = []
            for r in pod_results:
                status = r['status']
                status_display = f"{RED}{status}{ENDC}" if status != 'UP' else status
                table_data.append([r['component'], r['target'], r['host'], r['port'], status_display])

            print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"))

        all_results.extend(pod_results)

    return all_results

if __name__ == "__main__":
    main()