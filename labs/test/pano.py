#!/usr/bin/env python3.10

import argparse
import re
import pexpect
import subprocess
from pymongo import MongoClient
from tabulate import tabulate
import socket
import ssl
import sys
import threading
print("testing from the latest pano.py")
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from db_utils import get_vcenter_by_host

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED = '\033[91m'
ENDC = '\033[0m'

def strip_ansi(text): return ANSI_ESCAPE.sub('', text)
def log(msg):
    if VERBOSE: print(f"[DEBUG] {msg}")

def get_course_components(course_name):
    try:
        mongo_uri = "mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db"
        client = MongoClient(mongo_uri)
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}")
        if not doc or "components" not in doc:
            print(f"❌ No components found for course: {course_name}"); return [], []
        components = []
        skipped = []
        for c in doc["components"]:
            name = c.get("clone_name")
            ip = c.get("podip")
            port = c.get("podport")
            if name and ip and port:
                components.append((name, ip, port))
                log(f"Component template parsed: {name}, IP: {ip}, Port: {port}")
            else:
                skipped.append(c)
        return components, skipped
    except Exception as e:
        print(f"❌ MongoDB error: {e}"); return [], []

def resolve_ip(ip_template, pod, host):
    if "+X" in ip_template:
        base, _, offset = ip_template.partition("+X")
        parts = base.split(".")
        if len(parts) == 4:
            try:
                parts[-1] = str(int(parts[-1]) + pod)
                ip_template = ".".join(parts)
            except ValueError:
                log(f"Invalid integer in last octet for '+X': {base}")
    if host.lower() in ["hotshot", "trypticon"] and ip_template.startswith("172.30."):
        ip_template = ip_template.replace("172.30.", "172.26.", 1)
        log(f"IP remapped to {ip_template} for host {host.lower()}")
    return ip_template

def get_vm_power_map(si, pod):
    try:
        content = si.RetrieveContent()
        rp_name = f"pa-pod{pod}"
        container_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
        for rp in container_view.view:
            if rp.name == rp_name:
                return {vm.name: vm.runtime.powerState for vm in rp.vm}
        return {}
    except Exception as e:
        print(f"⚠️ Could not fetch VMs for pod {pod}: {e}"); return {}

# Renaming this function to what the test runner expects
def run_ssh_checks(pod, components, host, power_map, print_lock):
    host_fqdn = f"pavr{pod}.us" if host.lower() in ["hotshot", "trypticon"] else f"pavr{pod}"
    check_results = []
    child = None

    try:
        with print_lock:
            print(f"\n🔐 Connecting to {host_fqdn} (Pod {pod}) for internal tests...")
        child = pexpect.spawn(f"ssh {host_fqdn}", timeout=30, echo=False)
        child.expect(["[>#\$]"], timeout=10)
        child.sendline("export PS1='PROMPT> '"); child.expect_exact("PROMPT> ", timeout=10)
        with print_lock:
            print(f"✅ SSH to {host_fqdn} successful. Starting tests...")

    except Exception as e:
        with print_lock:
            print(f"❌ Pod {pod}: SSH connection failed on {host_fqdn}: {e}")
        check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host_fqdn, 'port': 22, 'status': 'FAILED', 'host': host_fqdn})
        with print_lock:
            print_results_table(pod, check_results, power_map)
        return check_results

    for raw_clone_name, raw_ip, port in components:
        clone_name = raw_clone_name.replace('{X}', str(pod))
        ip = resolve_ip(raw_ip, pod, host)
        status = "UNKNOWN"
        nmap_output = ""
        # Replace {X} in names
        clone_name = raw_clone_name.replace('{X}', str(pod))
        ip = resolve_ip(raw_ip, pod, host)

        # TEMP DEBUG

        try:
            print(f"1Testing {component.name} ({component.ip}:{component.port}) from {'host' if is_endpoint else 'vr'}")
            # 🛑 Skip VRs
            if 'pavr' in clone_name.lower() or 'vr' in clone_name.lower():
                log(f"Skipping VR: {clone_name}")
                continue

            # 🌐 Run external check for endpoints
            print(f"2Testing {component.name} ({component.ip}:{component.port}) from {'host' if is_endpoint else 'vr'}")
            if "endpoint" in component.name:
                log(f"🌐 External test for endpoint: {clone_name} @ {ip}:{port}")
                command = ["nmap", "-Pn", "-p", str(port), ip]
                result = subprocess.run(command, capture_output=True, text=True, timeout=20)
                nmap_output = result.stdout.lower()
                # Parse and store result
                match = re.search(rf"{port}/tcp\s+(\w+)", nmap_output)
                if match:
                    status = match.group(1).upper()
                elif "host is up" in nmap_output:
                    status = "FILTERED"
                else:
                    status = "DOWN"

                # Record result
                check_results.append({
                    'pod': pod, 'component': clone_name, 'ip': ip, 'port': port,
                    'status': status, 'host': f"pavr{pod}"
                })
                continue  # ✅ Prevent SSH check below
            print(f"3Testing {component.name} ({component.ip}:{component.port}) from {'host' if is_endpoint else 'vr'}")

            # 🔐 Otherwise, test from inside pod via SSH
            log(f"🔐 Internal test via SSH: {clone_name} @ {ip}:{port}")
            child.sendline(f"nmap -Pn -p {port} {ip}")
            child.expect_exact("PROMPT>", timeout=20)
            nmap_output = child.before.decode(errors="ignore").lower()

            match = re.search(rf"{port}/tcp\s+(\w+)", nmap_output)
            if match:
                status = match.group(1).upper()
            elif "host is up" in nmap_output:
                status = "FILTERED"
            else:
            status = "DOWN"

        except Exception as e:
            status = "ERROR"
            log(f"Test error for {ip}:{port} - {e}")

        check_results.append({
            'pod': pod, 'component': clone_name, 'ip': ip, 'port': port,
            'status': status, 'host': f"pavr{pod}"
        })

        check_results.append({
        'pod': pod, 'component': clone_name, 'ip': ip, 'port': port,
        'status': status, 'host': f"pavr{pod}"
    })

def print_results_table(pod, check_results, power_map):
    if not check_results:
        return
        
    print(f"\n📊 Network Test Summary for Pod {pod}")
    headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
    table_data = [[r['component'], r['ip'], r['host'], r['port'], r['status']] for r in check_results]
    
    formatted_rows = []
    for row in table_data:
        if row[4] not in ["OPEN", "UNKNOWN"]:
             formatted_rows.append([f"{RED}{cell}{ENDC}" for cell in row])
        else:
            formatted_rows.append(row)

    print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))

    any_failures = any(r['status'] not in ["OPEN", "UNKNOWN"] for r in check_results)
    if any_failures:
        powered_off_vms = [[vm, "POWERED OFF"] for vm, state in power_map.items() if state == vim.VirtualMachinePowerState.poweredOff]
        if powered_off_vms:
            print(f"\n🔌 VM Power State Summary for Pod {pod} (Resource Pool: pa-pod{pod})")
            print(tabulate(powered_off_vms, headers=["VM Name", "Power State"], tablefmt="fancy_grid"))
        else:
            print(f"\n🔌 All VMs in Pod {pod} are powered ON")


def main(argv=None, print_lock=None):
    if print_lock is None:
        print_lock = threading.Lock()

    global VERBOSE
    parser = argparse.ArgumentParser(description="PA Pod Network Checker")
    parser.add_argument("-g", "--course", required=True, help="Course name")
    parser.add_argument("--host", required=True, help="Target host label")
    parser.add_argument("-s", "--start", type=int, required=True, help="Start pod number")
    parser.add_argument("-e", "--end", type=int, required=True, help="End pod number")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("-c", "--component", help="Test specific components (comma-separated list).")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose

    vcenter_fqdn = get_vcenter_by_host(args.host)
    if not vcenter_fqdn:
        with print_lock:
            print(f"❌ Could not find vCenter for host '{args.host}' in the database.")
        return []

    try:
        ssl._create_default_https_context = ssl._create_unverified_context
        si = SmartConnect(host=vcenter_fqdn, user="administrator@vcenter.rededucation.com", pwd="pWAR53fht786123$")
        with print_lock:
            print(f"✅ Connected to vCenter: {vcenter_fqdn}")
    except Exception as e:
        with print_lock:
            print(f"❌ Failed to connect to vCenter '{vcenter_fqdn}': {e}")
        return []
    
    with print_lock:
        print(f"\n📘 Fetching components for course: {args.course}")
    # The list of components now includes the IP address again
    components, skipped_components = get_course_components(args.course)
    
    if args.component:
        selected_components = [c.strip() for c in args.component.split(',')]
        components = [c for c in components if c[0] in selected_components]

    if not components:
        with print_lock:
            print(f"❌ No usable components found (or matched filter). Skipping tests.")
        Disconnect(si)
        return []

    all_pod_results = []
    for pod in range(args.start, args.end + 1):
        power_map = get_vm_power_map(si, pod)
        # We must call the function that the test runner expects
        pod_results = run_ssh_checks(pod, components, args.host, power_map, print_lock)
        all_pod_results.extend(pod_results)

    Disconnect(si)
    return all_pod_results

# No 'if __name__ == "__main__"' block to prevent conflicts with the test runner
