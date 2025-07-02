# --- START OF FILE labs/test/f5.py ---

#!/usr/bin/env python3.10

import argparse, re, pexpect, ssl, sys, subprocess, threading
from pymongo import MongoClient
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED, ENDC = '\033[91m', '\033[0m'
HOST_ABBR = {"nightbird": "ni", "cliffjumper": "cl", "ultramagnus": "ul", "unicron": "un", "hotshot": "ho"}
VCENTER_MAP = {
    "cliffjumper": "vcenter-appliance-1", "hydra": "vcenter-appliance-1", "unicron": "vcenter-appliance-1",
    "apollo": "vcenter-appliance-2", "nightbird": "vcenter-appliance-2", "ultramagnus": "vcenter-appliance-2",
    "hotshot": "vcenter-appliance-3", "ps01": "vcenter-appliance-4", "ps02": "vcenter-appliance-4",
    "ps03": "vcenter-appliance-4", "shockwave": "vcenter-appliance-5", "optimus": "vcenter-appliance-5",
}

def strip_ansi(text): return ANSI_ESCAPE.sub('', text)
def log(msg, print_lock):
    if VERBOSE:
        with print_lock: print(f"[DEBUG] {msg}")

def resolve_ip(ip_template, pod, host_label, print_lock):
    host_label = host_label.lower().strip()
    if "+X" in ip_template:
        parts = ip_template.replace("+X", "").split(".");
        if len(parts) == 4:
            try:
                parts[-1] = str(int(parts[-1]) + pod); ip_template = ".".join(parts)
                log(f"IP after '+X' resolution: {ip_template}", print_lock)
            except ValueError: log(f"Invalid integer in last octet for '+X': {ip_template}", print_lock)
    if "X" in ip_template: ip_template = ip_template.replace("X", str(pod)); log(f"IP after 'X' substitution: {ip_template}", print_lock)
    if host_label == "hotshot":
        parts = ip_template.split(".")
        if len(parts) == 4 and parts[0] == "172":
            new_ip = ".".join(["172", "26", parts[2], parts[3]])
            log(f"Adjusted IP from 172.{parts[1]} to 172.26 -> {new_ip}", print_lock)
            return new_ip
    return ip_template

def get_course_groups(course_name, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; doc = db["temp_courseconfig"].find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}", print_lock); groups = []
        if doc and "groups" in doc:
            for group in doc["groups"]:
                group_name, comps = group.get("group_name", "Unknown"), []
                for c in group.get("component", []):
                    name, ip, port, clone_vm = c.get("component_name"), c.get("podip"), c.get("podport"), c.get("clone_vm")
                    if name and ip and port and clone_vm: comps.append((name, ip, port, clone_vm)); log(f"[{group_name}] Component: {name}, IP: {ip}, Port: {port}", print_lock)
                if comps: groups.append((group_name, comps))
        return groups
    except Exception as e:
        with print_lock: print(f"❌ MongoDB error: {e}"); return []

def get_vm_power_map_for_class(class_num, si, print_lock):
    try:
        rp_name, content = f"f5-class{class_num}", si.RetrieveContent()
        container_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
        power_map = {}
        def collect_all_vms(rpool):
            try:
                for vm in rpool.vm:
                    power_map[vm.name.lower()] = str(vm.runtime.powerState)
                    if VERBOSE: with print_lock: print(f"🔍 Found VM: {vm.name} - {vm.runtime.powerState}")
                for sub in rpool.resourcePool: log(f"📁 Entering sub-pool: {sub.name}", print_lock); collect_all_vms(sub)
            except Exception as e: log(f"⚠️ Error accessing VMs: {e}", print_lock)
        for rp in container_view.view:
            if rp.name == rp_name: log(f"✅ Located resource pool: {rp_name}", print_lock); collect_all_vms(rp); break
        return power_map
    except Exception as e:
        with print_lock: print(f"⚠️ Failed to collect VM power state info: {e}"); return {}

def run_checks_for_pod(pod, grouped_components, ssh_target, host, class_num, print_lock, first_pod):
    with print_lock: print(f"\n🌐 Starting checks for Pod {pod}...")
    check_results = []
    
    # Outside checks
    for group_name, components in grouped_components:
        for name, raw_ip, port, _ in components:
            if group_name.lower() in ["bigip", "w10"] or (group_name.lower() == "srv" and name == "vr"):
                ip_to_check = resolve_ip(raw_ip, class_num if name == "vr" else pod, host, print_lock)
                status = "UNKNOWN"
                try:
                    cmd = f"nmap -Pn -p {port} {ip_to_check}"; log(f"Running: {cmd}", print_lock)
                    child = pexpect.spawn(cmd, timeout=20); child.expect(pexpect.EOF)
                    status = "UP" if "open" in strip_ansi(child.before.decode()) else "DOWN"
                except Exception as e: with print_lock: print(f"⚠️ Error checking {name}: {e}")
                
                # VR check should only be reported once for the first pod in range
                if name == "vr" and pod != first_pod: continue
                check_results.append({'pod': pod, 'class': class_num, 'group': group_name, 'component': name, 'ip': ip_to_check, 'source': 'external', 'port': port, 'status': status})
    
    # Inside SSH checks
    try:
        child = pexpect.spawn(f"ssh {ssh_target}", timeout=30); child.expect(r"#\s*$")
        with print_lock: print(f"✅ SSH to {ssh_target} successful for Pod {pod} checks.")
        for group_name, components in grouped_components:
            for name, raw_ip, port, _ in components:
                if group_name.lower() in ["bigip", "w10"] or (group_name.lower() == "srv" and name == "vr"): continue
                ip = resolve_ip(raw_ip, pod, host, print_lock); status = "UNKNOWN"
                try:
                    cmd = f"nmap -Pn -p {port} {ip}"; log(f"Running (inside SSH): {cmd}", print_lock)
                    child.sendline(cmd); child.expect(r"#\s*$", timeout=20)
                    status = "UP" if "open" in strip_ansi(child.before.decode()) else "DOWN"
                except Exception as e: with print_lock: print(f"⚠️ Error checking {name} via SSH: {e}")
                check_results.append({'pod': pod, 'class': class_num, 'group': group_name, 'component': name, 'ip': ip, 'source': ssh_target, 'port': port, 'status': status})
        child.sendline("exit"); child.close()
    except Exception as e:
        with print_lock: print(f"❌ SSH session to {ssh_target} failed: {e}")
        check_results.append({'pod': pod, 'class': class_num, 'component': 'SSH Connection', 'ip': ssh_target, 'status': 'FAILED'})

    return check_results

def main(argv=None, print_lock=None):
    if print_lock is None: print_lock = threading.Lock()
    global VERBOSE
    parser = argparse.ArgumentParser(description="F5 Network & VM Checker")
    parser.add_argument("-g", "--course", required=True); parser.add_argument("--host", required=True)
    parser.add_argument("--classnum", type=int, required=True); parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True); parser.add_argument("--verbose", action="store_true")
    parser.add_argument("-c", "--component", help="Test specific components (comma-separated list).")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose
    ssh_target = f"f5vr{args.classnum}.us" if args.host.lower() == "hotshot" else f"f5vr{args.classnum}"
    
    with print_lock:
        print(f"[INFO] Connecting to: {ssh_target}")
        print(f"📘 Fetching group-based components for course: {args.course}")
    
    grouped_components = get_course_groups(args.course, print_lock)
    if not grouped_components: with print_lock: print(f"❌ No components found for course {args.course}"); return []
    
    if args.component:
        selected = [c.strip() for c in args.component.split(',')]
        filtered_groups, original_count, filtered_count = [], 0, 0
        for group_name, components in grouped_components:
            original_count += len(components)
            filtered_comps = [c for c in components if c[0] in selected]
            if filtered_comps: filtered_groups.append((group_name, filtered_comps)); filtered_count += len(filtered_comps)
        with print_lock: print(f"\n🔎 Filtering: Selected {filtered_count} of {original_count} components.")
        grouped_components = filtered_groups
    if not grouped_components: with print_lock: print(f"❌ No usable components found (or matched filter). Exiting."); return []

    vcenter_host = VCENTER_MAP.get(args.host.lower())
    if not vcenter_host: with print_lock: print(f"❌ Host '{args.host}' not in VCENTER_MAP."); return []
    try:
        ssl._create_default_https_context = ssl._create_unverified_context
        si = SmartConnect(host=f"{vcenter_host}.rededucation.com", user="administrator@vcenter.rededucation.com", pwd="pWAR53fht786123$")
        with print_lock: print(f"✅ Connected to vCenter: {vcenter_host}.rededucation.com")
    except Exception as e: with print_lock: print(f"❌ Failed to connect to vCenter: {e}"); return []
    
    power_map = get_vm_power_map_for_class(args.classnum, si, print_lock)
    all_results = []
    
    for pod in range(args.start, args.end + 1):
        pod_results = run_checks_for_pod(pod, grouped_components, ssh_target, args.host, args.classnum, print_lock, args.start)
        all_results.extend(pod_results)
    
    with print_lock:
        powered_off_vms = [[vm, state] for vm, state in power_map.items() if state != "poweredOn"]
        if powered_off_vms:
            print(f"\n🔌 Final VM Power State Check for F5-class{args.classnum}")
            formatted_vm_rows = [[f"{RED}{cell}{ENDC}" for cell in row] for row in powered_off_vms]
            print(tabulate(formatted_vm_rows, headers=["VM Name", "Power State"], tablefmt="fancy_grid"))
        else: print(f"\n✅ All VMs in F5-class{args.classnum} are powered ON")
    Disconnect(si)
    
    return all_results

if __name__ == "__main__":
    main()
# --- END OF FILE labs/test/f5.py ---