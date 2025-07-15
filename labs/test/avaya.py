#!/usr/bin/env python3.10

import argparse, re, socket, ssl, sys, pexpect, threading
from pymongo import MongoClient
from tabulate import tabulate
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from db_utils import get_vcenter_by_host

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED, ENDC = '\033[91m', '\033[0m'

def log(msg, print_lock):
    if VERBOSE:
        with print_lock: print(f"[DEBUG] {msg}")

def strip_ansi(text): return ANSI_ESCAPE.sub('', text)

def resolve_ip(ip_template, pod, print_lock):
    if "+X" in ip_template:
        base, _, offset = ip_template.partition("+X")
        parts = base.split(".")
        if len(parts) == 4:
            try:
                parts[-1] = str(int(parts[-1]) + pod)
                ip_template = ".".join(parts)
            except ValueError:
                log(f"Invalid integer in last octet for '+X': {base}", print_lock)
        log(f"IP after +X resolution: {ip_template}", print_lock)
    return ip_template

def get_course_components(course_name, pod, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}", print_lock)
        if not doc or "components" not in doc:
            with print_lock:
                print(f"❌ No components found for course: {course_name}")
            return [], []
        components, skipped = [], []
        for c in doc["components"]:
            name, ip, port = c.get("component_name"), c.get("podip"), c.get("podport")
            if name and ip and port:
                resolved_ip = resolve_ip(ip, pod, print_lock) if pod is not None else ip
                components.append((name, resolved_ip, port))
                log(f"Component parsed for pod {pod}: {name}, IP: {resolved_ip}, Port: {port}", print_lock)
            else:
                skipped.append(c)
        return components, skipped
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB error: {e}")
        return [], []

def get_vm_power_map(si, pod, print_lock):
    try:
        content, rp_name = si.RetrieveContent(), f"av-pod{pod}"
        container_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
        for rp in container_view.view:
            if rp.name == rp_name:
                return {vm.name: vm.runtime.powerState for vm in rp.vm}
        return {}
    except Exception as e:
        with print_lock:
            print(f"⚠️ Could not fetch VMs for pod {pod}: {e}")
        return {}

def run_ssh_checks(pod, components, host, power_map, print_lock):
    # --- MODIFIED ---
    host_fqdn = f"avvr{pod}.us" if host.lower() in ["hotshot", "trypticon"] else f"avvr{pod}"
    # --- END MODIFICATION ---
    results = []
    with print_lock:
        print(f"\n🔐 Connecting to {host_fqdn} (Pod {pod}) via SSH...")
    try:
        child = pexpect.spawn(f"ssh {host_fqdn}", timeout=30)
        child.expect(["[>#\$]"], timeout=10)
        child.sendline("export PS1='PROMPT> '")
        child.expect_exact("PROMPT> ", timeout=10)
        child.sendline("echo READY")
        child.expect_exact("READY", timeout=10)
        child.expect_exact("PROMPT> ", timeout=10)
        with print_lock:
            print(f"✅ SSH to {host_fqdn} (Pod {pod}) successful")

        for component, ip, port in components:
            status = "UNKNOWN"
            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                child.expect_exact("PROMPT>", timeout=10)
                iface = child.before.decode(errors="ignore").strip().splitlines()[-1] if child.before.decode(errors="ignore").strip().splitlines() else ""
                if iface:
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    child.expect_exact("PROMPT>", timeout=15)
                    status = "UP" if "Unicast reply" in child.before.decode(errors="ignore") else "DOWN"
            else:
                child.sendline(f"nmap -Pn -p {port} {ip}")
                child.expect_exact("PROMPT>", timeout=20)
                match = re.search(rf"{port}/tcp\s+(\w+)", child.before.decode(errors="ignore").lower())
                status = match.group(1).upper() if match else "DOWN"
            results.append({'pod': pod, 'component': component, 'ip': ip, 'port': port, 'status': status, 'host': host_fqdn})
        child.sendline("exit")
        child.expect(pexpect.EOF, timeout=10)
        child.close()
    except Exception as e:
        with print_lock:
            print(f"❌ Pod {pod}: SSH or command execution failed on {host_fqdn}: {e}")
        results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host_fqdn, 'status': 'FAILED'})

    with print_lock:
        print(f"\n📊 Network Test Summary for Pod {pod}")
        headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
        table_data = [[r['component'], r['ip'], r['host'], r['port'], r['status']] for r in results]
        formatted_rows = [[f"{RED}{cell}{ENDC}" if row[4] != 'UP' else cell for cell in row] for row in table_data]
        print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))
        if any(r['status'] != 'UP' for r in results):
            powered_off = [[vm, "POWERED OFF"] for vm, state in power_map.items() if state == vim.VirtualMachinePowerState.poweredOff]
            if powered_off:
                print(f"\n🔌 VM Power State Summary for Pod {pod} (Resource Pool: av-pod{pod})")
                print(tabulate(powered_off, headers=["VM Name", "Power State"], tablefmt="fancy_grid"))
            else:
                print(f"\n🔌 All VMs in Pod {pod} are powered ON")
    return results

def main(argv=None, print_lock=None):
    if print_lock is None:
        print_lock = threading.Lock()
    global VERBOSE
    parser = argparse.ArgumentParser(description="AV Pod Network Checker")
    parser.add_argument("-g", "--course", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("-c", "--component", help="Test specific components.")
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
    
    all_results = []
    for pod in range(args.start, args.end + 1):
        with print_lock:
            print(f"\n📘 Fetching components for course: {args.course} (Pod {pod})")
        components, _ = get_course_components(args.course, pod, print_lock)
        if args.component:
            selected = [c.strip() for c in args.component.split(',')]
            components = [c for c in components if c[0] in selected]
        if not components:
            with print_lock:
                print(f"❌ Pod {pod}: No usable components found. Skipping pod.")
            continue
        
        power_map = get_vm_power_map(si, pod, print_lock)
        log(f"Pod {pod} Power Map: {power_map}", print_lock)
        pod_results = run_ssh_checks(pod, components, args.host, power_map, print_lock)
        all_results.extend(pod_results)
    
    Disconnect(si)
    return all_results

if __name__ == "__main__":
    main()