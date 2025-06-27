#!/usr/bin/env python3.10

import argparse
import re
import socket
import ssl
import sys
import pexpect
from pymongo import MongoClient
from tabulate import tabulate
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED = '\033[91m'
ENDC = '\033[0m'

HOST_TO_VCENTER = {
    "cliffjumper": 1,
    "hydra": 1,
    "unicron": 1,
    "apollo": 2,
    "nightbird": 2,
    "ultramagnus": 2,
    "hotshot": 3,
    "ps01": 4,
    "ps02": 4,
    "ps03": 4,
    "shockwave": 5,
    "optimus": 5,
}

def log(msg):
    if VERBOSE:
        print(f"[DEBUG] {msg}")

def strip_ansi(text):
    return ANSI_ESCAPE.sub('', text)

def resolve_ip(ip_template, pod):
    if "+X" in ip_template:
        base = ip_template.replace("+X", "")
        parts = base.split(".")
        if len(parts) != 4:
            log(f"Invalid IP template after +X removal: {ip_template}")
            return ip_template
        parts[-1] = str(int(parts[-1]) + pod)
        ip_template = ".".join(parts)

    log(f"IP after +X resolution: {ip_template}")
    return ip_template

def get_course_components(course_name, pod=None):
    try:
        mongo_uri = "mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db"
        client = MongoClient(mongo_uri)
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}")

        if not doc or "components" not in doc:
            print(f"❌ No components found for course: {course_name}")
            return [], []

        components = []
        skipped = []
        for c in doc["components"]:
            name = c.get("component_name")
            ip = c.get("podip")
            port = c.get("podport")
            if name and ip and port:
                resolved_ip = resolve_ip(ip, pod) if pod is not None else ip
                components.append((name, resolved_ip, port))
                log(f"Component parsed for pod {pod}: {name}, IP: {resolved_ip}, Port: {port}")
            else:
                skipped.append(c)
        return components, skipped
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return [], []

def get_vm_power_map(si, pod):
    try:
        content = si.RetrieveContent()
        rp_name = f"av-pod{pod}"
        container_view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.ResourcePool], True)
        for rp in container_view.view:
            if rp.name == rp_name:
                return {vm.name: vm.runtime.powerState for vm in rp.vm}
        return {}
    except Exception as e:
        print(f"⚠️ Could not fetch VMs for pod {pod}: {e}")
        return {}

def run_ssh_checks(pod, components, host, power_map):
    host_fqdn = f"avvr{pod}.us" if host.lower() == "hotshot" else f"avvr{pod}"
    print(f"\n🔐 Connecting to {host_fqdn} (Pod {pod}) via SSH...")

    results = []
    try:
        child = pexpect.spawn(f"ssh {host_fqdn}", timeout=30)
        child.expect(["[>#\$]"], timeout=10)
        child.sendline("export PS1='PROMPT> '")
        child.expect_exact("PROMPT> ", timeout=10)
        child.sendline("echo READY")
        child.expect_exact("READY", timeout=10)
        child.expect_exact("PROMPT> ", timeout=10)

        print(f"✅ SSH to {host_fqdn} (Pod {pod}) successful")

        for component, ip, port in components:
            status = "UNKNOWN"
            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                child.expect_exact("PROMPT>", timeout=10)
                iface_lines = child.before.decode(errors="ignore").strip().splitlines()
                iface = iface_lines[-1] if iface_lines else ""
                if iface:
                    print(f"📶 Pod {pod}: Running arping -I {iface} {ip}")
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    child.expect_exact("PROMPT>", timeout=15)
                    output = child.before.decode(errors="ignore")
                    status = "UP" if "Unicast reply" in output else "DOWN"
                else:
                    print(f"⚠️ Pod {pod}: Could not determine interface for IP {ip}")
            else:
                print(f"🔎 Pod {pod}: Running nmap -Pn -p {port} {ip}")
                child.sendline(f"nmap -Pn -p {port} {ip}")
                child.expect_exact("PROMPT>", timeout=20)
                output = child.before.decode(errors="ignore").strip()
                match = re.search(rf"{port}/tcp\s+(\w+)", output.lower())
                status = match.group(1).upper() if match else "DOWN"

            results.append([component, ip, host_fqdn, port, status])

        child.sendline("exit")
        child.expect(pexpect.EOF, timeout=10)
        child.close()

    except Exception as e:
        print(f"❌ Pod {pod}: SSH or command execution failed on {host_fqdn}: {e}")
        if 'child' in locals():
            print(child)

    if results:
        print(f"\n📊 Network Test Summary for Pod {pod}")
        headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
        formatted_rows = []
        for row in results:
            if row[4] in ["DOWN", "FILTERED"]:
                formatted_rows.append([f"{RED}{cell}{ENDC}" for cell in row])
            else:
                formatted_rows.append(row)
        print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))

    if any(row[4] in ["DOWN", "FILTERED"] for row in results):
        powered_off = [[vm, "POWERED OFF"]
                       for vm, state in power_map.items()
                       if state == vim.VirtualMachinePowerState.poweredOff]
        if powered_off:
            print(f"\n🔌 VM Power State Summary for Pod {pod} (Resource Pool: av-pod{pod})")
            print(tabulate(powered_off, headers=["VM Name", "Power State"], tablefmt="fancy_grid"))
        else:
            print(f"\n🔌 All VMs in Pod {pod} are powered ON")

def main(argv=None):
    global VERBOSE
    parser = argparse.ArgumentParser(description="AV Pod Network Checker")
    parser.add_argument("-g", "--course", required=True, help="Course name")
    parser.add_argument("--host", required=True, help="Target host label")
    parser.add_argument("-s", "--start", type=int, required=True, help="Start pod number")
    parser.add_argument("-e", "--end", type=int, required=True, help="End pod number")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args(argv)
    VERBOSE = args.verbose

    vcenter_number = HOST_TO_VCENTER.get(args.host.lower())
    if not vcenter_number:
        print(f"❌ Host '{args.host}' is not mapped to a vCenter.")
        return

    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        si = SmartConnect(
            host=f"vcenter-appliance-{vcenter_number}.rededucation.com",
            user="administrator@vcenter.rededucation.com",
            pwd="pWAR53fht786123$"
        )
    except Exception as e:
        print(f"❌ Failed to connect to vCenter: {e}")
        return

    for pod in range(args.start, args.end + 1):
        print(f"\n📘 Fetching components for course: {args.course} (Pod {pod})")
        components, skipped = get_course_components(args.course, pod)
        if skipped:
            skipped_summary = [f"{c.get('component_name', 'unknown')} ({c.get('base_vm', 'unknown')})"
                               for c in skipped if isinstance(c, dict)]
            print(f"⚠️ Pod {pod}: Skipped components (missing podip/podport): {', '.join(skipped_summary)}")

        if not components:
            print(f"❌ Pod {pod}: No usable components found. Skipping pod.")
            continue

        power_map = get_vm_power_map(si, pod)
        log(f"Pod {pod} Power Map: {power_map}")
        run_ssh_checks(pod, components, args.host, power_map)

    Disconnect(si)

if __name__ == "__main__":
    main()