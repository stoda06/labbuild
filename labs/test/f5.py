#!/usr/bin/env python3.10

import argparse
import re
import pexpect
import ssl
import sys
from pymongo import MongoClient
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate
import subprocess 

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED = '\033[91m'
ENDC = '\033[0m'
HOST_ABBR = {
    "nightbird": "ni",
    "cliffjumper": "cl",
    "ultramagnus": "ul",
    "unicron": "un",
    "hotshot": "ho",
}
VCENTER_MAP = {
    "cliffjumper": "vcenter-appliance-1",
    "hydra": "vcenter-appliance-1",
    "unicron": "vcenter-appliance-1",
    "apollo": "vcenter-appliance-2",
    "nightbird": "vcenter-appliance-2",
    "ultramagnus": "vcenter-appliance-2",
    "hotshot": "vcenter-appliance-3",
    "ps01": "vcenter-appliance-4",
    "ps02": "vcenter-appliance-4",
    "ps03": "vcenter-appliance-4",
    "shockwave": "vcenter-appliance-5",
    "optimus": "vcenter-appliance-5",
}

def strip_ansi(text):
    return ANSI_ESCAPE.sub('', text)

def log(msg):
    if VERBOSE:
        print(f"[DEBUG] {msg}")

def resolve_ip(ip_template, pod, host_label):
    host_label = host_label.lower().strip()
    if "+X" in ip_template:
        parts = ip_template.replace("+X", "").split(".")
        if len(parts) == 4:
            try:
                parts[-1] = str(int(parts[-1]) + pod)
                ip_template = ".".join(parts)
                log(f"IP after '+X' resolution: {ip_template}")
            except ValueError:
                log(f"Invalid integer in last octet for '+X': {ip_template}")
    if "X" in ip_template:
        ip_template = ip_template.replace("X", str(pod))
        log(f"IP after 'X' substitution: {ip_template}")
    if host_label == "hotshot":
        parts = ip_template.split(".")
        if len(parts) == 4 and parts[0] == "172":
            original_octet = parts[1]
            parts[1] = "26"
            new_ip = ".".join(parts)
            log(f"Adjusted IP from 172.{original_octet} to 172.26 -> {new_ip}")
            return new_ip
    return ip_template

def get_course_groups(course_name):
    try:
        mongo_uri = "mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db"
        client = MongoClient(mongo_uri)
        db = client["labbuild_db"]
        doc = db["temp_courseconfig"].find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}")
        groups = []
        if doc and "groups" in doc:
            for group in doc["groups"]:
                group_name = group.get("group_name", "Unknown")
                comps = []
                for c in group.get("component", []):
                    name = c.get("component_name")
                    ip = c.get("podip")
                    port = c.get("podport")
                    clone_vm = c.get("clone_vm")
                    if name and ip and port and clone_vm:
                        comps.append((name, ip, port, clone_vm))
                        log(f"[{group_name}] Component: {name}, IP: {ip}, Port: {port}")
                if comps:
                    groups.append((group_name, comps))
        return groups
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return []

def get_vm_power_map_for_class(class_num, si):
    try:
        rp_name = f"f5-class{class_num}"
        content = si.RetrieveContent()
        container_view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.ResourcePool], True
        )
        power_map = {}
        def collect_all_vms(rpool):
            try:
                for vm in rpool.vm:
                    power_map[vm.name.lower()] = str(vm.runtime.powerState)
                    if VERBOSE:
                        print(f"🔍 Found VM: {vm.name} - {vm.runtime.powerState}")
                for sub in rpool.resourcePool:
                    log(f"📁 Entering sub-pool: {sub.name}")
                    collect_all_vms(sub)
            except Exception as e:
                log(f"⚠️ Error accessing VMs: {e}")
        for rp in container_view.view:
            if rp.name == rp_name:
                log(f"✅ Located resource pool: {rp_name}")
                collect_all_vms(rp)
                break
        return power_map
    except Exception as e:
        print(f"⚠️ Failed to collect VM power state info: {e}")
        return {}

def run_outside_checks(pod, grouped_components, ssh_target, host, class_num, vr_state):
    print(f"\n🌐 Starting **outside** checks for Pod {pod}...")
    results = []

    for group_name, components in grouped_components:
        for name, raw_ip, port, _ in components:
            group = group_name.lower()

            if group == "srv" and name == "vr":
                if vr_state["checked"]:
                    continue
                ip = resolve_ip(raw_ip, class_num, host)
                vr_state["checked"] = True
                vr_state["ip"] = ip
                vr_state["port"] = port
                vr_state["status"] = "UNKNOWN"
                try:
                    if port.lower() == "arping":
                        child = pexpect.spawn("bash", timeout=10)
                        subnet = ".".join(ip.split(".")[:3])
                        child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                        child.expect(r"\$ ")
                        iface = child.before.decode().splitlines()[-1]
                        log(f"Running: arping -c 3 -I {iface} {ip}")
                        child.sendline(f"arping -c 3 -I {iface} {ip}")
                        child.expect(r"\$ ")
                        out = strip_ansi(child.before.decode())
                        vr_state["status"] = "UP" if "Unicast reply" in out else "DOWN"
                    else:
                        cmd = f"nmap -Pn -p {port} {ip}"
                        log(f"Running: {cmd}")
                        child = pexpect.spawn(cmd, timeout=20)
                        child.expect(pexpect.EOF)
                        out = strip_ansi(child.before.decode())
                        vr_state["status"] = "UP" if "open" in out else "DOWN"
                except Exception as e:
                    print(f"⚠️ Error checking VR: {e}")
                if pod == vr_state.get("first_pod", pod):
                    results.append([pod, "srv", "vr", ip, "external", port, vr_state["status"]])
            elif group in ["bigip", "w10"]:
                ip = resolve_ip(raw_ip, pod, host)
                status = "UNKNOWN"
                try:
                    if port.lower() == "arping":
                        child = pexpect.spawn("bash", timeout=10)
                        subnet = ".".join(ip.split(".")[:3])
                        child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                        child.expect(r"\$ ")
                        iface = child.before.decode().splitlines()[-1]
                        log(f"Running: arping -c 3 -I {iface} {ip}")
                        child.sendline(f"arping -c 3 -I {iface} {ip}")
                        child.expect(r"\$ ")
                        out = strip_ansi(child.before.decode())
                        status = "UP" if "Unicast reply" in out else "DOWN"
                    else:
                        cmd = f"nmap -Pn -p {port} {ip}"
                        log(f"Running: {cmd}")
                        child = pexpect.spawn(cmd, timeout=20)
                        child.expect(pexpect.EOF)
                        out = strip_ansi(child.before.decode())
                        status = "UP" if "open" in out else "DOWN"
                except Exception as e:
                    print(f"⚠️ Error checking {name}: {e}")
                results.append([pod, group_name, name, ip, "external", port, status])

    print(f"\n📊 Outside Network Check Summary for Pod {pod}")
    headers = ["Pod", "Group", "Component", "IP", "Host", "Port", "Status"]
    print(tabulate([[f"{RED}{x}{ENDC}" if row[6] in ["DOWN", "FILTERED"] else x for x in row] for row in results], headers, tablefmt="fancy_grid"))
    return results

def run_ssh_checks(pod, grouped_components, ssh_target, host, child, class_num):
    print(f"\n🔐 Starting **inside SSH** checks for Pod {pod}...")
    results = []
    for group_name, components in grouped_components:
        for name, raw_ip, port, clone_template in components:
            if group_name.lower() in ["bigip", "w10"] or (group_name.lower() == "srv" and name == "vr"):
                continue
            ip = resolve_ip(raw_ip, pod, host)
            status = "UNKNOWN"
            try:
                if port.lower() == "arping":
                    subnet = ".".join(ip.split(".")[:3])
                    child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                    child.expect(r"#\s*$", timeout=10)
                    iface = child.before.decode().splitlines()[-1]
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    log(f"Running (inside SSH): arping -c 3 -I {iface} {ip}")
                    child.expect(r"#\s*$", timeout=10)
                    out = strip_ansi(child.before.decode())
                    status = "UP" if "Unicast reply" in out else "DOWN"
                else:
                    cmd = f"nmap -Pn -p {port} {ip}"
                    log(f"Running (inside SSH): {cmd}")
                    child.sendline(f"nmap -Pn -p {port} {ip}")
                    child.expect(r"#\s*$", timeout=20)
                    out = strip_ansi(child.before.decode())
                    status = "UP" if "open" in out else "DOWN"
            except Exception as e:
                print(f"⚠️ Error checking {name}: {e}")
            results.append([pod, group_name, name, ip, ssh_target, port, status])
    print(f"\n📊 Inside SSH Network Check Summary for Pod {pod}")
    headers = ["Pod", "Group", "Component", "IP", "SSH Host", "Port", "Status"]
    print(tabulate([[f"{RED}{x}{ENDC}" if row[6] in ["DOWN", "FILTERED"] else x for x in row] for row in results], headers, tablefmt="fancy_grid"))
    return results

def print_final_power_status(power_map, class_num):
    powered_off_vms = [[vm, state] for vm, state in power_map.items() if state != "poweredOn"]
    if powered_off_vms:
        print(f"\n🔌 Final VM Power State Check for F5-class{class_num}")
        formatted_vm_rows = [[f"{RED}{cell}{ENDC}" for cell in row] for row in powered_off_vms]
        print(tabulate(formatted_vm_rows, headers=["VM Name", "Power State"], tablefmt="fancy_grid"))
    else:
        print(f"\n✅ All VMs in F5-class{class_num} are powered ON")

def main(argv=None):
    global VERBOSE
    parser = argparse.ArgumentParser(description="F5 Network & VM Checker")
    parser.add_argument("-g", "--course", required=True, help="Course name")
    parser.add_argument("--host", required=True, help="Target host label")
    parser.add_argument("--classnum", type=int, required=True, help="Class number for f5vrX and F5-classX")
    parser.add_argument("-s", "--start", type=int, required=True, help="Start pod number")
    parser.add_argument("-e", "--end", type=int, required=True, help="End pod number")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose
    ssh_target = f"f5vr{args.classnum}.us" if args.host.lower() == "hotshot" else f"f5vr{args.classnum}"
    print(f"[INFO] Connecting to: {ssh_target}")
    print(f"📘 Fetching group-based components for course: {args.course}")
    grouped_components = get_course_groups(args.course)
    if not grouped_components:
        print(f"❌ No components found for course {args.course}")
        return
    vcenter_host = VCENTER_MAP.get(args.host.lower())
    if not vcenter_host:
        print(f"❌ Host '{args.host}' is not mapped to any vCenter in VCENTER_MAP.")
        return
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        si = SmartConnect(
            host=f"{vcenter_host}.rededucation.com",
            user="administrator@vcenter.rededucation.com",
            pwd="pWAR53fht786123$"
        )
        print(f"✅ Connected to vCenter: {vcenter_host}.rededucation.com")
    except Exception as e:
        print(f"❌ Failed to connect to vCenter: {e}")
        return
    power_map = get_vm_power_map_for_class(args.classnum, si)

    vr_state = {
        "checked": False,
        "first_pod": args.start
    }

    for pod in range(args.start, args.end + 1):
        run_outside_checks(pod, grouped_components, ssh_target, args.host, args.classnum, vr_state)

    try:
        child = pexpect.spawn(f"ssh {ssh_target}", timeout=30)
        child.expect(r"#\s*$")
        print(f"✅ SSH to {ssh_target} successful")
        for pod in range(args.start, args.end + 1):
            run_ssh_checks(pod, grouped_components, ssh_target, args.host, child, args.classnum)
        child.sendline("exit")
        child.close()
    except Exception as e:
        print(f"❌ SSH session to {ssh_target} failed: {e}")

    print_final_power_status(power_map, args.classnum)
    Disconnect(si)

    host_abbr = HOST_ABBR.get(args.host.lower())
    if host_abbr:
        print("\n▶️ Running testf5 script...")
        try:
            subprocess.run([
                "testf5",
                str(args.start),
                str(args.end),
                str(args.classnum),
                host_abbr
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ testf5 execution failed: {e}")
    else:
        print(f"⚠️ Could not resolve host abbreviation for '{args.host}' to run testf5.")



if __name__ == "__main__":
    main()
