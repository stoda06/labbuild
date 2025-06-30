#!/usr/bin/env python3.10

import argparse
import re
import pexpect
import sys
from pymongo import MongoClient
from tabulate import tabulate

VERBOSE = False

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def strip_ansi(text):
    return ANSI_ESCAPE.sub('', text)

def log(msg):
    if VERBOSE:
        print(f"[DEBUG] {msg}")

def get_course_components(course_name):
    try:
        mongo_uri = "mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db"
        client = MongoClient(mongo_uri)
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}")

        if not doc or "components" not in doc:
            print(f"❌ No components found for course: {course_name}")
            return []

        components = []
        for c in doc["components"]:
            name = c.get("component_name")
            ip = c.get("podip")
            port = c.get("podport")
            if name and ip and port:
                components.append((name, ip, port))
                log(f"Component parsed: {name}, IP: {ip}, Port: {port}")

        return components

    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return []

def resolve_ip(ip_template, pod, host_label):
    host_label = host_label.lower().strip()

    if "+X" in ip_template:
        base = ip_template.replace("+X", "")
        parts = base.split(".")
        if len(parts) != 4:
            log(f"Invalid IP template after +X removal: {ip_template}")
            return ip_template
        parts[-1] = str(int(parts[-1]) + pod)
        ip_template = ".".join(parts)

    log(f"IP after +X resolution: {ip_template}")

    if host_label == "hotshot":
        parts = ip_template.split(".")
        if len(parts) == 4 and parts[0] == "172":
            original_octet = parts[1]
            parts[1] = "26"
            new_ip = ".".join(parts)
            log(f"Adjusted IP from 172.{original_octet} to 172.26 -> {new_ip}")
            return new_ip

    return ip_template

def run_cluster_status(child, label, ip):
    print(f"\n🧪 Checking cluster status for {label} ({ip})")
    child.sendline(f"ssh nutanix@{ip}")
    child.expect("password")
    child.sendline("nutanix/4u")
    child.expect(r"nutanix@.*:\~\$")

    child.sendline("cluster status")
    child.expect(r"nutanix@.*:\~\$", timeout=60)
    output = strip_ansi(child.before.decode())

    lines = output.splitlines()
    down_services = []

    for line in lines:
        line = line.strip()
        if line.endswith("DOWN") or "DOWN\t" in line or "DOWN [" in line:
            parts = re.split(r'\s{2,}|\t+', line)
            if len(parts) >= 1:
                component = parts[0].strip()
                down_services.append([component, "DOWN"])

    child.sendline("exit")
    child.expect(r"#\s*$")

    if down_services:
        print(f"\n📉 {label.upper()} - Services NOT UP")
        print(tabulate(down_services, headers=["Component", "Status"], tablefmt="fancy_grid"))
        return (label.upper(), ip, "DOWN")
    else:
        print(f"\n✅ {label.upper()} - All services are UP")
        return (label.upper(), ip, "UP")

def run_ssh_checks(pod, components, host):
    host_fqdn = f"nuvr{pod}.us"
    print(f"\n🔐 Connecting to {host_fqdn} via SSH...")
    results = []

    try:
        child = pexpect.spawn(f"ssh {host_fqdn}", timeout=30)
        child.expect(r"#\s*$")
        log(f"Connected to {host_fqdn}")
        print(f"✅ Connected to {host_fqdn}")

        total = len(components)
        progress = 0

        for name, raw_ip, port in components:
            progress += 1

            bar_length = 30
            filled = int(bar_length * progress // total)
            bar = "#" * filled + " " * (bar_length - filled)
            sys.stdout.write(f"\r[{bar}] {progress}/{total} Checking: {name}   ")
            sys.stdout.flush()

            ip = resolve_ip(raw_ip, pod, host)
            status = "UNKNOWN"

            if VERBOSE:
                print(f"\n🔎 Running nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                print(f"✅ Using resolved IP for {name}: {ip}")

            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                cmd = f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1"
                child.sendline(cmd)
                child.expect(r"#\s*$")
                iface = strip_ansi(child.before.decode()).strip().splitlines()[-1]
                if iface:
                    cmd = f"arping -c 3 -I {iface} {ip}"
                    if VERBOSE:
                        print(f"📶 Running {cmd}")
                    child.sendline(cmd)
                    child.expect(r"#\s*$", timeout=15)
                    out = strip_ansi(child.before.decode())
                    status = "UP" if "Unicast reply" in out else "DOWN"
                    log(out)
                else:
                    print(f"⚠️ No interface found for {ip}")
            else:
                cmd = f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'"
                child.sendline(cmd)
                child.expect(r"#\s*$", timeout=20)
                out = strip_ansi(child.before.decode())
                status = "UP" if "open" in out else "DOWN"
                log(out)

            results.append([name, ip, host_fqdn, port, status])

        print()  # newline after progress bar

        # Now do cluster checks
        cluster1 = run_cluster_status(child, "cluster1", "192.168.1.12")
        cluster2 = run_cluster_status(child, "cluster2", "192.168.1.22")
        cluster_rows = [
            ["cluster1", cluster1[1], "cluster1", "N/A", cluster1[2]],
            ["cluster2", cluster2[1], "cluster2", "N/A", cluster2[2]],
        ]

        # Put cluster results at the top
        results = cluster_rows + results

        child.sendline("exit")
        child.close()

    except Exception as e:
        print(f"\n❌ SSH to {host_fqdn} failed: {e}")

    return results

def main(argv=None):
    global VERBOSE
    parser = argparse.ArgumentParser(description="NU Pod Network and Cluster Checker")
    parser.add_argument("-g", "--course", required=True, help="Course name")
    parser.add_argument("--host", required=True, help="Target host label")
    parser.add_argument("-v", "--vendor", help="Vendor name")
    parser.add_argument("-s", "--start", type=int, required=True, help="Start pod number")
    parser.add_argument("-e", "--end", type=int, required=True, help="End pod number")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("-c", "--component", help="Test specific components (comma-separated list).")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose

    print(f"[INFO] Host argument received: '{args.host}'")
    print(f"📘 Fetching components for course: {args.course}")
    components = get_course_components(args.course)
    
    if args.component:
        selected_components = [c.strip() for c in args.component.split(',')]
        original_count = len(components)
        components = [c for c in components if c[0] in selected_components]
        print(f"\n🔎 Filtering: Selected {len(components)} of {original_count} components based on user input.")

    if not components:
        print(f"❌ No usable components found (or matched filter). Exiting.")
        return

    full_results = []
    for pod in range(args.start, args.end + 1):
        pod_results = run_ssh_checks(pod, components, args.host)
        full_results.extend(pod_results)

    if full_results:
        print("\n📊 Network Check Summary")
        print(tabulate(full_results, headers=["Component", "Component IP", "NU Pod", "Port", "Status"], tablefmt="fancy_grid"))

if __name__ == "__main__":
    main()