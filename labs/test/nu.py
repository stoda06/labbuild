#!/usr/bin/env python3.10

import argparse, re, pexpect, sys, threading
from pymongo import MongoClient
from tabulate import tabulate

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED = '\033[91m'
ENDC = '\033[0m'

def strip_ansi(text): return ANSI_ESCAPE.sub('', text)
def log(msg, print_lock):
    if VERBOSE:
        with print_lock: print(f"[DEBUG] {msg}")

def get_course_components(course_name, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:%24%24u1QBd6%26372%23%24rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        doc = collection.find_one({"course_name": course_name})
        log(f"MongoDB raw result: {doc}", print_lock)
        if not doc or "components" not in doc:
            with print_lock:
                print(f"❌ No components found for course: {course_name}")
            return []
        components = []
        for c in doc["components"]:
            component_name, name, ip, port = c.get("component_name"), c.get("clone_name"), c.get("podip"), c.get("podport")
            if component_name and name and ip and port:
                components.append((component_name, name, ip, port))
                log(f"Component parsed: {component_name}, Name: {name}, IP: {ip}, Port: {port}", print_lock)
        return components
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB error: {e}")
        return []

def resolve_ip(ip_template, pod, host_label, print_lock):
    host_label = host_label.lower().strip()
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
    if host_label in ["hotshot", "trypticon"]:
        parts = ip_template.split(".")
        if len(parts) == 4 and parts[0] == "172":
            new_ip = ".".join(["172", "26", parts[2], parts[3]])
            log(f"Adjusted IP from 172.{parts[1]} to 172.26 for host {host_label} -> {new_ip}", print_lock)
            return new_ip
    return ip_template

def run_cluster_status(child, label, ip, pod, print_lock):
    with print_lock:
        print(f"\n🧪 Checking cluster status for {label} ({ip}) on pod {pod}")
    results = []
    try:
        child.sendline(f"ssh nutanix@{ip}")
        child.expect("password")
        child.sendline("nutanix/4u")
        child.expect(r"nutanix@.*:\~\$")
        child.sendline("cluster status")
        child.expect(r"nutanix@.*:\~\$", timeout=60)
        output = strip_ansi(child.before.decode())
        
        down_services = []
        for line in output.splitlines():
            u = line.upper()
            if u.strip().endswith("DOWN") or "DOWN\t" in u or "DOWN [" in u:
                component = re.split(r'\s{2,}|\t+', line.strip())[0].strip()
                results.append({'pod': pod, 'component': f"{label}-{component}", 'ip': ip, 'port': 'N/A', 'status': 'DOWN'})

        if not any(r['status'] == 'DOWN' for r in results):
            results.append({'pod': pod, 'component': label, 'ip': ip, 'port': 'N/A', 'status': 'UP'})

        child.sendline("exit")
        child.expect(r"#\s*$")
    except Exception as e:
        with print_lock:
            print(f"Error checking cluster {label} on pod {pod}: {e}")
        results.append({'pod': pod, 'component': label, 'ip': ip, 'port': 'N/A', 'status': 'FAILED'})

    with print_lock:
        if any(r['status'] == 'DOWN' for r in results):
            print(f"📉 {label.upper()} on pod {pod} - Services NOT UP")
        else:
            print(f"✅ {label.upper()} on pod {pod} - All services are UP")
    return results

def run_ssh_checks(pod, components, host, print_lock):
    host_fqdn = f"nuvr{pod}.us" if host.lower() in ["hotshot", "trypticon"] else f"nuvr{pod}"
    results = []
    with print_lock:
        print(f"\n🔐 Connecting to {host_fqdn} via SSH...")
    try:
        child = pexpect.spawn(f"ssh {host_fqdn}", timeout=30)
        child.expect(r"#\s*$")
        with print_lock:
            print(f"✅ Connected to {host_fqdn}")
        
        for component_name, raw_clone_name, raw_ip, port in components:
            ip = resolve_ip(raw_ip, pod, host, print_lock)
            clone_name = raw_clone_name.replace('{X}', str(pod))
            status = "UNKNOWN"
            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                iface_cmd = f"ip -o addr show | grep '{subnet}\\.' | awk '{{print $2}}'"
                log(f"Executing interface lookup: {iface_cmd}", print_lock)
                child.sendline(iface_cmd)
                child.expect(r"#\s*$")
                
                iface_raw_output = strip_ansi(child.before.decode())
                lines = iface_raw_output.strip().splitlines()
                iface = ""
                command_sent_found = False
                for line in lines:
                    if iface_cmd in line:
                        command_sent_found = True
                        continue
                    if command_sent_found and line.strip():
                        iface = line.strip()
                        break
                
                log(f"Interface lookup for subnet {subnet}: raw='{iface_raw_output}', cleaned='{iface}'", print_lock)

                if iface:
                    arp_cmd = f"arping -c 3 -I {iface} {ip}"
                    log(f"Executing arping command: {arp_cmd}", print_lock)
                    child.sendline(arp_cmd)
                    child.expect(r"#\s*$", timeout=15)
                    arping_output = child.before.decode()
                    log(f"Arping raw output for {ip}:\n---\n{arping_output}\n---", print_lock)
                    status = "UP" if "unicast reply" in arping_output.lower() else "DOWN"
                else:
                    log(f"Skipping arping for {ip} because no interface was found.", print_lock)
                    status = "FAILED"
            else:
                child.sendline(f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                child.expect(r"#\s*$", timeout=20)
                nmap_output = child.before.decode()
                status = "UP" if "open" in nmap_output.lower() else "DOWN"
            results.append({'pod': pod, 'component': clone_name, 'ip': ip, 'port': port, 'status': status, 'host': host_fqdn})
        
        results.extend(run_cluster_status(child, "cluster1", "192.168.1.12", pod, print_lock))
        results.extend(run_cluster_status(child, "cluster2", "192.168.1.22", pod, print_lock))
        
        child.sendline("exit")
        child.close()
    except Exception as e:
        with print_lock:
            print(f"\n❌ SSH to {host_fqdn} failed: {e}")
        results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host_fqdn, 'port': 22, 'status': 'FAILED', 'host': host_fqdn})

    with print_lock:
        print(f"\n📊 Network & Cluster Check Summary for Pod {pod}")
        headers=["Component", "Component IP", "NU Pod", "Port", "Status"]
        table_data = [[r['component'], r.get('ip', 'N/A'), r.get('host', host_fqdn), r.get('port', 'N/A'), r['status']] for r in results]
        formatted_rows = [[f"{RED}{cell}{ENDC}" if row[4] not in ['UP', 'SUCCESS'] else cell for cell in row] for row in table_data]
        print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))

    return results

def main(argv=None, print_lock=None):
    if print_lock is None:
        print_lock = threading.Lock()
    global VERBOSE
    parser = argparse.ArgumentParser(description="NU Pod Network and Cluster Checker")
    parser.add_argument("-g", "--course", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("-c", "--component", help="Test specific components.")
    args = parser.parse_args(argv)
    VERBOSE = args.verbose
    
    with print_lock:
        print(f"📘 Fetching components for course: {args.course}")
    components = get_course_components(args.course, print_lock)
    if args.component:
        selected = [c.strip() for c in args.component.split(',')]
        components = [c for c in components if c[0] in selected]
    
    if not components:
        with print_lock:
            print(f"❌ No usable components found (or matched filter). Exiting.")
        return []

    all_results = []
    for pod in range(args.start, args.end + 1):
        pod_results = run_ssh_checks(pod, components, args.host, print_lock)
        all_results.extend(pod_results)
    
    return all_results

if __name__ == "__main__":
    main()
