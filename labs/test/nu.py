#!/usr/bin/env python3

import logging  # <--- THIS IS THE FIX
import argparse
import re
import pexpect
import sys
import threading
from pymongo import MongoClient
from tabulate import tabulate
from typing import Dict, Any, Optional, List

VERBOSE = False
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
RED = '\033[91m'
ENDC = '\033[0m'

# Use a consistent logger name
logger = logging.getLogger('labbuild.test.nu')

def strip_ansi(text): return ANSI_ESCAPE.sub('', text)
def log(msg, print_lock):
    if VERBOSE:
        # Use the logger instead of print for better integration
        with print_lock:
            logger.debug(msg)

def get_course_components(course_name, print_lock):
    try:
        # Note: Hardcoding connection details is not recommended for production.
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
    finally:
        if 'client' in locals() and client:
            client.close()

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
    
    # --- DYNAMIC PROMPT FIX ---
    possible_prompts = [
        r"nutanix@.*:~\$",       # Old prompt
        r"\[root@.* ~\]#",       # New prompt
        r"password:",           # Password prompt
        pexpect.TIMEOUT,
        pexpect.EOF
    ]
    
    try:
        child.sendline(f"ssh nutanix@{ip}")
        index = child.expect(possible_prompts)

        if index == 2: # Matched password prompt
            child.sendline("nutanix/4u")
            index = child.expect(possible_prompts)

        if index == 0 or index == 1: # Matched nutanix or root prompt
            child.sendline("cluster status")
            child.expect(possible_prompts, timeout=90) # Increased timeout
            output = strip_ansi(child.before.decode())
            
            # More reliable check for cluster state
            is_up = "The state of the cluster: start" in output

            if is_up:
                results.append({'pod': pod, 'component': label, 'ip': ip, 'port': 'N/A', 'status': 'UP', 'test_status': 'success'})
            else:
                down_services = [line.strip().split()[0] for line in output.splitlines() if "DOWN" in line.upper()]
                error_msg = f"Services DOWN: {', '.join(down_services)}" if down_services else "Cluster not in 'start' state."
                results.append({'pod': pod, 'component': label, 'ip': ip, 'port': 'N/A', 'status': 'DOWN', 'error': error_msg, 'test_status': 'failed'})

            child.sendline("exit")
            child.expect(r"#\s*$")
        else:
             raise Exception("Failed to get expected shell prompt after login.")

    except Exception as e:
        with print_lock:
            print(f"Error checking cluster {label} on pod {pod}: {e}")
        results.append({'pod': pod, 'component': label, 'ip': ip, 'port': 'N/A', 'status': 'FAILED', 'error': str(e), 'test_status': 'failed'})

    with print_lock:
        if any(r['test_status'] == 'failed' for r in results):
            print(f"📉 {label.upper()} on pod {pod} - Checks FAILED")
        else:
            print(f"✅ {label.upper()} on pod {pod} - All services are UP")
    return results

def run_ssh_checks(pod, components, host, print_lock):
    host_fqdn = f"nuvr{pod}.us" if host.lower() in ["hotshot", "trypticon"] else f"nuvr{pod}.au"
    results = []
    with print_lock:
        print(f"\n🔐 Connecting to {host_fqdn} via SSH...")
    
    # --- DYNAMIC PROMPT FIX FOR MAIN SSH ---
    possible_prompts = [r"\[root@.* ~\]#", pexpect.TIMEOUT, pexpect.EOF]
    
    child = None
    try:
        child = pexpect.spawn(f"ssh {host_fqdn}", timeout=30)
        index = child.expect(possible_prompts)
        
        if index != 0:
            raise Exception(f"Failed to connect or get prompt. Reason: {possible_prompts[index]}")

        with print_lock:
            print(f"✅ Connected to {host_fqdn}")
        
        for component_name, raw_clone_name, raw_ip, port in components:
            ip = resolve_ip(raw_ip, pod, host, print_lock)
            clone_name = raw_clone_name.replace('{X}', str(pod))
            status, test_status = "UNKNOWN", "failed"

            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                iface_cmd = f"ip -o addr show | grep '{subnet}\\.' | awk '{{print $2}}'"
                log(f"Executing interface lookup: {iface_cmd}", print_lock)
                child.sendline(iface_cmd)
                child.expect(possible_prompts[0]) # Expect the root prompt
                iface_raw_output = strip_ansi(child.before.decode())
                lines = [line for line in iface_raw_output.strip().splitlines() if iface_cmd not in line and line.strip()]
                iface = lines[0].strip() if lines else ""
                log(f"Interface lookup for subnet {subnet}: raw='{iface_raw_output}', cleaned='{iface}'", print_lock)

                if iface:
                    arp_cmd = f"arping -c 3 -I {iface} {ip}"
                    log(f"Executing arping command: {arp_cmd}", print_lock)
                    child.sendline(arp_cmd)
                    child.expect(possible_prompts[0], timeout=15)
                    arping_output = child.before.decode()
                    log(f"Arping raw output for {ip}:\n---\n{arping_output}\n---", print_lock)
                    if "unicast reply" in arping_output.lower():
                        status, test_status = "UP", "success"
                    else:
                        status, test_status = "DOWN", "failed"
                else:
                    log(f"Skipping arping for {ip} because no interface was found.", print_lock)
                    status, test_status = "FAILED", "failed"
            else:
                child.sendline(f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                child.expect(possible_prompts[0], timeout=20)
                nmap_output = child.before.decode()
                if "open" in nmap_output.lower():
                    status, test_status = "UP", "success"
                else:
                    status, test_status = "DOWN", "failed"
            results.append({'pod': pod, 'component': clone_name, 'ip': ip, 'port': port, 'status': status, 'test_status': test_status, 'host': host_fqdn})
        
        results.extend(run_cluster_status(child, "cluster1", "192.168.1.12", pod, print_lock))
        results.extend(run_cluster_status(child, "cluster2", "192.168.1.22", pod, print_lock))
        
    except Exception as e:
        with print_lock:
            print(f"\n❌ SSH to {host_fqdn} failed: {e}")
        results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host_fqdn, 'port': 22, 'status': 'FAILED', 'test_status': 'failed', 'host': host_fqdn})
    finally:
        if child and child.isalive():
            child.sendline("exit")
            child.close()

    with print_lock:
        print(f"\n📊 Network & Cluster Check Summary for Pod {pod}")
        headers=["Component", "Component IP", "NU Pod", "Port", "Status"]
        table_data = [[r['component'], r.get('ip', 'N/A'), r.get('host', host_fqdn), r.get('port', 'N/A'), r['status']] for r in results]
        # Colorize the row based on the 'test_status' key
        formatted_rows = [[f"{RED}{cell}{ENDC}" if row_data['test_status'] != 'success' else cell for cell in row] for row, row_data in zip(table_data, results)]
        print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))

    return results

def main(argv: List[str], print_lock: Optional[threading.Lock] = None) -> List[Dict[str, Any]]:
    if print_lock is None:
        print_lock = threading.Lock()
    global VERBOSE
    
    # --- FRAMEWORK COMPATIBILITY FIX ---
    parser = argparse.ArgumentParser(description="NU Pod Network and Cluster Checker")
    parser.add_argument("-g", "--group", required=True, help="Course name (group)")
    parser.add_argument("--host", required=True)
    parser.add_argument("-s", "--start-pod", type=int, required=True)
    parser.add_argument("-e", "--end-pod", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("-c", "--component", help="Test specific components.")
    # --- END FIX ---
    
    args = parser.parse_args(argv)
    VERBOSE = args.verbose
    
    with print_lock:
        print(f"📘 Fetching components for course: {args.group}")
    components = get_course_components(args.group, print_lock)
    if args.component:
        selected = [c.strip() for c in args.component.split(',')]
        components = [c for c in components if c[0] in selected]
    
    if not components:
        with print_lock:
            print(f"❌ No usable components found (or matched filter). Exiting.")
        return []

    all_results = []
    for pod in range(args.start_pod, args.end_pod + 1):
        pod_results = run_ssh_checks(pod, components, args.host, print_lock)
        all_results.extend(pod_results)
    
    return all_results

if __name__ == "__main__":
    # This allows running the script directly for debugging
    main(sys.argv[1:])