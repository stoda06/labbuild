#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate
import ssl, argparse, sys, pexpect, concurrent.futures, threading, re
from pymongo import MongoClient
from db_utils import get_vcenter_by_host
from collections import defaultdict

context = ssl._create_unverified_context()

RED = '\033[91m'
END = '\033[0m'

def threaded_fn_test_cp(pod, vcenter_host, group, verbose, print_lock):
    try:
        si = SmartConnect(host=vcenter_host, user="administrator@vcenter.rededucation.com", pwd="pWAR53fht786123$", sslContext=context)
        result, power_map, comparison_results = fn_test_cp(pod, si, group, verbose, print_lock)
        Disconnect(si)
        return pod, result, power_map, comparison_results
    except Exception as e:
        with print_lock:
            print(f"❌ Error connecting to vCenter for pod {pod}: {e}")
        return pod, [], {}, []

def compare_vms_with_components(pod, vms_in_pool, course_name, print_lock):
    comparison_results = []
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; collection = db["temp_courseconfig"]
        course_doc = collection.find_one({"course_name": course_name})
        if not course_doc or "components" not in course_doc:
            with print_lock:
                print(f"❌ Unable to fetch components for course '{course_name}'")
            return []
        expected_vms = [comp.get("clone_name", "").replace("{X}", str(pod)) for comp in course_doc["components"] if "{X}" in comp.get("clone_name", "")]
        vm_set, expected_set = set(vms_in_pool), set(expected_vms)
        missing, extra = expected_set - vm_set, vm_set - expected_set

        for name in sorted(missing):
            comparison_results.append({'pod': pod, 'component': name, 'status': 'Missing from pool'})
        for name in sorted(extra):
            comparison_results.append({'pod': pod, 'component': name, 'status': 'Unexpected in pool'})

        if not missing and not extra:
            with print_lock:
                print(f"\n✅ All expected components are present for Pod {pod}")
            return []

        diff_rows = [["Missing from pool", name] for name in sorted(missing)] + [["Unexpected in pool", name] for name in sorted(extra)]
        with print_lock:
            print(f"\n📌 VM vs Component Check for Pod {pod}")
            print(tabulate(diff_rows, headers=["Status", "VM Name"], tablefmt="fancy_grid"))
        
        return comparison_results
    except Exception as e:
        with print_lock:
            print(f"❌ Error during VM comparison for pod {pod}: {e}")
        return [{'pod': pod, 'component': 'VM Comparison', 'status': 'FAILED', 'error': str(e)}]

def get_entity_by_name(content, name, vimtype):
    container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
    for entity in container.view:
        if entity.name == name:
            container.Destroy()
            return entity
    container.Destroy()
    return None

def get_permissions_for_user(entity, username): return [perm for perm in entity.permission if username.lower() in perm.principal.lower()]
def get_role_name(content, role_id):
    for role in content.authorizationManager.roleList:
        if role.roleId == role_id: return role.name
    return None
def get_vms_count_in_folder(folder): return len([vm for vm in folder.childEntity if isinstance(vm, vim.VirtualMachine)])
def get_vms_count_in_resource_pool(resource_pool, verbose, print_lock):
    try:
        results = [(vm.name, vm.runtime.powerState) for vm in resource_pool.vm]
        if verbose:
            with print_lock:
                print(f"📦 Resource Pool '{resource_pool.name}' VM Status:")
                for name, power in results: print(f"   - {name} [{power}]")
        return results
    except Exception as e:
        with print_lock:
            print(f"⚠️ Failed to retrieve VMs: {e}")
        return []

def check_entity_permissions_and_vms(pod, content, entity_name, entity_type, username, verbose, print_lock):
    result_rows, vm_names, vm_power_map = [], [], {}
    entity = get_entity_by_name(content, entity_name, entity_type)
    if entity:
        perms = get_permissions_for_user(entity, username)
        if perms:
            for perm in perms:
                role_name, propagate = get_role_name(content, perm.roleId), "Yes" if perm.propagate else "No"
                error = "" if perm.propagate else "Permissions not set to propagate"
                if isinstance(entity, vim.Folder): vm_count, vm_names = get_vms_count_in_folder(entity), []
                else:
                    vm_data = get_vms_count_in_resource_pool(entity, verbose, print_lock)
                    vm_names, vm_power_map, vm_count = [n for n, _ in vm_data], {n: p for n, p in vm_data}, len(vm_data)
                result_rows.append([ "Folder" if isinstance(entity, vim.Folder) else "ResourcePool", entity_name, username, role_name, propagate, vm_count, error])
        else: result_rows.append([entity_type[0].__name__, entity_name, username, "-", "-", 0, "No roles found"])
    else: result_rows.append([entity_type[0].__name__, entity_name, username, "-", "-", 0, "Entity not found"])
    return result_rows, vm_names, vm_power_map

def fn_test_cp(pod, si, group, verbose, print_lock):
    content = si.RetrieveContent()
    username, rp_name, folder_name = f"labcp-{pod}", f"cp-pod{pod}", f"cp-pod{pod}-folder"
    table_data = []
    rows_rp, vms, power_map = check_entity_permissions_and_vms(pod, content, rp_name, [vim.ResourcePool], username, verbose, print_lock)
    rows_folder, _, _ = check_entity_permissions_and_vms(pod, content, folder_name, [vim.Folder], username, verbose, print_lock)
    table_data.extend(rows_rp + rows_folder)
    comparison_results = compare_vms_with_components(pod, vms, group, print_lock)
    return table_data, power_map, comparison_results

def fetch_course_config(pod, group, verbose, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; collection = db["temp_courseconfig"]
        course = collection.find_one({"course_name": group})
        if not course or "components" not in course:
            with print_lock:
                print(f"❌ No components found for course '{group}'")
            return None, [], []
        
        all_components = course.get("components", [])
        
        testable_components = []
        ignore_list = []
        for c in all_components:
            if c.get("state") == "poweroff":
                if c.get("podip") and c.get("podport"):
                    ignore_list.append((c["component_name"], c["podip"], c["podport"]))
            elif c.get("podip") and c.get("podport"):
                testable_components.append((c["component_name"], c["podip"], c["podport"]))
        
        return all_components, testable_components, ignore_list
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB query failed: {e}")
        return None, [], []

def resolve_pod_ip(ip_raw, pod):
    if "+X" in ip_raw:
        base, _, offset = ip_raw.partition("+X"); octets = base.split(".")
        octets[-1] = str(int(octets[-1]) + pod); return ".".join(octets)
    return ip_raw

def perform_network_checks_over_ssh(pod, all_course_components, testable_components, ignore_list, vm_power_map, comparison_results, verbose, print_lock):
    # --- DYNAMIC VR HOSTNAME LOGIC ---
    vr_component = next((c for c in all_course_components if 'vr' in c.get('component_name', '').lower()), None)
    if not vr_component:
        with print_lock: print(f"❌ VR component not found in course config for pod {pod}. Cannot perform SSH checks.")
        # Return existing comparison results plus this new error
        comparison_results.append({'pod': pod, 'component': 'SSH Prerequisite', 'status': 'VR component config not found'})
        return comparison_results
    
    ssh_host = vr_component.get('clone_name', '').replace('{X}', str(pod))
    # --- END DYNAMIC LOGIC ---

    ssh_prompt_pattern = r"(?:\[root@pod-vr ~\]# |vr:~# )"
    
    check_results = comparison_results[:]

    for name, raw_ip, port in ignore_list:
        ip = resolve_pod_ip(raw_ip, pod)
        check_results.append({'pod': pod, 'component': name, 'ip': ip, 'port': port, 'status': 'SKIPPED (Powered Off)', 'host': ssh_host})

    # --- THIS IS THE CRITICAL FIX ---
    # The entire SSH process is wrapped in a try/except block.
    # If it fails at any point, it appends the error and returns the *full* list of results.
    try:
        if verbose:
            with print_lock:
                print(f"\n🔐 Connecting to {ssh_host} via SSH...")
        
        # Only attempt SSH if there are components to test.
        if not testable_components:
             return check_results

        child = pexpect.spawn(f"ssh {ssh_host}", timeout=30, encoding='utf-8')
        login_patterns = [r"Are you sure you want to continue connecting.*", ssh_prompt_pattern, r"[Pp]assword:", pexpect.EOF, pexpect.TIMEOUT]
        i = child.expect(login_patterns, timeout=20)

        if i == 0:
            child.sendline("yes")
            child.expect(ssh_prompt_pattern)
        elif i == 1:
            if verbose:
                with print_lock: print(f"✅ SSH to {ssh_host} successful")
        else:
            # This handles failed login (timeout, EOF, password prompt)
            raise pexpect.exceptions.ExceptionPexpect(f"Failed to connect to {ssh_host}. Reason: {child.before.strip()}")
        
        for name, raw_ip, port in testable_components:
            ip = resolve_pod_ip(raw_ip, pod)
            status = "UNKNOWN"
            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                child.expect(ssh_prompt_pattern)
                output_lines = child.before.strip().splitlines()
                iface = output_lines[-1].strip() if output_lines and output_lines[-1].strip() else None
                if iface:
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    child.expect(ssh_prompt_pattern)
                    status = "UP" if "Unicast reply" in child.before else "DOWN"
                else:
                    status = "DOWN (iface not found)"
            elif port.lower() == "ping":
                child.sendline(f"ping -c 3 -W 2 {ip}")
                child.expect(ssh_prompt_pattern, timeout=15)
                output = child.before
                match = re.search(r"(\d+)\s+packets\s+transmitted,\s+(\d+)\s+(received|packets\s+received)", output)
                status = "UP" if match and int(match.group(2)) > 0 else "DOWN"
            else:
                child.sendline(f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                child.expect(ssh_prompt_pattern)
                status = "UP" if "open" in child.before else "DOWN"
            check_results.append({'pod': pod, 'component': name, 'ip': ip, 'port': port, 'status': status, 'host': ssh_host})
        
        child.sendline("exit")
        child.expect(pexpect.EOF)
        child.close()

    except Exception as e:
        with print_lock: print(f"❌ SSH or command execution failed on {ssh_host}: {e}")
        # **THE FIX**: APPEND the failure to the existing results.
        check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': ssh_host, 'port': 22, 'status': 'FAILED', 'error': str(e)})
        # Return the combined list of all errors found so far.
        return check_results
    # --- END OF CRITICAL FIX ---


    with print_lock:
        if check_results:
            displayable_results = [r for r in check_results if 'ip' in r and 'port' in r]
            if displayable_results:
                print(f"\n📊 Network Test Summary for Pod {pod}")
                headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
                table_data = [[r['component'], r['ip'], r['host'], r['port'], r['status']] for r in displayable_results]
                formatted_rows = [[f"{RED}{cell}{END}" if row[4] not in ["UP", "SKIPPED (Powered Off)"] else cell for cell in row] for row in table_data]
                print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))
        
        powered_off_vms = [[name, str(state)] for name, state in vm_power_map.items() if state == vim.VirtualMachinePowerState.poweredOff]
        if powered_off_vms:
            print(f"\n🔌 VM Power State Summary for Pod {pod}")
            print(tabulate(powered_off_vms, headers=["VM Name", "Power State"], tablefmt="grid"))

    return check_results

def main(argv=None, print_lock=None):
    if print_lock is None:
        print_lock = threading.Lock()

    parser = argparse.ArgumentParser(description="Check CP pod permissions, fetch configs, and run network tests.", prog='checkpoint.py')
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    parser.add_argument("-H", "--host", required=True)
    parser.add_argument("-g", "--group", required=True)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-c", "--component", help="Test specific components.")
    args = parser.parse_args(argv)

    vcenter_fqdn = get_vcenter_by_host(args.host)
    if not vcenter_fqdn:
        with print_lock:
            print(f"❌ Could not find vCenter for host '{args.host}' in the database.")
        sys.exit(1)
    
    pod_range = list(range(args.start, args.end + 1))
    power_states = {}
    all_comparison_results = defaultdict(list)
    all_check_results = []
    
    with print_lock:
        print(f"\n🌐 Connecting to vCenter: {vcenter_fqdn}")
    
    full_table_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(threaded_fn_test_cp, pod, vcenter_fqdn, args.group, args.verbose, print_lock) for pod in pod_range]
        for future in concurrent.futures.as_completed(futures):
            pod, table_rows, power_map, comparison_results = future.result()
            full_table_data.extend(table_rows)
            power_states[pod] = power_map
            all_comparison_results[pod].extend(comparison_results)

    with print_lock:
        print("\n📊 Permissions and VM Count Summary\n")
        print(tabulate(full_table_data, headers=["Entity Type", "Entity Name", "Username", "Role", "Propagate", "VM Count", "Error"], tablefmt="fancy_grid"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for pod in pod_range:
            all_course_components, testable_components, ignore_list = fetch_course_config(pod, args.group, args.verbose, print_lock)
            
            # If config fetch fails, we still need to pass through any existing comparison errors
            if all_course_components is None:
                all_check_results.extend(all_comparison_results.get(pod, []))
                continue

            if args.component:
                selected_components = [c.strip() for c in args.component.split(',')]
                testable_components = [c for c in testable_components if c[0] in selected_components]
                if not testable_components:
                    with print_lock: print(f"   - No matching components found for pod {pod}. Skipping network checks.")
                    all_check_results.extend(all_comparison_results.get(pod, []))
                    continue
            
            future = executor.submit(perform_network_checks_over_ssh, 
                                     pod, all_course_components, testable_components, ignore_list, 
                                     power_states.get(pod, {}), 
                                     all_comparison_results.get(pod, []),
                                     args.verbose, print_lock)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            all_check_results.extend(future.result())

    return all_check_results

if __name__ == "__main__":
    main()