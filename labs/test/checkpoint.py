#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate
import ssl, argparse, sys, pexpect, concurrent.futures, threading, re
from pymongo import MongoClient
from db_utils import get_vcenter_by_host

context = ssl._create_unverified_context()

RED = '\033[91m'
ORANGE = '\033[93m'
END = '\033[0m'

def threaded_fn_test_cp(pod, vcenter_host, group, verbose, print_lock):
    try:
        si = SmartConnect(host=vcenter_host, user="administrator@vcenter.rededucation.com", pwd="pWAR53fht786123$", sslContext=context)
        result, power_map, missing_vm_errors = fn_test_cp(pod, si, group, verbose, print_lock)
        Disconnect(si)
        return pod, result, power_map, missing_vm_errors
    except Exception as e:
        with print_lock:
            print(f"❌ Error connecting to vCenter for pod {pod}: {e}")
        return pod, [], {}, []

def compare_vms_with_components(pod, vms_in_pool, course_name, print_lock):
    missing_vm_errors = []
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; collection = db["temp_courseconfig"]
        course_doc = collection.find_one({"course_name": course_name})
        if not course_doc or "components" not in course_doc:
            with print_lock:
                print(f"❌ Unable to fetch components for course '{course_name}'")
            return missing_vm_errors
        expected_vms = [comp.get("clone_name", "").replace("{X}", str(pod)) for comp in course_doc["components"] if "{X}" in comp.get("clone_name", "")]
        vm_set, expected_set = set(vms_in_pool), set(expected_vms)
        missing, extra = expected_set - vm_set, vm_set - expected_set

        if not missing and not extra:
            with print_lock:
                print(f"\n✅ All expected components are present for Pod {pod}")
            return missing_vm_errors
        
        for vm_name in sorted(missing):
            missing_vm_errors.append({
                'pod': pod,
                'component': vm_name,
                'status': 'MISSING FROM POOL',
                'ip': 'N/A',
                'port': 'N/A',
            })

        diff_rows = [["Missing from pool", name] for name in sorted(missing)] + [["Unexpected in pool", name] for name in sorted(extra)]
        with print_lock:
            print(f"\n📌 VM vs Component Check for Pod {pod}")
            print(tabulate(diff_rows, headers=["Status", "VM Name"], tablefmt="fancy_grid"))
    except Exception as e:
        with print_lock:
            print(f"❌ Error during VM comparison for pod {pod}: {e}")
    return missing_vm_errors

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
def get_vms_count_in_folder(folder):
    return len([vm for vm in folder.childEntity if isinstance(vm, vim.VirtualMachine)])
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
    
    display_user = username if username else "N/A"
    
    if entity:
        if isinstance(entity, vim.Folder):
            vm_count = get_vms_count_in_folder(entity)
            vm_names = []
        else:
            vm_data = get_vms_count_in_resource_pool(entity, verbose, print_lock)
            vm_names = [n for n, _ in vm_data]
            vm_power_map = {n: p for n, p in vm_data}
            vm_count = len(vm_data)

        if username:
            perms = get_permissions_for_user(entity, username)
            if perms:
                for perm in perms:
                    role_name = get_role_name(content, perm.roleId)
                    propagate = "Yes" if perm.propagate else "No"
                    error = "" if perm.propagate else "Permissions not set to propagate"
                    result_rows.append([entity_type[0].__name__, entity_name, username, role_name, propagate, vm_count, error])
            else:
                result_rows.append([entity_type[0].__name__, entity_name, username, "-", "-", vm_count, "No roles found"])
        else:
            result_rows.append([entity_type[0].__name__, entity_name, display_user, "N/A", "N/A", vm_count, "Permissions check skipped"])
    else:
        result_rows.append([entity_type[0].__name__, entity_name, display_user, "-", "-", 0, "Entity not found"])
        
    return result_rows, vm_names, vm_power_map

def fn_test_cp(pod, si, group, verbose, print_lock):
    content = si.RetrieveContent()
    is_maestro = 'maestro' in group.lower()
    table_data = []
    
    # For Maestro, we only need the VM list for comparison, not the permissions table.
    if is_maestro:
        rp_name = f"cp-maestro-pod{pod}"
        entity = get_entity_by_name(content, rp_name, [vim.ResourcePool])
        if entity:
            vm_data = get_vms_count_in_resource_pool(entity, verbose, print_lock)
            vms = [n for n, _ in vm_data]
            power_map = {n: p for n, p in vm_data}
        else:
            vms, power_map = [], {}
            with print_lock:
                print(f"❌ Maestro resource pool '{rp_name}' not found for Pod {pod}.")
    # For standard labs, perform all checks and build the summary table data.
    else:
        username, rp_name, folder_name = f"labcp-{pod}", f"cp-pod{pod}", f"cp-pod{pod}-folder"
        rows_rp, vms, power_map = check_entity_permissions_and_vms(pod, content, rp_name, [vim.ResourcePool], username, verbose, print_lock)
        table_data.extend(rows_rp)
        if folder_name:
            rows_folder, _, _ = check_entity_permissions_and_vms(pod, content, folder_name, [vim.Folder], username, verbose, print_lock)
            table_data.extend(rows_folder)
    
    # This check is useful for all lab types.
    missing_vm_errors = compare_vms_with_components(pod, vms, group, print_lock)
    
    # Return empty table_data for Maestro, populated for standard labs.
    return table_data, power_map, missing_vm_errors

def fetch_course_config(pod, group, verbose, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; collection = db["temp_courseconfig"]
        course = collection.find_one({"course_name": group})
        if not course or "components" not in course:
            with print_lock:
                print(f"❌ No components found for course '{group}'")
            return [], []
        components = []
        ignore_list = []
        for c in course["components"]:
            raw_name = c.get("clone_name")
            if c.get("state") == "poweroff":
                if raw_name and c.get("podip") and c.get("podport"):
                    resolved_name = raw_name.replace("{X}", str(pod))
                    ignore_list.append((resolved_name, c["podip"], c["podport"]))
                elif verbose:
                    with print_lock:
                        print(f"⚠️ Skipping incomplete component config (powered off): {c.get('clone_name', 'UNKNOWN')}")
            elif raw_name and c.get("podip") and c.get("podport"):
                resolved_name = raw_name.replace("{X}", str(pod))
                components.append((resolved_name, c["podip"], c["podport"]))
            elif verbose:
                with print_lock:
                    print(f"⚠️ Skipping incomplete component config: {c.get('clone_name', 'UNKNOWN')}")
        return components, ignore_list
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB query failed: {e}")
        return [], []

def resolve_pod_ip(ip_raw, pod, host_key):
    if "+X" in ip_raw:
        base, _, offset = ip_raw.partition("+X")
        octets = base.split(".")
        octets[-1] = str(int(octets[-1]) + pod)
        ip_template = ".".join(octets)
    else:
        ip_template = ip_raw

    if host_key.lower() in ["hotshot", "trypticon"] and ip_template.startswith("172.30."):
        return ip_template.replace("172.30.", "172.26.", 1)

    return ip_template

def perform_network_checks(pod, components, ignore_list, host_key, vm_power_map, verbose, print_lock, is_maestro):
    check_results = []
    for name, raw_ip, port in ignore_list:
        ip = resolve_pod_ip(raw_ip, pod, host_key)
        host_identifier = ip if is_maestro else f"cpvr{pod}"
        check_results.append({'pod': pod, 'component': name, 'ip': ip, 'port': port, 'status': 'SKIPPED (Powered Off)', 'host': host_identifier})

    if components:
        if is_maestro:
            if verbose:
                with print_lock:
                    print(f"\n⚙️ Performing direct network checks for Maestro Pod {pod}...")
            for name, raw_ip, port in components:
                ip = resolve_pod_ip(raw_ip, pod, host_key)
                status = "UNKNOWN"
                nmap_cmd = f"nmap -Pn -p {port} {ip}"
                if verbose:
                    with print_lock:
                        print(f"   -> Executing command for '{name}': {nmap_cmd}")
                try:
                    nmap_output, exit_status = pexpect.run(nmap_cmd, timeout=30, withexitstatus=True, encoding='utf-8')
                    if "open" in nmap_output: status = "UP"
                    elif "filtered" in nmap_output: status = "FILTERED"
                    else: status = "DOWN"
                except pexpect.exceptions.TIMEOUT: status = "DOWN (Timeout)"
                except Exception as e: status = f"ERROR ({e})"
                check_results.append({'pod': pod, 'component': name, 'ip': ip, 'port': port, 'status': status, 'host': ip})
        else:
            host = f"cpvr{pod}.us" if host_key in ["hotshot", "trypticon"] else f"cpvr{pod}"
            ssh_prompt_pattern = r"(?:\[root@pod-vr ~\]# |vr:~# )"
            if verbose:
                with print_lock: print(f"\n🔐 Connecting to {host} via SSH...")
            try:
                child = pexpect.spawn(f"ssh {host}", timeout=30, encoding='utf-8')
                i = child.expect([r"Are you sure you want to continue connecting.*", ssh_prompt_pattern, r"[Pp]assword:", pexpect.EOF, pexpect.TIMEOUT], timeout=20)
                if i == 0: child.sendline("yes"); child.expect(ssh_prompt_pattern)
                elif i == 1:
                    if verbose: 
                        with print_lock: print(f"✅ SSH to {host} successful")
                else:
                    with print_lock: print(f"❌ Failed to connect to {host}. Reason: {child.before}")
                    check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host, 'port': 22, 'status': 'FAILED', 'host': host})
                    return check_results

                for name, raw_ip, port in components:
                    ip = resolve_pod_ip(raw_ip, pod, host_key)
                    status = "UNKNOWN"
                    if port.lower() == "arping":
                        subnet = ".".join(ip.split(".")[:3])
                        child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                        child.expect(ssh_prompt_pattern)
                        output_lines = child.before.strip().splitlines()
                        iface = output_lines[-1].strip() if output_lines and output_lines[-1].strip() else None
                        if iface:
                            arping_cmd = f"arping -c 3 -I {iface} {ip}"
                            if verbose: 
                                with print_lock: print(f"   -> Executing command for '{name}': {arping_cmd}")
                            child.sendline(arping_cmd); child.expect(ssh_prompt_pattern)
                            status = "UP" if "Unicast reply" in child.before else "DOWN"
                        else: status = "DOWN (iface not found)"
                    elif port.lower() == "ping":
                        ping_cmd = f"ping -c 3 -W 2 {ip}"
                        if verbose: 
                            with print_lock: print(f"   -> Executing command for '{name}': {ping_cmd}")
                        child.sendline(ping_cmd); child.expect(ssh_prompt_pattern, timeout=15)
                        match = re.search(r"(\d+)\s+packets\s+transmitted,\s+(\d+)\s+(received|packets\s+received)", child.before)
                        if match: status = "UP" if int(match.group(2)) > 0 else "DOWN"
                        else: status = "DOWN"
                    else:
                        nmap_cmd = f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'"
                        if verbose: 
                            with print_lock: print(f"   -> Executing command for '{name}': {nmap_cmd}")
                        child.sendline(nmap_cmd); child.expect(ssh_prompt_pattern)
                        nmap_output = child.before
                        if "open" in nmap_output: status = "UP"
                        elif "filtered" in nmap_output: status = "FILTERED"
                        else: status = "DOWN"
                    check_results.append({'pod': pod, 'component': name, 'ip': ip, 'port': port, 'status': status, 'host': host})
                child.sendline("exit"); child.expect(pexpect.EOF); child.close()
            except Exception as e:
                with print_lock: print(f"❌ SSH or command execution failed on {host}: {e}")
                check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host, 'port': 22, 'status': 'FAILED', 'host': host})
                return check_results

    with print_lock:
        if check_results:
            print(f"\n📊 Network Test Summary for Pod {pod}")
            headers = ["Component", "Component IP", "Test Host/Target", "Port", "Status"]
            table_data = [[r['component'], r['ip'], r['host'], r['port'], r['status']] for r in check_results]
            formatted_rows = []
            for row in table_data:
                status_cell = row[4]
                if status_cell == "FILTERED": row[4] = f"{ORANGE}{status_cell}{END}"
                elif status_cell not in ["UP", "SKIPPED (Powered Off)"]: row[4] = f"{RED}{status_cell}{END}"
                formatted_rows.append(row)
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
    
    is_maestro = 'maestro' in args.group.lower()

    vcenter_fqdn = get_vcenter_by_host(args.host)
    if not vcenter_fqdn:
        with print_lock:
            print(f"❌ Could not find vCenter for host '{args.host}' in the database.")
        sys.exit(1)

    pod_range, power_states = list(range(args.start, args.end + 1)), {}
    all_check_results = []

    with print_lock:
        print(f"\n🌐 Connecting to vCenter: {vcenter_fqdn}")

    full_table_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(threaded_fn_test_cp, pod, vcenter_fqdn, args.group, args.verbose, print_lock) for pod in pod_range]
        for future in concurrent.futures.as_completed(futures):
            pod, table_rows, power_map, missing_vm_errors = future.result()
            full_table_data.extend(table_rows)
            power_states[pod] = power_map
            all_check_results.extend(missing_vm_errors)

    # Conditionally print the summary table only if it's NOT a Maestro lab.
    if not is_maestro:
        with print_lock:
            print("\n📊 Permissions and VM Count Summary\n")
            print(tabulate(full_table_data, headers=["Entity Type", "Entity Name", "Username", "Role", "Propagate", "VM Count", "Error"], tablefmt="fancy_grid"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for pod in pod_range:
            components, ignore_list = fetch_course_config(pod, args.group, args.verbose, print_lock)
            if args.component:
                selected_components = [c.strip() for c in args.component.split(',')]
                components = [c for c in components if c[0] in selected_components]
                if not components:
                    with print_lock:
                        print(f"   - No matching components found for pod {pod}. Skipping network checks for this pod.")
                    continue
            future = executor.submit(perform_network_checks, pod, components, ignore_list, args.host, power_states.get(pod, {}), args.verbose, print_lock, is_maestro)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            all_check_results.extend(future.result())

    return all_check_results

if __name__ == "__main__":
    main()