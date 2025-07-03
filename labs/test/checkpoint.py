#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate
import ssl, argparse, sys, pexpect, concurrent.futures, threading
from pymongo import MongoClient
# --- MODIFIED ---
from db_utils import get_vcenter_by_host

context = ssl._create_unverified_context()

# REMOVED VCENTER_MAP

RED = '\033[91m'
END = '\033[0m'

def threaded_fn_test_cp(pod, vcenter_host, group, verbose, print_lock):
    try:
        si = SmartConnect(host=vcenter_host, user="administrator@vcenter.rededucation.com", pwd="pWAR53fht786123$", sslContext=context)
        result, power_map = fn_test_cp(pod, si, group, verbose, print_lock)
        Disconnect(si)
        return pod, result, power_map
    except Exception as e:
        with print_lock:
            print(f"❌ Error connecting to vCenter for pod {pod}: {e}")
        return pod, [], {}

def compare_vms_with_components(pod, vms_in_pool, course_name, print_lock):
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; collection = db["temp_courseconfig"]
        course_doc = collection.find_one({"course_name": course_name})
        if not course_doc or "components" not in course_doc:
            with print_lock:
                print(f"❌ Unable to fetch components for course '{course_name}'")
            return
        expected_vms = [comp.get("clone_name", "").replace("{X}", str(pod)) for comp in course_doc["components"] if "{X}" in comp.get("clone_name", "")]
        vm_set, expected_set = set(vms_in_pool), set(expected_vms)
        missing, extra = expected_set - vm_set, vm_set - expected_set
        if not missing and not extra:
            with print_lock:
                print(f"\n✅ All expected components are present for Pod {pod}")
            return
        diff_rows = [["Missing from pool", name] for name in sorted(missing)] + [["Unexpected in pool", name] for name in sorted(extra)]
        with print_lock:
            print(f"\n📌 VM vs Component Check for Pod {pod}")
            print(tabulate(diff_rows, headers=["Status", "VM Name"], tablefmt="fancy_grid"))
    except Exception as e:
        with print_lock:
            print(f"❌ Error during VM comparison for pod {pod}: {e}")

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
    compare_vms_with_components(pod, vms, group, print_lock)
    return table_data, power_map

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
        for c in course["components"]:
            if c.get("podip") and c.get("podport"): components.append((c["component_name"], c["podip"], c["podport"]))
            elif verbose:
                with print_lock:
                    print(f"⚠️ Skipping incomplete component config: {c.get('component_name', 'UNKNOWN')}")
        return components, []
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB query failed: {e}")
        return [], []

def resolve_pod_ip(ip_raw, pod):
    if "+X" in ip_raw:
        base, _, offset = ip_raw.partition("+X"); octets = base.split(".")
        octets[-1] = str(int(octets[-1]) + pod); return ".".join(octets)
    return ip_raw

def perform_network_checks_over_ssh(pod, components, ignore_list, host_key, vm_power_map, verbose, print_lock):
    host = f"cpvr{pod}.us" if host_key == "hotshot" else f"cpvr{pod}"
    if verbose:
        with print_lock:
            print(f"\n🔐 Connecting to {host} via SSH...")
    
    check_results = []
    try:
        child = pexpect.spawn(f"ssh {host}", timeout=30)
        i = child.expect([r"Are you sure you want to continue connecting.*", r"vr:~#", r"[Pp]assword:", pexpect.EOF, pexpect.TIMEOUT], timeout=20)
        if i == 0:
            child.sendline("yes")
            child.expect(r"vr:~#")
        elif i == 1:
            if verbose:
                with print_lock:
                    print(f"✅ SSH to {host} successful")
        else:
            with print_lock:
                print(f"❌ Failed to connect to {host}.")
            check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host, 'port': 22, 'status': 'FAILED', 'host': host})
            return check_results
        
        for name, raw_ip, port in components:
            ip = resolve_pod_ip(raw_ip, pod)
            status = "UNKNOWN"
            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                child.expect(r"vr:~#")
                iface = child.before.decode().strip().splitlines()[-1]
                if iface:
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    child.expect(r"vr:~#")
                    status = "UP" if "Unicast reply" in child.before.decode() else "DOWN"
            else:
                child.sendline(f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                child.expect(r"vr:~#")
                status = "UP" if "open" in child.before.decode() else "DOWN"
            
            check_results.append({'pod': pod, 'component': name, 'ip': ip, 'port': port, 'status': status, 'host': host})
        
        child.sendline("exit")
        child.expect(pexpect.EOF)
        child.close()
    except Exception as e:
        with print_lock:
            print(f"❌ SSH or command execution failed on {host}: {e}")
        check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host, 'port': 22, 'status': 'FAILED', 'host': host})
        return check_results

    with print_lock:
        if check_results:
            print(f"\n📊 Network Test Summary for Pod {pod}")
            headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
            table_data = [[r['component'], r['ip'], r['host'], r['port'], r['status']] for r in check_results]
            formatted_rows = [[f"{RED}{cell}{END}" if row[4] != "UP" else cell for cell in row] for row in table_data]
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

    # --- MODIFIED: Dynamic vCenter Lookup ---
    vcenter_fqdn = get_vcenter_by_host(args.host)
    if not vcenter_fqdn:
        with print_lock:
            print(f"❌ Could not find vCenter for host '{args.host}' in the database.")
        sys.exit(1)
    
    pod_range, power_states = list(range(args.start, args.end + 1)), {}
    all_check_results = []
    
    with print_lock:
        print(f"\n🌐 Connecting to vCenter: {vcenter_fqdn}")
    # --- END MODIFICATION ---

    full_table_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(threaded_fn_test_cp, pod, vcenter_fqdn, args.group, args.verbose, print_lock) for pod in pod_range]
        for future in concurrent.futures.as_completed(futures):
            pod, table_rows, power_map = future.result()
            full_table_data.extend(table_rows)
            power_states[pod] = power_map

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
            future = executor.submit(perform_network_checks_over_ssh, pod, components, ignore_list, args.host, power_states.get(pod, {}), args.verbose, print_lock)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            all_check_results.extend(future.result())

    return all_check_results

if __name__ == "__main__":
    main()