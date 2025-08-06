#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
import subprocess
from pyVmomi import vim
from tabulate import tabulate
import ssl, argparse, sys, concurrent.futures, threading
import paramiko
from pymongo import MongoClient
from db_utils import get_vcenter_by_host

context = ssl._create_unverified_context()

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

def fn_test_cp(pod, si, group, verbose, lock):
    content = si.RetrieveContent()
    username = f"labcp-{pod}"
    maestro_group = group.lower().startswith("maestro-")

    if maestro_group:
        rp_candidates = f"cp-maestro-pod{pod}"
        folder_candidates = f"cp-maestro-{pod}-folder"
    else:
        folder_candidates = f"cp-pod{pod}-folder"
        rp_candidates = f"cp-pod{pod}"

    rp_rows, vms, power_map = check_entity_permissions_and_vms(
        pod, content, rp_candidates, [vim.ResourcePool], username, verbose, lock)
    folder_rows, _, _ = check_entity_permissions_and_vms(
        pod, content, folder_candidates, [vim.Folder], username, verbose, lock)

    compare_vms_with_components(pod, vms, group, lock)
    return rp_rows + folder_rows, power_map


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
            if c.get("podip") and c.get("podport"):
                components.append((c.get("component_name"), c.get("clone_name"), c.get("podip"), c.get("podport")))
            elif verbose:
                with print_lock:
                    print(f"⚠️ Skipping incomplete component config: {c.get('component_name', 'UNKNOWN')}")
        return components, []
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB query failed: {e}")
        return [], []

def resolve_pod_ip(ip_raw, pod, host_key):
    if "+X" in ip_raw:
        base, _, offset = ip_raw.partition("+X")
        octets = base.split(".")
        octets[-1] = str(int(octets[-1]) + pod)
        resolved_ip = ".".join(octets)
    else:
        resolved_ip = ip_raw

    is_us_host = host_key.lower() in ["hotshot", "trypticon"]
    octets = resolved_ip.split('.')

    if is_us_host and len(octets) == 4 and octets[1] == '30':
        octets[1] = '26'
        return ".".join(octets)

    return resolved_ip

def perform_network_checks_over_ssh(pod, components, ignore_list, host_key, vm_power_map, verbose, print_lock):
    # --- FIX: Construct the correct hostname at the beginning ---
    host = f"cpvr{pod}.us" if host_key.lower() in ["hotshot", "trypticon"] else f"cpvr{pod}"
    check_results = []

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        if verbose:
            with print_lock:
                print(f"\n🔐 Connecting to {host} via SSH...")

        client.connect(
            hostname=host,
            username='root',
            key_filename='/usr/local/share/reded/cp.id_rsa',
            timeout=20
        )
        if verbose:
            with print_lock:
                print(f"✅ SSH to {host} successful")

        for _, clone_name_template, raw_ip, port in components:
            ip = resolve_pod_ip(raw_ip, pod, host_key)
            display_name = clone_name_template.replace('{X}', str(pod)) if clone_name_template else "Unknown Component"
            status = "UNKNOWN"

            if display_name in vm_power_map and vm_power_map[display_name] == vim.VirtualMachinePowerState.poweredOff:
                status = "SKIPPED (Powered Off)"
                check_results.append({'pod': pod, 'component': display_name, 'ip': ip, 'port': port, 'status': status, 'host': host})
                continue

            command = ""
            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                iface_cmd = f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1"
                if verbose:
                    with print_lock: print(f"   -> Running (get iface): {iface_cmd}")

                _, stdout, stderr = client.exec_command(iface_cmd)
                iface = stdout.read().decode().strip().rstrip(':')
                err = stderr.read().decode().strip()

                if err or not iface:
                    status = "DOWN (No iface)"
                    if verbose:
                        with print_lock: print(f"      -> Error getting interface: {err or 'No interface found'}")
                else:
                    command = f"arping -c 3 -I {iface} {ip}"
                    if verbose:
                        with print_lock: print(f"   -> Running: {command}")
                    _, stdout, _ = client.exec_command(command)
                    output = stdout.read().decode()
                    status = "UP" if "Unicast reply" in output else "DOWN"
            else: # nmap for TCP ports
                command = f"nmap -Pn -p {port} {ip}"
                if verbose:
                    with print_lock: print(f"   -> Running: {command}")
                _, stdout, _ = client.exec_command(command)
                output = stdout.read().decode()
                # --- FIX: Handle 'open' and 'filtered' states from nmap ---
                if f"{port}/tcp open" in output:
                    status = "UP"
                elif f"{port}/tcp filtered" in output:
                    status = "FILTERED"
                else:
                    status = "DOWN"

            check_results.append({'pod': pod, 'component': display_name, 'ip': ip, 'port': port, 'status': status, 'host': host})

    except Exception as e:
        with print_lock:
            print(f"❌ SSH or command execution failed on {host}: {e}")
        check_results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host, 'port': 22, 'status': 'FAILED', 'host': host})
        return check_results
    finally:
        client.close()

    with print_lock:
        if check_results:
            print(f"\n📊 Network Test Summary for Pod {pod}")
            headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
            table_data = []
            for r in check_results:
                row = [r['component'], r['ip'], r['host'], r['port'], r['status']]
                # --- FIX: Color 'FILTERED' status red as well ---
                if r['status'] in ['DOWN', 'FAILED', 'FILTERED']:
                    row[4] = f"{RED}{r['status']}{END}"
                table_data.append(row)
            print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"))

        powered_off_vms = [[name, str(state)] for name, state in vm_power_map.items() if state == vim.VirtualMachinePowerState.poweredOff]
        if powered_off_vms:
            print(f"\n🔌 VM Power State Summary for Pod {pod}")
            print(tabulate(powered_off_vms, headers=["VM Name", "Power State"], tablefmt="grid"))

    return check_results

def perform_network_checks_local_nmap(pod, components, _ignore, host_key, power_map, verbose, lock):
    results = []
    for _name, clone, ip_raw, port in components:
        name = (clone or "UNKNOWN").replace("{X}", str(pod))
        ip = resolve_pod_ip(ip_raw, pod, host_key)
        status = "UNKNOWN"
        if power_map.get(name) == vim.VirtualMachinePowerState.poweredOff:
            status = "SKIPPED (powered off)"
            results.append({"pod": pod, "component": name, "ip": ip, "port": port, "status": status, "host": "localhost"})
            continue
        cmd = ["nmap", "-Pn", "-p", str(port), ip] if str(port).isdigit() else ["nmap", "-Pn", ip]
        if verbose:
            with lock:
                print(f"LOCAL‑nmap: {' '.join(cmd)}")
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode(errors="ignore")
            if str(port).isdigit() and f"{port}/tcp open" in out:
                status = "UP"
            elif str(port).isdigit() and f"{port}/tcp filtered" in out:
                status = "FILTERED"
            else:
                status = "DOWN"
        except subprocess.CalledProcessError as e:
            status = f"ERROR ({e.returncode})"
        except FileNotFoundError:
            status = "ERROR (nmap missing)"
        results.append({"pod": pod, "component": name, "ip": ip, "port": port, "status": status, "host": "localhost"})
    return results

def main(argv=None, print_lock=None):
    parser = argparse.ArgumentParser(description="CP Maestro-aware pod checks with local nmap")
    parser.add_argument("-s","--start", type=int, required=True)
    parser.add_argument("-e","--end", type=int, required=True)
    parser.add_argument("-H","--host", required=True)
    parser.add_argument("-g","--group", required=True)
    parser.add_argument("-v","--verbose", action="store_true")
    parser.add_argument("-c","--component", help="comma-separated filter list")
    args = parser.parse_args(argv)

    lock = print_lock or threading.Lock()
    vcn = get_vcenter_by_host(args.host)
    if not vcn:
        with lock:
            print(f"❌ vCenter lookup failed for host key '{args.host}'")
        sys.exit(1)

    pods = range(args.start, args.end+1)
    full_rows, power_maps, net_results = [], {}, []

    if args.verbose:
        print(f"🌐 Connecting to vCenter: {vcn}", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe:
        for fut in [exe.submit(threaded_fn_test_cp, pod, vcn, args.group, args.verbose, lock) for pod in pods]:
            pod_id, rows, pm = fut.result()
            full_rows.extend(rows)
            power_maps[pod_id] = pm

    with lock:
        print("\n📊 Permissions and VM Count Summary")
        print(tabulate(full_rows,
                       headers=["Entity Type","Entity Name","Username","Role","Propagate","VM Count","Error"],
                       tablefmt="fancy_grid"))

    is_maestro = args.group.lower().startswith("maestro-")
    func = perform_network_checks_local_nmap if is_maestro else perform_network_checks_over_ssh

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe2:
        for pod in pods:
            comps, _ = fetch_course_config(pod, args.group, args.verbose, lock)
            if args.component:
                want = set(c.strip() for c in args.component.split(","))
                comps = [c for c in comps if c[0] in want]
                if not comps:
                    with lock:
                        print(f"   - Pod {pod} filtered out; skipping net check")
                    continue
            fut = exe2.submit(func, pod, comps, None, args.host, power_maps.get(pod, {}), args.verbose, lock)
            res = fut.result()
            if res:
                net_results.extend(res)

    return net_results

if __name__ == "__main__":
    main()