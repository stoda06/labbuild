#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
import subprocess
from pyVmomi import vim
from tabulate import tabulate
import re
import ssl, argparse, sys, concurrent.futures, threading
import paramiko
from pymongo import MongoClient
from db_utils import get_vcenter_by_host

context = ssl._create_unverified_context()

RED = '\033[91m'
END = '\033[0m'

def _count_arping_replies(output: str) -> int:
    """
    Return how many ARP replies were observed in `arping` output across
    common implementations/formats.
    """
    out = output or ""

    # 1) Count per-reply lines (iputils/busybox often print these)
    #    e.g., "Unicast reply from 172.30.4.161 [00:50:56:04:00:3d]  1.23ms"
    #    or "60 bytes from 00:50:56:... (172.30.4.161): index=0 time=..."
    per_reply = 0
    per_reply += len(re.findall(r'\bUnicast reply\b', out, re.IGNORECASE))
    per_reply += len(re.findall(r'^\s*\d+\s+bytes\s+from\b', out, re.IGNORECASE | re.MULTILINE))

    # 2) Summary style: "Received N response(s)"
    m = re.search(r'Received\s+(\d+)\s+response', out, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # 3) Summary style: "... packets received, ... (N extra)"
    #    The 'extra' count indicates (received - transmitted). We want total received.
    m = re.search(r'(\d+)\s+packets\s+received.*?\((\d+)\s+extra\)', out, re.IGNORECASE)
    if m:
        # total received = reported_received; we can trust the first capture directly
        return int(m.group(1))

    # Fallback to per-reply count if we saw any
    if per_reply:
        return per_reply

    # As a last resort, try: "... (\d+)\s+packets\s+received" without "(N extra)"
    m = re.search(r'(\d+)\s+packets\s+received', out, re.IGNORECASE)
    if m:
        return int(m.group(1))

    return 0


def is_problem_status(status: str) -> bool:
    """Return True if the status should be considered an error for the final output."""
    return (status or "").strip().upper() != "UP"

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

def get_expected_vm_count(course_name, pod, print_lock):
    """
    Returns how many VMs are expected for this course/pod based on components
    that have a clone_name containing '{X}'.
    """
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]; collection = db["temp_courseconfig"]
        course_doc = collection.find_one({"course_name": course_name})
        if not course_doc or "components" not in course_doc:
            with print_lock:
                print(f"❌ Unable to fetch components for course '{course_name}'")
            return 0
        expected_vms = [c.get("clone_name", "") for c in course_doc["components"] if "{X}" in c.get("clone_name", "")]
        return len(expected_vms)
    except Exception as e:
        with print_lock:
            print(f"❌ Error retrieving expected VM count for course '{course_name}': {e}")
        return 0

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

def check_entity_permissions_and_vms(pod, content, entity_name, entity_type, username, verbose, print_lock, expected_count=0):
    result_rows, vm_names, vm_power_map = [], [], {}
    entity = get_entity_by_name(content, entity_name, entity_type)
    if entity:
        perms = get_permissions_for_user(entity, username)
        if perms:
            for perm in perms:
                role_name = get_role_name(content, perm.roleId)
                propagate = "Yes" if perm.propagate else "No"

                # Count VMs
                if isinstance(entity, vim.Folder):
                    vm_count = get_vms_count_in_folder(entity)
                    vm_names = []
                else:
                    vm_data = get_vms_count_in_resource_pool(entity, verbose, print_lock)
                    vm_names = [n for n, _ in vm_data]
                    vm_power_map = {n: p for n, p in vm_data}
                    vm_count = len(vm_data)

                # Build error string: only flag when actual > expected
                error = ""
                if expected_count and vm_count > expected_count:
                    diff = vm_count - expected_count
                    error = f"{RED}+{diff} (should be {expected_count}){END}"
                elif not perm.propagate:
                    error = "Permissions not set to propagate"

                result_rows.append([
                    "Folder" if isinstance(entity, vim.Folder) else "ResourcePool",
                    entity_name,
                    username,
                    role_name,
                    propagate,
                    vm_count,
                    error
                ])
        else:
            result_rows.append([entity_type[0].__name__, entity_name, username, "-", "-", 0, "No roles found"])
    else:
        result_rows.append([entity_type[0].__name__, entity_name, username, "-", "-", 0, "Entity not found"])
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

    # NEW: compute expected VM count for this course/pod
    expected_count = get_expected_vm_count(group, pod, lock)

    rp_rows, vms, power_map = check_entity_permissions_and_vms(
        pod, content, rp_candidates, [vim.ResourcePool], username, verbose, lock, expected_count=expected_count)

    folder_rows, _, _ = check_entity_permissions_and_vms(
        pod, content, folder_candidates, [vim.Folder], username, verbose, lock, expected_count=expected_count)

    compare_vms_with_components(pod, vms, group, lock)
    return rp_rows + folder_rows, power_map

def resolve_pod_ip(ip_raw, pod, host_key):
    if "+X" in ip_raw:
        base, _, _ = ip_raw.partition("+X")
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
    # Construct the correct hostname
    host = f"cpvr{pod}.us" if host_key.lower() in ["hotshot", "trypticon"] else f"cpvr{pod}"
    check_results = []

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def _iface_for_ip(ip: str) -> str:
        """Find an interface name on the VR that matches the target IP's /24."""
        subnet = ".".join(ip.split(".")[:3])
        iface_cmd = f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1"
        if verbose:
            with print_lock:
                print(f"   -> Running (get iface): {iface_cmd}")
        _, stdout, stderr = client.exec_command(iface_cmd)
        iface = stdout.read().decode().strip().rstrip(':')
        err = stderr.read().decode().strip()
        if err or not iface:
            return ""
        return iface

    def _arping_has_duplicates(ip: str) -> bool:
        """
        Run `arping -c 1` on the right interface and detect if >1 replies were seen.
        We treat either multiple 'bytes from' lines or the summary showing 'packets received' > 1
        (or '(N extra)') as duplicates.
        """
        iface = _iface_for_ip(ip)
        if not iface:
            if verbose:
                with print_lock:
                    print(f"      -> No interface found for {ip}; skipping ARP duplicate check")
            return False

        # Send exactly 1 request and parse replies
        arping_cmd = f"arping -I {iface} -c 1 {ip}"
        if verbose:
            with print_lock:
                print(f"   -> Running: {arping_cmd}")
        _, stdout, _ = client.exec_command(arping_cmd, timeout=20)
        out = stdout.read().decode()

        # Count reply lines like: "60 bytes from 00:50:56:... (IP): index=0 time=..."
        reply_lines = [ln for ln in out.splitlines() if "bytes from" in ln]
        if len(reply_lines) > 1:
            return True

        # Fallback: parse the summary line "X packets received, (Y extra)" if present
        # e.g. "... 1 packets transmitted, 2 packets received, 0% unanswered (1 extra)"
        m = re.search(r"(\d+)\s+packets received.*?(?:\((\d+)\s+extra\))?", out, flags=re.IGNORECASE)
        if m:
            recv = int(m.group(1))
            extra = int(m.group(2)) if m.group(2) else 0
            if recv > 1 or extra > 0:
                return True

        return False

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

            # Skip powered-off VMs
            if display_name in vm_power_map and vm_power_map[display_name] == vim.VirtualMachinePowerState.poweredOff:
                status = "SKIPPED (Powered Off)"
                check_results.append({'pod': pod, 'component': display_name, 'ip': ip, 'port': port, 'status': status, 'host': host})
                continue

            # 1) Do the existing connectivity check (arping or TCP)
            if isinstance(port, str) and port.lower() == "arping":
                # Use arping for reachability
                iface = _iface_for_ip(ip)
                if not iface:
                    status = "DOWN (No iface)"
                else:
                    cmd = f"arping -I {iface} -c 1 {ip}"
                    if verbose:
                        with print_lock:
                            print(f"   -> Running: {cmd}")
                    _, stdout, stderr = client.exec_command(cmd, timeout=20)
                    out = stdout.read().decode()
                    err = stderr.read().decode()
                    # consider UP if we saw at least one reply
                    status = "UP" if "bytes from" in out or "1 packets received" in out else "DOWN"
            else:
                # TCP port check via nmap from VR
                cmd = f"nmap -Pn -p {port} {ip}"
                if verbose:
                    with print_lock:
                        print(f"   -> Running: {cmd}")
                _, stdout, _ = client.exec_command(cmd)
                output = stdout.read().decode()
                if f"{port}/tcp open" in output:
                    status = "UP"
                elif f"{port}/tcp filtered" in output:
                    status = "FILTERED"
                else:
                    status = "DOWN"

            # 2) If this is the VR component, ALSO run the duplicate ARP check and override status if dupes found
            #    (We key off the name containing 'vr', e.g., cp-R81-vr-61)
            is_vr = "vr" in (display_name or "").lower()
            if is_vr:
                try:
                    if _arping_has_duplicates(ip):
                        status = "DUPLICATE ARP"
                        # Make the message match exactly what you requested in the error tables
                        with print_lock:
                            print(f"⚠️  duplicate responses from {ip} for cpvr{pod}")
                except Exception as dup_e:
                    # non-fatal: keep original status if dup check couldn’t run
                    if verbose:
                        with print_lock:
                            print(f"   -> ARP duplicate check failed for {ip}: {dup_e}")

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
                if is_problem_status(r['status']):
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

        # Skip powered-off VMs
        if power_map.get(name) == vim.VirtualMachinePowerState.poweredOff:
            status = "SKIPPED (Powered Off)"
            results.append({"pod": pod, "component": name, "ip": ip, "port": port, "status": status, "host": "localhost"})
            continue

        # Build nmap cmd
        if str(port).isdigit():
            cmd = ["nmap", "-Pn", "-p", str(port), ip]
        else:
            cmd = ["nmap", "-Pn", ip]

        if verbose:
            with lock:
                print(f"LOCAL-nmap: {' '.join(cmd)}")

        try:
            # use run() with timeout so a hung host doesn’t stall the test
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            out = proc.stdout or ""
            if verbose and out:
                with lock:
                    print(f"nmap raw output for {ip}:{port}:\n{out}")

            # Parse line like "3389/tcp filtered" or "3389/tcp open"
            if str(port).isdigit():
                m = re.search(rf"\b{port}/tcp\s+([a-zA-Z0-9|\-]+)", out)
                if m:
                    state = m.group(1).lower()
                    if "open" in state:
                        status = "UP"
                    elif "filtered" in state or "closed|filtered" in state or "closed" in state:
                        status = "FILTERED"  # treat as a failure state
                    else:
                        status = "DOWN"
                else:
                    status = "DOWN"
            else:
                # If no port specified, consider host up/down based on "Host is up."
                status = "UP" if "Host is up" in out else "DOWN"

        except subprocess.TimeoutExpired:
            status = "ERROR (timeout)"
        except FileNotFoundError:
            status = "ERROR (nmap missing)"
        except subprocess.CalledProcessError as e:
            status = f"ERROR ({e.returncode})"

        results.append({"pod": pod, "component": name, "ip": ip, "port": port, "status": status, "host": "localhost"})
    return results

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
            # 1) honor ignore flag
            if _is_truthy(c.get("ignore")):
                if verbose:
                    with print_lock:
                        print(f"⏭️  Skipping ignored component: {c.get('component_name', 'UNKNOWN')}")
                continue
            # 2) take only complete endpoints
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

def _is_truthy(v) -> bool:
    """Treat 'true', '1', 'yes', 'y' (case-insensitive) and boolean True as truthy."""
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"true", "1", "yes", "y"}


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
    has_failures = False  # track across permission/VM count and network checks

    if args.verbose:
        print(f"🌐 Connecting to vCenter: {vcn}", flush=True)

    # ---- vCenter scans ----
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe:
        futures = [exe.submit(threaded_fn_test_cp, pod, vcn, args.group, args.verbose, lock) for pod in pods]
        for fut in futures:
            pod_id, rows, pm = fut.result()
            full_rows.extend(rows)
            power_maps[pod_id] = pm

    # Always show the full permissions/count table
    with lock:
        print("\n📊 Permissions and VM Count Summary")
        print(tabulate(full_rows,
                       headers=["Entity Type","Entity Name","Username","Role","Propagate","VM Count","Error"],
                       tablefmt="fancy_grid"))

    # Collect permission/count overage errors for final error table
    perm_error_rows = [r for r in full_rows if isinstance(r, list) and r[-1]]
    if perm_error_rows:
        has_failures = True  # mark failure if we saw overages or any other error string

    # ---- Network checks ----
    is_maestro = args.group.lower().startswith("maestro-")
    func = perform_network_checks_local_nmap if is_maestro else perform_network_checks_over_ssh

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe2:
        futures2 = []
        for pod in pods:
            comps, _ = fetch_course_config(pod, args.group, args.verbose, lock)
            if args.component:
                want = set(c.strip() for c in args.component.split(","))
                comps = [c for c in comps if c[0] in want]
                if not comps:
                    with lock:
                        print(f"   - Pod {pod} filtered out; skipping net check")
                    continue
            futures2.append(exe2.submit(func, pod, comps, None, args.host, power_maps.get(pod, {}), args.verbose, lock))

        for fut in futures2:
            res = fut.result()
            if res:
                net_results.extend(res)

    # Print the network summary table
    if net_results:
        headers = ["Component", "Component IP", "Host", "Port", "Status"]
        table = []
        any_net_failures = False

        for r in net_results:
            s = r["status"]
            if is_problem_status(s):
                s_print = f"{RED}{s}{END}"
                any_net_failures = True
            else:
                s_print = s
            table.append([r["component"], r["ip"], r["host"], r["port"], s_print])

        print("\n📊 Network Test Summary (Local nmap)" if is_maestro else "\n📊 Network Test Summary")
        print(tabulate(table, headers=headers, tablefmt="fancy_grid"))

        if any_net_failures:
            has_failures = True

    # ---- FINAL CONSOLIDATED ERROR SECTION ----
    if has_failures:
        # 1) Permission / VM count errors (includes red +N (should be X))
        if perm_error_rows:
            print("\n🚨 Errors: Permissions / VM Count")
            print(tabulate(
                perm_error_rows,
                headers=["Entity Type","Entity Name","Username","Role","Propagate","VM Count","Error"],
                tablefmt="fancy_grid"
            ))

        # 2) Network failures: include ALL non-UP statuses (e.g., DOWN, FILTERED, SKIPPED, timeouts, etc.)
        net_error_rows = []
        for r in net_results:
            if is_problem_status(r["status"]):
                net_error_rows.append([
                    r["component"], r["ip"], r["host"], r["port"], f"{RED}{r['status']}{END}"
                ])
        if net_error_rows:
            print("\n🚨 Errors: Network Failures")
            print(tabulate(
                net_error_rows,
                headers=["Component","Component IP","Host","Port","Status"],
                tablefmt="fancy_grid"
            ))

        print("\n❌ One or more checks failed.")
        sys.exit(2)

    # If we reach here, everything is OK
    print("\n✅ No errors found.")
    sys.exit(0)

if __name__ == "__main__":
    main()

