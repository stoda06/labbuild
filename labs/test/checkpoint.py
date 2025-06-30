#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate
import ssl, argparse, sys, pexpect, concurrent.futures
from pymongo import MongoClient

context = ssl._create_unverified_context()

VCENTER_MAP = {
    "cliffjumper": "vcenter-appliance-1",
    "hydra": "vcenter-appliance-1",
    "unicron": "vcenter-appliance-1",
    "apollo": "vcenter-appliance-2",
    "nightbird": "vcenter-appliance-2",
    "ultramagnus": "vcenter-appliance-2",
    "hotshot": "vcenter-appliance-3",
    "ps01": "vcenter-appliance-4",
    "ps02": "vcenter-appilance-4",
    "ps03": "vcenter-appliance-4",
    "shockwave": "vcenter-appliance-5",
    "optimus": "vcenter-appliance-5",
}

def threaded_fn_test_cp(pod, vcenter_host, group, verbose):
    try:
        si = SmartConnect(
            host=vcenter_host,
            user="administrator@vcenter.rededucation.com",
            pwd="pWAR53fht786123$",
            sslContext=context
        )
        result, power_map = fn_test_cp(pod, si, group, verbose)
        Disconnect(si)
        return pod, result, power_map
    except Exception as e:
        print(f"❌ Error connecting to vCenter for pod {pod}: {e}")
        return pod, [], {}

def compare_vms_with_components(pod, vms_in_pool, course_name):
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        course_doc = collection.find_one({"course_name": course_name})

        if not course_doc or "components" not in course_doc:
            print(f"❌ Unable to fetch components for course '{course_name}'")
            return

        expected_vms = []
        for comp in course_doc["components"]:
            clone_name = comp.get("clone_name", "")
            if "{X}" in clone_name:
                expected_vms.append(clone_name.replace("{X}", str(pod)))

        vm_set = set(vms_in_pool)
        expected_set = set(expected_vms)

        missing = expected_set - vm_set
        extra = vm_set - expected_set

        if not missing and not extra:
            print(f"\n✅ All expected components are present for Pod {pod}")
            return

        diff_rows = [["Missing from pool", name] for name in sorted(missing)] + \
                    [["Unexpected in pool", name] for name in sorted(extra)]
        print(f"\n📌 VM vs Component Check for Pod {pod}")
        print(tabulate(diff_rows, headers=["Status", "VM Name"], tablefmt="fancy_grid"))

    except Exception as e:
        print(f"❌ Error during VM comparison for pod {pod}: {e}")

def get_entity_by_name(content, name, vimtype):
    container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
    for entity in container.view:
        if entity.name == name:
            container.Destroy()
            return entity
    container.Destroy()
    return None

def get_permissions_for_user(entity, username):
    return [perm for perm in entity.permission if username.lower() in perm.principal.lower()]

def get_role_name(content, role_id):
    for role in content.authorizationManager.roleList:
        if role.roleId == role_id:
            return role.name
    return None

def get_vms_count_in_folder(folder):
    return len([vm for vm in folder.childEntity if isinstance(vm, vim.VirtualMachine)])

def get_vms_count_in_resource_pool(resource_pool, verbose=False):
    try:
        results = [(vm.name, vm.runtime.powerState) for vm in resource_pool.vm]
        if verbose:
            print(f"📦 Resource Pool '{resource_pool.name}' VM Status:")
            for name, power in results:
                print(f"   - {name} [{power}]")
        return results
    except Exception as e:
        print(f"⚠️ Failed to retrieve VMs: {e}")
        return []

def check_entity_permissions_and_vms(pod, content, entity_name, entity_type, username, verbose=False):
    result_rows = []
    vm_names = []
    vm_power_map = {}

    entity = get_entity_by_name(content, entity_name, entity_type)
    if entity:
        perms = get_permissions_for_user(entity, username)
        if perms:
            for perm in perms:
                role_name = get_role_name(content, perm.roleId)
                propagate = "Yes" if perm.propagate else "No"
                error = "" if perm.propagate else "Permissions not set to propagate"
                if isinstance(entity, vim.Folder):
                    vm_count = get_vms_count_in_folder(entity)
                    vm_names = []
                else:
                    vm_data = get_vms_count_in_resource_pool(entity, verbose)
                    vm_names = [name for name, _ in vm_data]
                    vm_power_map = {name: power for name, power in vm_data}
                    vm_count = len(vm_names)

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

def fn_test_cp(pod, si, group, verbose=False):
    content = si.RetrieveContent()
    username = f"labcp-{pod}"
    rp_name = f"cp-pod{pod}"
    folder_name = f"cp-pod{pod}-folder"

    table_data = []
    rows_rp, vms, power_map = check_entity_permissions_and_vms(pod, content, rp_name, [vim.ResourcePool], username, verbose)
    rows_folder, _, _ = check_entity_permissions_and_vms(pod, content, folder_name, [vim.Folder], username, verbose)

    table_data.extend(rows_rp + rows_folder)
    compare_vms_with_components(pod, vms, group)
    return table_data, power_map

def fetch_course_config(pod, group, verbose=False):
    try:
        client = MongoClient("mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db")
        db = client["labbuild_db"]
        collection = db["temp_courseconfig"]
        course = collection.find_one({"course_name": group})
        if not course or "components" not in course:
            print(f"❌ No components found for course '{group}'")
            return [], []

        components = []
        for c in course["components"]:
            if c.get("podip") and c.get("podport"):
                components.append((c["component_name"], c["podip"], c["podport"]))
            elif verbose:
                print(f"⚠️ Skipping incomplete component config: {c.get('component_name', 'UNKNOWN')}")
        return components, []
    except Exception as e:
        print(f"❌ MongoDB query failed: {e}")
        return [], []

def resolve_pod_ip(ip_raw, pod):
    if "+X" in ip_raw:
        base = ip_raw.replace("+X", "")
        octets = base.split(".")
        octets[-1] = str(int(octets[-1]) + pod)
        return ".".join(octets)
    return ip_raw

def perform_network_checks_over_ssh(pod, components, ignore_list, host_key, vm_power_map, verbose=False):
    from tabulate import tabulate
    from pyVmomi import vim

    RED = '\033[91m'
    END = '\033[0m'

    host = f"cpvr{pod}.us" if host_key == "hotshot" else f"cpvr{pod}"
    if verbose:
        print(f"\n🔐 Connecting to {host} via SSH...")

    results = []
    vm_power_issues = []
    vm_unexpected_issues = []

    try:
        child = pexpect.spawn(f"ssh {host}", timeout=30)

        # Handle possible SSH prompts: fingerprint confirmation, shell prompt, password prompt, etc.
        i = child.expect([
            r"Are you sure you want to continue connecting.*",
            r"vr:~#",
            r"[Pp]assword:",
            pexpect.EOF,
            pexpect.TIMEOUT
        ], timeout=20)

        if i == 0:
            print(f"🆕 SSH to {host} triggered fingerprint prompt — sending 'yes'")
            child.sendline("yes")
            child.expect(r"vr:~#")
        elif i == 1:
            if verbose:
                print(f"✅ SSH to {host} successful")
        elif i == 2:
            print(f"❌ Unexpected password prompt from {host}, exiting.")
            return
        else:
            print(f"❌ Failed to connect to {host}, no expected prompt received.")
            return

        for name, raw_ip, port in components:
            ip = resolve_pod_ip(raw_ip, pod)
            status = "UNKNOWN"

            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                child.expect(r"vr:~#")
                iface = child.before.decode().strip().splitlines()[-1]

                if iface:
                    if verbose:
                        print(f"📶 Running arping -I {iface} {ip}")
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    child.expect(r"vr:~#")
                    output = child.before.decode()
                    status = "UP" if "Unicast reply" in output else "DOWN"
            else:
                if verbose:
                    print(f"🔎 Running nmap -Pn -p {port} {ip} (checking if port is open)")
                child.sendline(f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                child.expect(r"vr:~#")
                output = child.before.decode()
                status = "UP" if "open" in output else "DOWN"

            results.append([name, ip, host, port, status])

            if status == "DOWN":
                vm_state = vm_power_map.get(name)
                if vm_state == vim.VirtualMachinePowerState.poweredOff:
                    vm_power_issues.append(name)
                elif vm_state == vim.VirtualMachinePowerState.poweredOn:
                    vm_unexpected_issues.append(name)

        child.sendline("exit")
        child.expect(pexpect.EOF)
        child.close()
    except Exception as e:
        print(f"❌ SSH or command execution failed on {host}: {e}")
        return

    if results:
        print(f"\n📊 Network Test Summary for Pod {pod}")
        headers = ["Component", "Component IP", "Pod ID", "Pod Port", "Status"]
        formatted_rows = []
        for row in results:
            if row[4] == "DOWN":
                formatted_row = [f"{RED}{cell}{END}" if i == 4 else cell for i, cell in enumerate(row)]
                formatted_rows.append(formatted_row)
            else:
                formatted_rows.append(row)
        print(tabulate(formatted_rows, headers=headers, tablefmt="fancy_grid"))

    powered_off_vms = [[name, str(state)] for name, state in vm_power_map.items() if state == vim.VirtualMachinePowerState.poweredOff]
    if powered_off_vms:
        print(f"\n🔌 VM Power State Summary for Pod {pod}")
        print(tabulate(powered_off_vms, headers=["VM Name", "Power State"], tablefmt="grid"))

    if vm_power_issues:
        print(f"\n⚠️ Components failing because VM is powered off in Pod {pod}:")
        for name in vm_power_issues:
            print(f"   🔌 {name} [POWERED OFF]")

    if vm_unexpected_issues:
        print(f"\n❗ VM is ON but component still failing in Pod {pod}:")
        for name in vm_unexpected_issues:
            print(f"   🟥 {name} [RED ALERT]")

    if ignore_list:
        print(f"\n📝 Note: {', '.join(ignore_list)} can be ignored.")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check CP pod permissions, fetch configs, and run network tests.",prog='checkpoint.py')
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    parser.add_argument("-H", "--host", required=True)
    parser.add_argument("-g", "--group", required=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-c", "--component", help="Test specific components (comma-separated list).")
    args = parser.parse_args(argv)

    host_key = args.host.lower()
    if host_key not in VCENTER_MAP:
        print(f"❌ Host '{args.host}' is not recognized.")
        sys.exit(1)

    vcenter_host = VCENTER_MAP[host_key]
    pod_range = list(range(args.start, args.end + 1))
    power_states = {}

    print(f"\n🌐 Connecting to vCenter: {vcenter_host}")

    full_table_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(threaded_fn_test_cp, pod, vcenter_host, args.group, args.verbose) for pod in pod_range]
        for future in concurrent.futures.as_completed(futures):
            pod, table_rows, power_map = future.result()
            full_table_data.extend(table_rows)
            power_states[pod] = power_map

    print("\n📊 Permissions and VM Count Summary\n")
    print(tabulate(full_table_data, headers=["Entity Type", "Entity Name", "Username", "Role", "Propagate", "VM Count", "Error"], tablefmt="fancy_grid"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for pod in pod_range:
            components, ignore_list = fetch_course_config(pod, args.group, args.verbose)
            
            if args.component:
                selected_components = [c.strip() for c in args.component.split(',')]
                original_count = len(components)
                components = [c for c in components if c[0] in selected_components]
                print(f"\n🔎 Filtering for pod {pod}: Selected {len(components)} of {original_count} components based on user input.")
                if not components:
                    print(f"   - No matching components found for pod {pod}. Skipping network checks for this pod.")
                    continue

            futures.append(executor.submit(perform_network_checks_over_ssh, pod, components, ignore_list, host_key, power_states.get(pod, {}), args.verbose))
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    main()