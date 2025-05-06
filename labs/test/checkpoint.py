#!/usr/bin/env python3.10

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
from tabulate import tabulate
import ssl
import sys
import pexpect
from pymongo import MongoClient
import concurrent.futures

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

def get_entity_by_name(content, name, vimtype):
    container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
    entities = container.view
    container.Destroy()
    for entity in entities:
        if entity.name == name:
            return entity
    return None

def get_permissions_for_user(entity, username):
    return [perm for perm in entity.permission if username.lower() in perm.principal.lower()]

def get_role_name(content, role_id):
    auth_manager = content.authorizationManager
    for role in auth_manager.roleList:
        if role.roleId == role_id:
            return role.name
    return None

def get_vms_count_in_folder(folder):
    return len([entity for entity in folder.childEntity if isinstance(entity, vim.VirtualMachine)])

def get_vms_count_in_resource_pool(resource_pool):
    return len(resource_pool.vm)

def check_entity_permissions_and_vms(pod, content, entity_name, entity_type, username):
    result_rows = []
    entity = get_entity_by_name(content, entity_name, entity_type)

    if entity:
        permissions = get_permissions_for_user(entity, username)
        if permissions:
            for perm in permissions:
                role_name = get_role_name(content, perm.roleId)
                propagate = "Yes" if perm.propagate else "No"
                error = "" if perm.propagate else "Permissions not set to propagate"
                vm_count = get_vms_count_in_folder(entity) if isinstance(entity, vim.Folder) else get_vms_count_in_resource_pool(entity)
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
            result_rows.append([
                "Folder" if entity_type == [vim.Folder] else "ResourcePool",
                entity_name,
                username,
                "-",
                "-",
                0,
                "No roles found"
            ])
    else:
        result_rows.append([
            "Folder" if entity_type == [vim.Folder] else "ResourcePool",
            entity_name,
            username,
            "-",
            "-",
            0,
            "Entity not found"
        ])
    return result_rows

def fn_test_cp(pod, si, group):
    content = si.RetrieveContent()
    resource_pool_name = f"cp-pod{pod}"
    folder_name = f"cp-pod{pod}-folder"
    username = f"labcp-{pod}"

    table_data = []
    table_data.extend(check_entity_permissions_and_vms(pod, content, resource_pool_name, [vim.ResourcePool], username))
    table_data.extend(check_entity_permissions_and_vms(pod, content, folder_name, [vim.Folder], username))
    Disconnect(si)
    return table_data

def fetch_course_config(pod, group):
    try:
        mongo_uri = "mongodb://labbuild_user:$$u1QBd6&372#$rF@builder:27017/?authSource=labbuild_db"
        client = MongoClient(mongo_uri)
        db = client["labbuild_db"]
        ini_collection = db["labbuild_ini"]

        section_name = f"cp-{group}"
        section = ini_collection.find_one({"section": section_name}, {"componentlist": 1, "list_to_power_off": 1, "_id": 0})
        if not section or "componentlist" not in section:
            print(f"❌ No component list found for section '{section_name}'")
            return [], []

        ignore_components = [c.strip() for c in section.get("list_to_power_off", "").split(",") if c.strip()]
        componentlist_raw = section["componentlist"]
        componentlist = [f"cp-{line.strip()}" for line in componentlist_raw.splitlines() if line.strip()]

        pod_info = []
        for component in componentlist:
            if component in ignore_components:
                continue
            doc = ini_collection.find_one({"section": component}, {"podip": 1, "podport": 1, "_id": 0})
            if doc:
                pod_info.append((component, doc.get("podip", ""), doc.get("podport", "")))

        return pod_info, ignore_components

    except Exception as e:
        print(f"❌ MongoDB query failed: {e}")
        return [], []

def resolve_pod_ip(ip_raw, pod):
    if "+X" in ip_raw:
        base_ip = ip_raw.replace("+X", "")
        octets = base_ip.split(".")
        octets[-1] = str(int(octets[-1]) + pod)
        return ".".join(octets)
    return ip_raw

def perform_network_checks_over_ssh(pod, components, ignore_list):
    host = f"cpvr{pod}"
    print(f"\n🔐 Connecting to {host} via SSH...")

    results = []

    try:
        child = pexpect.spawn(f"ssh {host}", timeout=30)
        child.expect(r"vr:~#")
        print(f"✅ SSH to {host} successful")

        for component, raw_ip, port in components:
            ip = resolve_pod_ip(raw_ip, pod)
            status = "UNKNOWN"

            if port.lower() == "arping":
                subnet = ".".join(ip.split(".")[:3])
                child.sendline(f"ifconfig | grep {subnet} -B 1 | awk '{{print $1}}' | head -n 1")
                child.expect(r"vr:~#")
                iface_lines = child.before.decode().strip().splitlines()
                iface = iface_lines[-1] if iface_lines else ""
                if iface:
                    print(f"📶 Running arping -I {iface} {ip}")
                    child.sendline(f"arping -c 3 -I {iface} {ip}")
                    child.expect(r"vr:~#", timeout=15)
                    output = child.before.decode()
                    status = "UP" if "Unicast reply" in output else "DOWN"
                else:
                    print(f"⚠️ Could not determine interface for IP {ip}")
                    status = "UNKNOWN"
            else:
                print(f"🔎 Running nmap -Pn -p {port} {ip} (checking if port is open)")
                child.sendline(f"nmap -Pn -p {port} {ip} | grep '{port}/tcp'")
                child.expect(r"vr:~#", timeout=20)
                output = child.before.decode()
                status = "UP" if "open" in output else "DOWN"

            results.append([component, ip, host, port, status])

        child.sendline("exit")
        child.close()

    except Exception as e:
        print(f"❌ SSH or command execution failed on {host}: {e}")

    if results:
        print("\n📊 Network Test Summary")
        print(tabulate(results, headers=["Component", "Component IP", "Pod ID", "Pod Port", "Status"], tablefmt="fancy_grid"))

    if ignore_list:
        print(f"\n📝 Note: {', '.join(ignore_list)} can be ignored.")

def process_pod(pod, group):
    components, ignore_list = fetch_course_config(pod, group)
    perform_network_checks_over_ssh(pod, components, ignore_list)

def main(args):
    print(f"Running Checkpoint Test")
    print(f"Vendor: {args.vendor}")
    print(f"Start Pod: {args.start_pod}")
    print(f"End Pod: {args.end_pod}")
    print(f"Host: {args.host}")
    print(f"Group: {args.group}")

    host_key = args.host.lower()
    if host_key not in VCENTER_MAP:
        print(f"❌ Error: Host '{args.host}' is not recognized.")
        sys.exit(1)

    vcenter_host = VCENTER_MAP[host_key]
    full_table_data = []

    for i in range(args.start_pod, args.end_pod + 1):
        print(f"\n🌐 Connecting to vCenter: {vcenter_host}")
        si = SmartConnect(
            host=vcenter_host,
            user="administrator@vcenter.rededucation.com",
            pwd="pWAR53fht786123$",
            sslContext=context
        )
        full_table_data.extend(fn_test_cp(i, si, args.group))

    print("\n📊 Permissions and VM Count Summary\n")
    headers = ["Entity Type", "Entity Name", "Username", "Role", "Propagate", "VM Count", "Error"]
    print(tabulate(full_table_data, headers=headers, tablefmt="fancy_grid"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_pod, i, args.group) for i in range(args.start_pod, args.end_pod + 1)]
        concurrent.futures.wait(futures)
