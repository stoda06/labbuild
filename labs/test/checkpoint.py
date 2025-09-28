#!/usr/bin/env python3.10
import ssl, argparse, sys, concurrent.futures, threading, re, subprocess, json, shutil, shlex, inspect
from pyVim.connect import SmartConnect, Disconnect
import os
from pyVmomi import vim
from tabulate import tabulate
import paramiko
from pymongo import MongoClient
from db_utils import get_vcenter_by_host
import ipaddress as _ip
VC_MAX_WORKERS=3

# ===========================
# Globals / constants
# ===========================
context = ssl._create_unverified_context()
RED = '\033[91m'
END = '\033[0m'

# ===========================
# Tiny debug helper
# ===========================
def _debug_which_fn(fn, verbose, print_lock):
    if verbose:
        with print_lock:
            print(f"🧭 Using fn_test_cp from {inspect.getsourcefile(fn)}:{fn.__code__.co_firstlineno}", flush=True)

# ============================================================
# Local helpers
# ============================================================
def _count_arping_replies(output: str) -> int:
    out = output or ""
    per_reply = 0
    per_reply += len(re.findall(r'\bUnicast reply\b', out, re.IGNORECASE))
    per_reply += len(re.findall(r'^\s*\d+\s+bytes\s+from\b', out, re.IGNORECASE | re.MULTILINE))
    m = re.search(r'Received\s+(\d+)\s+response', out, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'(\d+)\s+packets\s+received.*?\((\d+)\s+extra\)', out, re.IGNORECASE)
    if m: return int(m.group(1))
    if per_reply: return per_reply
    m = re.search(r'(\d+)\s+packets\s+received', out, re.IGNORECASE)
    if m: return int(m.group(1))
    return 0

def is_problem_status(status: str) -> bool:
    s = (status or "").strip().upper()
    if s.startswith("SKIPPED"):
        return False
    return s not in {"UP", "SELF (UP)"}

# ============================================================
# Utilities for robust local IP/route inspection
# ============================================================
def _ip_bin() -> str:
    for p in ("/usr/sbin/ip", "/sbin/ip", shutil.which("ip")):
        if p:
            return p
    return "ip"

def _local_route_get(ip: str) -> str:
    ipbin = _ip_bin()
    try:
        proc = subprocess.run([ipbin, "-4", "route", "get", ip],
                              capture_output=True, text=True, timeout=5)
        out = (proc.stdout or "").strip()
        if out:
            return out.splitlines()[0]
    except Exception:
        pass
    return ""

def _iface_from_addrinfo(ip: str) -> str:
    ipbin = _ip_bin()
    try:
        p = subprocess.run([ipbin, "-j", "-4", "addr", "show"],
                           capture_output=True, text=True, timeout=5)
        if not p.stdout:
            return ""
        j = json.loads(p.stdout)
        target = _ip.ip_address(ip)
        best = ("", -1)
        for link in j:
            ifname = link.get("ifname", "")
            for a in link.get("addr_info", []):
                local = a.get("local"); plen = a.get("prefixlen")
                if not local or plen is None:
                    continue
                try:
                    net = _ip.ip_network(f"{local}/{plen}", strict=False)
                except Exception:
                    continue
                if target in net and plen > best[1]:
                    best = (ifname, plen)
        return best[0]
    except Exception:
        return ""

def _iface_exists_and_up(iface: str) -> bool:
    if not iface:
        return False
    ipbin = _ip_bin()
    try:
        proc = subprocess.run([ipbin, "-o", "link", "show", iface],
                              capture_output=True, text=True, timeout=3)
        if proc.returncode != 0:
            return False
        line = proc.stdout or ""
        return "state UP" in line or ",UP" in line or " UP" in line
    except Exception:
        return False

def _get_local_iface_for_ip(ip: str, verbose=False, print_lock=None) -> str:
    line = _local_route_get(ip)
    if verbose:
        with (print_lock or threading.Lock()):
            print(f"   -> local route: {line or '(none)'}")
    m = re.search(r"\bdev\s+([^\s]+)", line) if line else None
    if m:
        return m.group(1)

    dev = _iface_from_scope_link_route(ip)
    if dev:
        return dev

    dev = _iface_from_addrinfo(ip)
    if dev:
        return dev

    try:
        oct3 = ".".join(ip.split(".")[:3]) + "."
        proc = subprocess.run(
            ["bash","-lc",
             f"ifconfig | grep '{oct3}' -B 2 | awk '/^[a-z0-9]/ {{gsub(\":$\",\"\",$1); print $1}}'"],
            capture_output=True, text=True, timeout=5)
        lines = (proc.stdout or "").splitlines()
        if lines:
            return lines[0].strip()
    except Exception:
        pass
    return ""

def _local_is_onlink(ip: str) -> bool:
    line = _local_route_get(ip)
    if line and " via " not in line:
        return True
    if _iface_from_scope_link_route(ip):
        return True
    if _iface_from_addrinfo(ip):
        return True
    return False

# ============================================================
# Remote helpers (for ARP/Nmap from cpvr host)
# ============================================================
def _remote_is_local_ip(ssh, ip: str) -> bool:
    try:
        _, so, _ = ssh.exec_command(f"ip route get {ip} | sed -n '1p'", timeout=5)
        line = (so.read().decode() or "").strip().lower()
        if line.startswith("local "): return True
    except Exception:
        pass
    try:
        _, so, _ = ssh.exec_command("ip -j -4 addr show", timeout=5)
        data = so.read().decode()
        if data:
            for link in json.loads(data):
                for a in link.get("addr_info", []):
                    if a.get("local") == ip:
                        return True
    except Exception:
        pass
    return False

def _remote_pick_iface_for_ip(ssh, ip: str) -> str:
    try:
        _, so, _ = ssh.exec_command(f"ip route get {ip} | sed -n '1p'", timeout=5)
        line = (so.read().decode() or "").strip()
        m = re.search(r"\bdev\s+([^\s]+)", line)
        if m: return m.group(1)
    except Exception:
        pass
    try:
        _, so, _ = ssh.exec_command("ip -j -4 addr show", timeout=5)
        data = so.read().decode()
        if data:
            target = _ip.ip_address(ip)
            net24  = _ip.ip_network(str(target) + "/24", strict=False)
            for link in json.loads(data):
                ifname = link.get("ifname")
                for a in link.get("addr_info", []):
                    local = a.get("local")
                    if local and _ip.ip_address(local) in net24:
                        return ifname
    except Exception:
        pass
    return ""

def _detect_remote_arping_flag(ssh) -> str:
    try:
        _, so, se = ssh.exec_command("arping -h 2>&1 || arping --help 2>&1 || true", timeout=5)
        h = (so.read().decode() if so else "") + (se.read().decode() if se else "")
        if re.search(r"\s-i\s*<interface>", h) or " -i " in h:
            return "-i"
        if " -I " in h or re.search(r"\b-I\b", h):
            return "-I"
    except Exception:
        pass
    return "-I"

def _remote_arping_check(ssh, pod: int, ip: str, display_name: str, verbose: bool, print_lock, host: str):
    flag = _detect_remote_arping_flag(ssh)
    r_if = _remote_pick_iface_for_ip(ssh, ip)
    if r_if:
        cmd = f"arping -c 1 {flag} {r_if} {ip}"
    else:
        cmd = f"arping -c 1 {ip}"

    if verbose:
        with print_lock:
            print(f"   -> Running (remote arping on {host}): {cmd}")

    try:
        _, so, se = ssh.exec_command(cmd, timeout=10)
        txt = ((so.read().decode() if so else "") + (se.read().decode() if se else "")) or ""

        # robust success detection
        ok = (
            _count_arping_replies(txt) >= 1
            or "Unicast reply" in txt
            or re.search(r"\b1\s+packets?\s+received\b", txt, re.IGNORECASE)
        )
        status = "UP" if ok else "DOWN"
    except Exception as e:
        status = f"ERROR (remote arping: {e})"

    return {'pod': pod, 'component': display_name, 'ip': ip,
            'port': 'arping', 'status': status, 'host': host}

# ============================================================
# vCenter permission helpers
# ============================================================
def _get_role_map(si):
    """Return {roleId: roleName}"""
    try:
        am = si.RetrieveContent().authorizationManager
        return {r.roleId: r.name for r in (am.roleList or [])}
    except Exception:
        return {}

def _get_entity_permissions(si, entity, inherited=True):
    """Return list of vim.AuthorizationManager.Permission"""
    try:
        am = si.RetrieveContent().authorizationManager
        return list(am.RetrieveEntityPermissions(entity=entity, inherited=inherited) or [])
    except Exception:
        return []

# --- Add these helpers somewhere above fn_test_cp ----------------------------
import time
from pyVmomi import vmodl

def _pc_make_specs(container, obj_type, props):
    obj_spec  = vmodl.query.PropertyCollector.ObjectSpec(obj=container, skip=True,
                    selectSet=[vmodl.query.PropertyCollector.TraversalSpec(
                        name="tSpec", type=vim.view.ContainerView, path="view", skip=False,
                        selectSet=[]
                    )])
    prop_spec = vmodl.query.PropertyCollector.PropertySpec(type=obj_type,
                    pathSet=list(props), all=False)
    return [vmodl.query.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec])]

def _fast_power_counts_with_timeout(si, expected_names, timeout_s=10, verbose=False, print_lock=None):
    """
    Returns (on_count, off_count, power_map, timed_out: bool)
    Only collects 'name' and 'runtime.powerState' and stops once all expected are seen.
    Enforces a hard timeout by running the collector in a worker thread.
    """
    result = {"on": 0, "off": 0, "pm": {}, "timed_out": False, "err": None}

    def worker():
        try:
            content = si.RetrieveContent()
            view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
            try:
                pc = content.propertyCollector
                filt = pc.CreateFilter(_pc_make_specs(view, vim.VirtualMachine, {"name", "runtime.powerState"})[0], True)
                try:
                    # Initial page
                    r = pc.RetrievePropertiesEx(specSet=_pc_make_specs(view, vim.VirtualMachine, {"name", "runtime.powerState"}),
                                                options=vmodl.query.PropertyCollector.RetrieveOptions(maxObjects=200))
                    seen = 0
                    def consume(objs):
                        nonlocal seen
                        for o in objs or []:
                            nm = None; pstate = None
                            for p in o.propSet or []:
                                if p.name == "name": nm = p.val
                                elif p.name == "runtime.powerState": pstate = p.val
                            if not nm or nm not in expected_names:  # skip non-targets
                                continue
                            result["pm"][nm] = pstate
                            if pstate == vim.VirtualMachinePowerState.poweredOn:
                                result["on"] += 1
                            else:
                                result["off"] += 1
                            seen += 1
                        return seen

                    if r:
                        consume(r.objects)
                        # Early exit if we already have them all
                        while r and r.token and seen < len(expected_names):
                            r = pc.ContinueRetrievePropertiesEx(r.token)
                            consume(r.objects)

                finally:
                    try: pc.DestroyFilter(filt)
                    except Exception: pass
            finally:
                try: view.Destroy()
                except Exception: pass
        except Exception as e:
            result["err"] = e

    th = threading.Thread(target=worker, daemon=True)
    th.start()
    th.join(timeout_s)
    if th.is_alive():
        result["timed_out"] = True
        # Thread will die when process exits; we just stop waiting here.

    # Optional debug
    if verbose and (result["timed_out"] or result["err"]) and print_lock:
        with print_lock:
            if result["timed_out"]:
                print(f"   -> power-state collection timed out after {timeout_s}s")
            if result["err"]:
                print(f"   -> power-state collection error: {result['err']}")

    return result["on"], result["off"], result["pm"], result["timed_out"]

def _find_vm_folder_by_name(si, folder_name: str):
    """
    Return the vim.Folder (virtual machine folder) whose name == folder_name, or None if not found.
    Uses a ContainerView over Folder objects and checks name equality.
    """
    from pyVmomi import vim
    content = si.RetrieveContent()
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Folder], True)
    try:
        for f in (view.view or []):
            if getattr(f, "name", "") == folder_name:
                return f
    finally:
        try:
            view.Destroy()
        except Exception:
            pass
    return None

def _folder_vm_name_set(folder) -> set:
    """
    Given a vim.Folder that (logically) contains virtual machines,
    return the set of VM names found within it, recursing into child folders and vApps.

    Traversal logic:
      - If child is vim.VirtualMachine -> add its .name
      - If child is vim.Folder        -> recurse into its childEntity
      - If child is vim.VirtualApp    -> collect its .vm list (VM objects)
    """
    from pyVmomi import vim
    names = set()

    def walk(entity):
        try:
            # Virtual Machine
            if isinstance(entity, vim.VirtualMachine):
                nm = getattr(entity, "name", None)
                if nm:
                    names.add(nm)
                return
            # Folder -> recurse children
            if isinstance(entity, vim.Folder):
                for e in getattr(entity, "childEntity", []) or []:
                    walk(e)
                return
            # vApp -> take its VM list
            if isinstance(entity, vim.VirtualApp):
                for vm in getattr(entity, "vm", []) or []:
                    nm = getattr(vm, "name", None)
                    if nm:
                        names.add(nm)
                # Also walk any nested resource pools/folders a vApp may expose via .resourcePool
                for rp in getattr(entity, "resourcePool", []) or []:
                    for vm in getattr(rp, "vm", []) or []:
                        nm = getattr(vm, "name", None)
                        if nm:
                            names.add(nm)
                return
        except Exception:
            # Be defensive; skip entities we can't read
            return

    try:
        for e in getattr(folder, "childEntity", []) or []:
            walk(e)
    except Exception:
        pass

    return names

def _find_resource_pool_by_name(si, rp_name: str):
    """
    Return the vim.ResourcePool whose name == rp_name, or None if not found.
    Uses a ContainerView over ResourcePool objects.
    """
    from pyVmomi import vim
    content = si.RetrieveContent()
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
    try:
        for rp in (view.view or []):
            if getattr(rp, "name", "") == rp_name:
                return rp
    finally:
        try:
            view.Destroy()
        except Exception:
            pass
    return None
def _rp_vm_name_set(rp) -> set:
    """
    Given a vim.ResourcePool, return {vm.name, ...} for all VMs currently in the pool.
    (Does not recurse into child pools; add recursion if you use nested RPs.)
    """
    names = set()
    try:
        for vm in getattr(rp, "vm", []) or []:
            try:
                nm = getattr(vm, "name", None)
                if nm:
                    names.add(nm)
            except Exception:
                pass
    except Exception:
        pass
    return names

# ============================================================
# Core: permissions + VM power summary (with debug gating)
# ============================================================
# MODIFIED: fn_test_cp — adds an "RP Membership" row comparing expected vs. actual
def fn_test_cp(pod, si, group, verbose, debug, print_lock):
    """
    Returns:
      rows: [["Entity Type","Entity Name","Username","Role","Propagate","VM Count","Error"], ...]
            - Pod summary row ("N on / M off")
            - Permission rows (cp-pod{X}-folder, cp-pod{X})
            - RP Membership row (actual vs expected in cp-pod{X})
            - NEW: Folder Membership row (actual vs expected in cp-pod{X}-folder)
      power_map: { vm_name: powerState }
    """
    from pyVmomi import vim
    import concurrent.futures

    def vprint(msg):
        if verbose:
            with print_lock:
                print(msg, flush=True)

    def run_with_timeout(fn, seconds, *a, **kw):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fn, *a, **kw)
            try:
                return True, fut.result(timeout=seconds)
            except Exception as e:
                return False, e

    rows = []
    power_map = {}

    vprint(f"\n🔎 fn_test_cp(): starting for pod={pod}, group='{group}'")

    # --- 1) Expected names (from Mongo) ---
    try:
        comps, _ = fetch_course_config(pod, group, verbose, print_lock)
    except Exception as e:
        with print_lock:
            print(f"❌ fn_test_cp(): Mongo fetch failed for pod {pod}: {e}")
        rows.append(["Pod", f"Pod {pod}", "-", "-", "-", "0 on / 0 off", f"Mongo error: {e}"])
        return rows, power_map

    if verbose:
        with print_lock:
            print(f"   -> Mongo components for pod {pod}: {len(comps)} item(s)")

    expected_names = set()
    for _comp_name, clone_tmpl, _podip, _podport in comps:
        if clone_tmpl:
            vm_name = clone_tmpl.replace("{X}", str(pod))
            if vm_name:
                expected_names.add(vm_name)

    # --- 2) Count ON/OFF for expected VMs (PropertyCollector) ---
    content = si.RetrieveContent()

    def _count_power_states():
        vm_view = None
        try:
            content_local = si.RetrieveContent()
            pc = content_local.propertyCollector
            vm_view = content_local.viewManager.CreateContainerView(
                content_local.rootFolder, [vim.VirtualMachine], True
            )
            traversal = vim.PropertyCollector.TraversalSpec(
                name='tSpec', path='view', skip=False, type=vim.view.ContainerView
            )
            prop_spec = vim.PropertyCollector.PropertySpec(
                type=vim.VirtualMachine, pathSet=['name', 'runtime.powerState'], all=False
            )
            obj_spec = vim.PropertyCollector.ObjectSpec(
                obj=vm_view, skip=True, selectSet=[traversal]
            )
            filter_spec = vim.PropertyCollector.FilterSpec(
                objectSet=[obj_spec], propSet=[prop_spec]
            )
            options = vim.PropertyCollector.RetrieveOptions(maxObjects=200)
            result = pc.RetrievePropertiesEx(specSet=[filter_spec], options=options)

            by_name_state = {}
            def _ingest(obj_updates):
                for o in obj_updates or []:
                    name = None; pstate = None
                    for c in o.propSet or []:
                        if c.name == 'name': name = c.val
                        elif c.name == 'runtime.powerState': pstate = c.val
                    if name:
                        by_name_state[name] = pstate

            _ingest(getattr(result, 'objects', []))
            token = getattr(result, 'token', None)
            while token:
                result = pc.ContinueRetrievePropertiesEx(token=token)
                _ingest(getattr(result, 'objects', []))
                token = getattr(result, 'token', None)

            on = off = 0
            for name in expected_names:
                state = by_name_state.get(name)
                if state is None:
                    continue
                power_map[name] = state
                if state == vim.VirtualMachinePowerState.poweredOn:
                    on += 1
                else:
                    off += 1
            return on, off
        finally:
            try:
                if vm_view: vm_view.Destroy()
            except Exception:
                pass

    vprint("   -> Counting VM power states (fast props) with timeout...")
    ok, res = run_with_timeout(_count_power_states, 20)
    if ok:
        on, off = res
    else:
        with print_lock:
            print(f"   -> WARNING: power-state count timed out/failed for pod {pod}: {res}")
        on, off = 0, 0

    rows.append(["Pod", f"Pod {pod}", "-", "-", "-", f"{on} on / {off} off", ""])
    vprint(f"✅ fn_test_cp(): done power count for pod {pod} — {on} on / {off} off")

    # Optional compact debug
    try:
        debug_vm_power_map(pod, power_map, expected_names, verbose, print_lock)
    except NameError:
        pass

    # --- 3) Permissions (unchanged behavior) ---
    target_principal = f"VCENTER.REDEDUCATION.COM\\labcp-{pod}"
    target_role_name = "labcp-0-role"
    pod_folder_name  = f"cp-pod{pod}-folder"
    pod_rp_name      = f"cp-pod{pod}"

    try:
        am = content.authorizationManager
        role_map = {r.roleId: r.name for r in (am.roleList or [])}
    except Exception:
        am = None
        role_map = {}

    def _emit_if_match(entity_label, perm):
        principal = perm.principal or "-"
        role_name = role_map.get(perm.roleId, str(perm.roleId))
        if principal.upper() == target_principal.upper() and role_name == target_role_name:
            rows.append(["Permission", entity_label, principal, role_name,
                         "Y" if perm.propagate else "N", "", ""])

    def _find_folder_and_emit():
        if am is None: return
        fv = content.viewManager.CreateContainerView(content.rootFolder, [vim.Folder], True)
        try:
            for f in fv.view or []:
                if getattr(f, "name", "") == pod_folder_name:
                    try:
                        for p in am.RetrieveEntityPermissions(f, True) or []:
                            _emit_if_match(pod_folder_name, p)
                    except Exception as e:
                        rows.append(["Permission", pod_folder_name, "-", "-", "-", "", f"perm error: {e}"])
                    break
        finally:
            try: fv.Destroy()
            except Exception: pass

    def _find_rp_and_emit():
        if am is None: return
        rv = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
        try:
            for rp in rv.view or []:
                if getattr(rp, "name", "") == pod_rp_name:
                    try:
                        for p in am.RetrieveEntityPermissions(rp, True) or []:
                            _emit_if_match(pod_rp_name, p)
                    except Exception as e:
                        rows.append(["Permission", pod_rp_name, "-", "-", "-", "", f"perm error: {e}"])
                    break
        finally:
            try: rv.Destroy()
            except Exception: pass

    vprint("   -> Looking up folder permissions (timed)...")
    ok, err = run_with_timeout(_find_folder_and_emit, 20)
    if not ok and verbose:
        with print_lock:
            print(f"   -> Skipping folder permissions (timeout/err): {err}")

    vprint("   -> Looking up resource pool permissions (timed)...")
    ok, err = run_with_timeout(_find_rp_and_emit, 8)
    if not ok and verbose:
        with print_lock:
            print(f"   -> Skipping resource pool permissions (timeout/err): {err}")

    # --- 4) NEW: Resource Pool membership comparison ---
    try:
        rp = _find_resource_pool_by_name(si, pod_rp_name)
        if rp:
            actual_names = _rp_vm_name_set(rp)
            exp = expected_names
            missing = sorted(exp - actual_names)
            extra   = sorted(actual_names - exp)
            detail_parts = []
            if missing:
                detail_parts.append("missing: " + ", ".join(missing))
            if extra:
                detail_parts.append("extra: " + ", ".join(extra))
            detail = " | ".join(detail_parts)
            rows.append(["RP Membership", pod_rp_name, "-", "-", "-", f"{len(actual_names)} in RP / {len(exp)} expected", detail])
        else:
            rows.append(["RP Membership", pod_rp_name, "-", "-", "-", "", "resource pool not found"])
    except Exception as e:
        rows.append(["RP Membership", pod_rp_name, "-", "-", "-", "", f"rp check error: {e}"])

    # --- 5) NEW: Folder membership comparison ---
    try:
        f = _find_vm_folder_by_name(si, pod_folder_name)
        if f:
            folder_vm_names = _folder_vm_name_set(f)
            exp = expected_names
            missing = sorted(exp - folder_vm_names)
            extra   = sorted(folder_vm_names - exp)
            detail_parts = []
            if missing:
                detail_parts.append("missing: " + ", ".join(missing))
            if extra:
                detail_parts.append("extra: " + ", ".join(extra))
            detail = " | ".join(detail_parts)
            rows.append(["Folder Membership", pod_folder_name, "-", "-", "-", f"{len(folder_vm_names)} in folder / {len(exp)} expected", detail])
        else:
            rows.append(["Folder Membership", pod_folder_name, "-", "-", "-", "", "folder not found"])
    except Exception as e:
        rows.append(["Folder Membership", pod_folder_name, "-", "-", "-", "", f"folder check error: {e}"])

    vprint(f"✅ fn_test_cp(): done for pod {pod} — {on} on / {off} off")
    return rows, power_map

def perform_network_checks_local_nmap(pod, components, ignore_list, host_key, vm_power_map, verbose, print_lock):
    """
    Local-only network checks (used for Maestro groups):

    NEW BEHAVIOR:
      - Checks are driven only by podport:
          * If podport == 'arping' -> LOCAL ARP only
          * If podport is numeric  -> LOCAL TCP probe (nmap -Pn -p <port> <ip>)
      - No additional ARP/TCP(22) checks are performed implicitly.

      - Skip any component whose VM power state is OFF (per vm_power_map).
    """
    results = []

    for _, clone_tmpl, raw_ip, port in components:
        ip = resolve_pod_ip(raw_ip, pod, host_key)
        name = clone_tmpl.replace("{X}", str(pod)) if clone_tmpl else "Unknown"
        only_arp = isinstance(port, str) and str(port).lower() == "arping"

        # Skip powered-off VMs (from vCenter power map)
        if name in vm_power_map and vm_power_map[name] == vim.VirtualMachinePowerState.poweredOff:
            results.append({'pod': pod, 'component': name, 'ip': ip,
                            'port': port, 'status': 'SKIPPED (Powered Off)', 'host': 'localhost'})
            if verbose:
                with print_lock:
                    print(f"🔌 Skipping powered-off component: {name}")
            continue

        if verbose:
            with print_lock:
                print(f"   -> Component: {name}, raw_ip={raw_ip}, resolved_ip={ip}, port={port}")

        # --- podport == 'arping' → LOCAL ARP only ---
        if only_arp:
            arp_row = _local_arping_check(pod, ip, name, verbose, print_lock)
            results.append(arp_row)
            continue

        # --- podport should be numeric → LOCAL TCP on that port only ---
        if not (isinstance(port, int) or (isinstance(port, str) and port.isdigit())):
            results.append({'pod': pod, 'component': name, 'ip': ip,
                            'port': port, 'status': 'ERROR (invalid TCP port)', 'host': 'localhost'})
            continue

        try:
            pnum = int(port)
            cmd = ["bash", "-lc", f"nmap -Pn -p {pnum} {ip}"]
            if verbose:
                with print_lock:
                    print("   -> Running (local nmap):", " ".join(cmd))
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            out = proc.stdout or ""
            if f"{pnum}/tcp open" in out:
                status = "UP"
            elif f"{pnum}/tcp filtered" in out:
                status = "FILTERED"
            else:
                status = "DOWN"
        except subprocess.TimeoutExpired:
            status = "ERROR (nmap timeout)"
        except FileNotFoundError:
            status = "ERROR (nmap missing)"
        except Exception as e:
            status = f"ERROR ({e})"

        results.append({'pod': pod, 'component': name, 'ip': ip,
                        'port': int(port), 'status': status, 'host': 'localhost'})

    # Pretty print per-pod summary
    with print_lock:
        if results:
            headers = ["Component","IP","Host","Port","Status"]
            table = []
            for r in results:
                s = r['status']
                if is_problem_status(s):
                    s = f"{RED}{s}{END}"
                table.append([r['component'], r['ip'], r['host'], r['port'], s])
            print(f"\n📊 Network Test Summary for Pod {pod}")
            print(tabulate(table, headers=headers, tablefmt="fancy_grid"))

    return results

def perform_network_checks_over_ssh(pod, components, ignore_list, host_key, vm_power_map, verbose, print_lock):
    """
    Network checks driven only by podport:

      - If podport == 'arping':
          * Perform ARP.
              - For 172.26.* VRs, try LOCAL ARP first; if needed, fallback to
                connect.us.rededucation.com (with sudo -n). (Debug shows flag/iface/cmd/exit code.)
      - If podport is numeric:
          * Perform TCP check on that exact port (nmap -Pn -p <port> <ip>) from the cpvr host.
      - No implicit extra checks (no default TCP/22, no extra ARP unless requested).

      - Skip any component whose VM power state is OFF (from vm_power_map).
    """
    host = f"cpvr{pod}.us" if host_key.lower() in ["hotshot", "trypticon"] else f"cpvr{pod}"
    results = []

    mongo_off = set(ignore_list or [])

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        if verbose:
            with print_lock:
                print(f"\n🔐 Connecting to {host} via SSH...")
        client.connect(hostname=host, username='root',
                       key_filename='/usr/local/share/reded/cp.id_rsa', timeout=20)
        if verbose:
            with print_lock:
                print(f"✅ SSH to {host} successful")

        for _, clone_tmpl, raw_ip, port in components:
            ip = resolve_pod_ip(raw_ip, pod, host_key)
            name = clone_tmpl.replace("{X}", str(pod)) if clone_tmpl else "Unknown"
            only_arp = isinstance(port, str) and port.lower() == "arping"

            # Skip (Mongo powered-off)
            if name in mongo_off:
                results.append({'pod': pod, 'component': name, 'ip': ip,
                                'port': port, 'status': 'SKIPPED (Powered Off - Mongo)', 'host': host})
                continue

            # Skip (vCenter powered-off)
            if name in vm_power_map and vm_power_map[name] == vim.VirtualMachinePowerState.poweredOff:
                results.append({'pod': pod, 'component': name, 'ip': ip,
                                'port': port, 'status': 'SKIPPED (Powered Off)', 'host': host})
                if verbose:
                    with print_lock:
                        print(f"🔌 Skipping powered-off component: {name}")
                continue

            if verbose:
                with print_lock:
                    print(f"   -> Component: {name}, raw_ip={raw_ip}, resolved_ip={ip}, port={port}")

            # =========================
            # podport == 'arping' → ARP
            # =========================
            if only_arp:
                # Prefer local ARP for 172.26.* (keeps the prior reachability path)
                if str(ip).startswith("172.26."):
                    local_row = _local_arping_check(pod, ip, name, verbose, print_lock)
                    # If the local ARP completed (UP/DOWN/ERROR), use it; only fallback if we explicitly SKIPPED
                    if not local_row.get("status", "").startswith("SKIPPED"):
                        results.append(local_row)
                        continue

                    # Fallback: connect.us.rededucation.com with sudo -n
                    alt_client = None
                    try:
                        if verbose:
                            with print_lock:
                                print("   -> VR @172.26.*: using connect.us.rededucation.com for ARP (fallback)")

                        c_user = os.environ.get("CONNECT_ARP_USER", "daniel.storey")
                        c_key  = os.environ.get("CONNECT_ARP_KEY", "/home/daniel.storey/sharedaniel.storey/.ssh/id_rsa")

                        alt_client = paramiko.SSHClient()
                        alt_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        alt_client.connect(hostname="connect.us.rededucation.com",
                                           username=c_user, key_filename=c_key, timeout=20)

                        flag = _detect_remote_arping_flag(alt_client)
                        r_if = _remote_pick_iface_for_ip(alt_client, ip)

                        if verbose:
                            with print_lock:
                                print(f"   -> (connect) arping flag detected: {flag}")
                                print(f"   -> (connect) selected interface: {r_if or '(none)'}")

                        cmd = f"sudo -n arping -c 1 {flag} {r_if} {ip}" if r_if else f"sudo -n arping -c 1 {ip}"
                        if verbose:
                            with print_lock:
                                print(f"   -> (connect) running: {cmd}")

                        stdin, stdout, stderr = alt_client.exec_command(cmd, timeout=15)
                        out_txt = (stdout.read().decode() if stdout else "")
                        err_txt = (stderr.read().decode() if stderr else "")
                        exit_status = stdout.channel.recv_exit_status()  # safe after reads; see Paramiko docs/notes. :contentReference[oaicite:0]{index=0}

                        if verbose:
                            with print_lock:
                                print(f"   -> (connect) exit_status: {exit_status}")
                                if out_txt.strip():
                                    print("   -> (connect) stdout:\n" + out_txt.strip())
                                if err_txt.strip():
                                    print("   -> (connect) stderr:\n" + err_txt.strip())

                        txt = (out_txt + ("\n" + err_txt if err_txt else "")).strip()
                        ok = (
                            _count_arping_replies(txt) >= 1
                            or "Unicast reply" in txt
                            or re.search(r"\b1\s+packets?\s+received\b", txt, re.IGNORECASE)
                            or exit_status == 0
                        )
                        status = "UP" if ok else "DOWN"
                        results.append({'pod': pod, 'component': name, 'ip': ip,
                                        'port': 'arping', 'status': status, 'host': "connect.us.rededucation.com"})
                    except Exception as e:
                        if verbose:
                            with print_lock:
                                print(f"   -> (connect) fallback error: {e}")
                        results.append({'pod': pod, 'component': name, 'ip': ip,
                                        'port': 'arping', 'status': 'SKIPPED (connect ARP unavailable)',
                                        'host': "connect.us.rededucation.com"})
                    finally:
                        try:
                            if alt_client:
                                alt_client.close()
                        except Exception:
                            pass
                    continue

                # Non-172.26.*: perform ARP from cpvr host
                arp_row = _remote_arping_check(client, pod, ip, name, verbose, print_lock, host)
                results.append(arp_row)
                continue

            # ====================================
            # podport is numeric → TCP on that port
            # ====================================
            if not (isinstance(port, int) or (isinstance(port, str) and port.isdigit())):
                results.append({'pod': pod, 'component': name, 'ip': ip,
                                'port': port, 'status': 'ERROR (invalid TCP port)', 'host': host})
                continue

            pnum = int(port)
            cmd = f"nmap -Pn -p {pnum} {ip}"
            if verbose:
                with print_lock:
                    print("   -> Running:", cmd)
            try:
                _, so, _ = client.exec_command(cmd, timeout=20)
                txt = so.read().decode() if so else ""
                if f"{pnum}/tcp open" in txt:
                    status = "UP"
                elif f"{pnum}/tcp filtered" in txt:
                    status = "FILTERED"
                else:
                    status = "DOWN"
            except Exception as e:
                status = f"ERROR ({e})"

            results.append({'pod': pod, 'component': name, 'ip': ip,
                            'port': pnum, 'status': status, 'host': host})

    except Exception as e:
        with print_lock:
            print(f"❌ SSH or command execution failed on {host}: {e}")
        results.append({'pod': pod, 'component': 'SSH Connection', 'ip': host,
                        'port': 22, 'status': 'FAILED', 'host': host})
    finally:
        client.close()

    # Pretty print per-pod summary
    with print_lock:
        if results:
            headers = ["Component","IP","Host","Port","Status"]
            table = []
            for row in results:
                if not isinstance(row, dict):
                    continue
                status = row.get('status', 'UNKNOWN')
                status_print = f"{RED}{status}{END}" if is_problem_status(status) else status
                table.append([row.get('component','UNKNOWN'),
                              row.get('ip','UNKNOWN'),
                              row.get('host','UNKNOWN'),
                              row.get('port','UNKNOWN'),
                              status_print])
            print(f"\n📊 Network Test Summary for Pod {pod}")
            print(tabulate(table, headers=headers, tablefmt="fancy_grid"))

    return results

# ---------- FAST VM INVENTORY (name + power) ----------
from datetime import datetime

from pyVmomi import vmodl

# NEW: find a Resource Pool by exact name (lightweight ContainerView scan)
def _find_resource_pool_by_name(si, rp_name: str):
    """
    Return the vim.ResourcePool whose name == rp_name, or None if not found.
    Uses a ContainerView over ResourcePool objects.
    """
    from pyVmomi import vim
    content = si.RetrieveContent()
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
    try:
        for rp in (view.view or []):
            if getattr(rp, "name", "") == rp_name:
                return rp
    finally:
        try:
            view.Destroy()
        except Exception:
            pass
    return None

# NEW: get the set of VM names currently in a Resource Pool
def _rp_vm_name_set(rp) -> set:
    """
    Given a vim.ResourcePool, return {vm.name, ...} for all VMs currently in the pool.
    """
    names = set()
    try:
        for vm in getattr(rp, "vm", []) or []:
            try:
                names.add(getattr(vm, "name", None))
            except Exception:
                pass
    except Exception:
        pass
    return {n for n in names if n}

def _list_vms_fast(si, want_names, verbose, print_lock, timeout_sec=30):
    """
    Return (matched_vms:list[vim.VirtualMachine], power_map:dict)
    - Fetches ONLY 'name' and 'runtime.powerState' via PropertyCollector
    - Stops early when all names in want_names are found
    - Enforced hard timeout
    """
    want_names = set(want_names or [])
    found_map = {}   # name -> (vm_ref, power_state)

    def _collect():
        content = si.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.VirtualMachine], True
        )
        try:
            pc = content.propertyCollector
            obj_specs = [vmodl.query.PropertyCollector.ObjectSpec(obj=view, skip=True,
                         selectSet=[vmodl.query.PropertyCollector.TraversalSpec(
                            name='tSpec', type=type(view),
                            path='view', skip=False
                         )])]
            prop_specs = [vmodl.query.PropertyCollector.PropertySpec(
                type=vim.VirtualMachine, pathSet=['name', 'runtime.powerState'], all=False
            )]
            spec = vmodl.query.PropertyCollector.FilterSpec(objectSet=obj_specs, propSet=prop_specs)
            options = vmodl.query.PropertyCollector.RetrieveOptions(maxObjects=500)

            r = pc.RetrievePropertiesEx(specSet=[spec], options=options)
            while True:
                if r is None or not getattr(r, 'objects', None):
                    break
                for o in r.objects:
                    vm = o.obj
                    name = None
                    pstate = None
                    for dp in o.propSet:
                        if dp.name == 'name':
                            name = dp.val
                        elif dp.name == 'runtime.powerState':
                            pstate = dp.val
                    if name:
                        if (not want_names) or (name in want_names):
                            found_map[name] = (vm, pstate)
                # stop early if we’ve found everything we wanted
                if want_names and want_names.issubset(found_map.keys()):
                    break
                if not getattr(r, 'token', None):
                    break
                r = pc.ContinueRetrievePropertiesEx(r.token)
        finally:
            try:
                view.Destroy()
            except Exception:
                pass

    # Enforce a hard timeout so we never hang
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_collect)
        try:
            fut.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            with print_lock:
                print(f"⏱️  vCenter inventory timed out after {timeout_sec}s — proceeding with partial/empty results")

    matched_vms = []
    power_map = {}
    for name, (vm, pstate) in found_map.items():
        matched_vms.append(vm)
        power_map[name] = pstate
    return matched_vms, power_map


# ============================================================
# Local / remote ARP + TCP checks
# ============================================================

def _local_vr_checks_tcp(ip: str, tcp_port):
    if isinstance(tcp_port, int) or (isinstance(tcp_port, str) and tcp_port.isdigit()):
        p = int(tcp_port)
    else:
        p = 22
    cmd = ["bash", "-lc", f"nmap -Pn -p {p} {ip}"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        out = proc.stdout or ""
        if f"{p}/tcp open" in out:
            status = "UP"
        elif f"{p}/tcp filtered" in out:
            status = "FILTERED"
        else:
            status = "DOWN"
    except subprocess.TimeoutExpired:
        status = "ERROR (nmap timeout)"
    except FileNotFoundError:
        status = "ERROR (nmap missing)"
    except Exception as e:
        status = f"ERROR ({e})"
    return p, status

def _is_vr_component(name: str) -> bool:
    if not name: return False
    n = name.lower()
    return ("-vr-" in n) or bool(re.search(r'(^|[-_])vr([-_]|$)', n))

# ============================================================
# Network checks
# ============================================================


def _ssh_run(client, cmd, timeout=20):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = (stdout.read() or b"").decode("utf-8", "ignore")
    err = (stderr.read() or b"").decode("utf-8", "ignore")
    return (out + ("\n" + err if err.strip() else "")).strip()


def _local_arping_check(pod, ip, name, verbose, print_lock):
    """
    Local ARP reachability check that ALWAYS returns a dict with keys:
      'pod', 'component', 'ip', 'port', 'status', 'host'

    Args:
      pod (int or str): pod identifier
      ip (str): IP address
      name (str): component name
      verbose (bool): whether to print verbose logs
      print_lock (threading.Lock): lock for printing safely in threads

    Returns:
      dict: {'pod', 'component', 'ip', 'port', 'status', 'host'}
    """
    host = "localhost"
    status = "UNKNOWN"
    try:
        # Try to pick a local interface for this IP
        iface = _get_local_iface_for_ip(ip, verbose=verbose, print_lock=print_lock) or ""
        flag = "-I"  # interface flag for arping; fallback if iface is empty
        cmd = ["sudo", "arping", "-c", "2"]
        if iface:
            cmd += [flag, iface]
        cmd.append(ip)

        if verbose:
            with print_lock:
                print(f"   -> Running (local arping): {' '.join(cmd)}")

        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
        out = (proc.stdout or "") + (("\n" + proc.stderr) if proc.stderr else "")

        # Use helper to count replies or test returncode
        replies = _count_arping_replies(out)
        if proc.returncode == 0 or replies >= 1:
            status = "UP"
        else:
            status = "DOWN"

    except subprocess.TimeoutExpired:
        status = "ERROR (arping timeout)"
    except FileNotFoundError:
        status = "ERROR (arping missing)"
    except Exception as e:
        status = f"ERROR (arping: {e})"

    row = {
        "pod": pod,
        "component": name,
        "ip": ip,
        "port": "arping",
        "status": status,
        "host": host
    }

    if verbose:
        with print_lock:
            print(f"   -> ARP check for {name} @{ip}: {status}")

    return row

def resolve_pod_ip(ip_raw: str, pod: int, host_key: str) -> str:
    ip = ip_raw.replace("{X}", str(pod))
    if "+X" in ip:
        base, _, _ = ip.partition("+X")
        octs = base.split(".")
        try:
            octs[-1] = str(int(octs[-1]) + pod)
            ip = ".".join(octs)
        except Exception:
            ip = base
    is_us = host_key.lower() in ["hotshot", "trypticon"]
    parts = ip.split(".")
    if is_us and len(parts) == 4 and parts[1] == "30":
        parts[1] = "26"
        ip = ".".join(parts)
    return ip

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
        mongo_powered_off = set()

        for c in course["components"]:
            if _is_truthy(c.get("ignore")):
                if verbose:
                    with print_lock:
                        print(f"⏭️  Skipping ignored component: {c.get('component_name', 'UNKNOWN')}")
                continue

            # Detect “powered off” from Mongo (support a few field spellings)
            state = str(c.get("state", "")).strip().lower()
            is_po = (
                _is_truthy(c.get("poweroff")) or
                _is_truthy(c.get("powered_off")) or
                state in {"poweroff","poweredoff","off","down"}
            )

            clone = c.get("clone_name") or ""
            if clone:
                vm_name = clone.replace("{X}", str(pod))
                if is_po and vm_name:
                    mongo_powered_off.add(vm_name)

            if c.get("podip") and c.get("podport"):
                components.append((c.get("component_name"), clone, c.get("podip"), c.get("podport")))
            elif verbose:
                with print_lock:
                    print(f"⚠️ Skipping incomplete component config: {c.get('component_name', 'UNKNOWN')}")

        return components, list(mongo_powered_off)
    except Exception as e:
        with print_lock:
            print(f"❌ MongoDB query failed: {e}")
        return [], []

# top-level (globals)
_VC_GUARD = threading.Semaphore(int(os.environ.get("VC_CONCURRENCY", "3")))

def threaded_fn_test_cp(pod, vcenter_host, group, verbose, debug, print_lock):
    """
    Thread worker:
      - Connects to vCenter
      - Runs fn_test_cp(pod, ...)
      - Always returns (pod, rows, power_map) even on failure
    """
    try:
        if verbose:
            with print_lock:
                print(f"🌐 vCenter[{vcenter_host}] → pod {pod}: connecting...", flush=True)

        si = SmartConnect(
            host=vcenter_host,
            user="administrator@vcenter.rededucation.com",
            pwd="pWAR53fht786123$",
            sslContext=context,
        )

        try:
            # Optional: show which fn_test_cp is active
            try:
                _debug_which_fn(fn_test_cp, verbose, print_lock)
            except Exception:
                pass

            rows, power_map = fn_test_cp(pod, si, group, verbose, debug, print_lock)
        finally:
            Disconnect(si)

        return pod, rows, power_map

    except Exception as e:
        with print_lock:
            print(f"❌ Error connecting to vCenter for pod {pod}: {e}")
        # Keep the executor pipeline healthy: always return a tuple
        return pod, [], {}


def _fast_power_counts_with_timeout(si, expected_names, timeout_s=30, verbose=False, print_lock=None):
    result = {"on":0,"off":0,"pm":{},"timed_out":False,"err":None}

    def worker():
        try:
            with _VC_GUARD:  # <<< limit concurrent PC traffic
                content = si.RetrieveContent()
                view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
                try:
                    pc = content.propertyCollector
                    filt = pc.CreateFilter(_pc_make_specs(view, vim.VirtualMachine, {"name","runtime.powerState"})[0], True)
                    try:
                        r = pc.RetrievePropertiesEx(
                            specSet=_pc_make_specs(view, vim.VirtualMachine, {"name","runtime.powerState"}),
                            options=vmodl.query.PropertyCollector.RetrieveOptions(maxObjects=200)
                        )
                        # ... (rest unchanged)
                    finally:
                        try: pc.DestroyFilter(filt)
                        except Exception: pass
                finally:
                    try: view.Destroy()
                    except Exception: pass
        except Exception as e:
            result["err"] = e

def run_with_timeout(fn, seconds, *a, **kw):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *a, **kw)
        try:
            return True, fut.result(timeout=seconds)
        except Exception as e:
            return False, e

# globals
_POWER_CACHE = {}
_POWER_CACHE_LOCK = threading.Lock()

def get_full_power_map(si):
    # one bulk pull for name + power
    content = si.RetrieveContent()
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    try:
        pc = content.propertyCollector
        prop_spec = vmodl.query.PropertyCollector.PropertySpec(
            type=vim.VirtualMachine, pathSet=['name','runtime.powerState'], all=False)
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec(
            obj=view, skip=True,
            selectSet=[vmodl.query.PropertyCollector.TraversalSpec(
                name='tSpec', type=vim.view.ContainerView, path='view', skip=False)])
        spec = vmodl.query.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec])
        opts = vmodl.query.PropertyCollector.RetrieveOptions(maxObjects=500)
        by_name = {}
        with _VC_GUARD:
            r = pc.RetrievePropertiesEx(specSet=[spec], options=opts)
            while r:
                for o in getattr(r, 'objects', []) or []:
                    name = None; pstate = None
                    for p in o.propSet or []:
                        if p.name == 'name': name = p.val
                        elif p.name == 'runtime.powerState': pstate = p.val
                    if name: by_name[name] = pstate
                token = getattr(r, 'token', None)
                if not token: break
                r = pc.ContinueRetrievePropertiesEx(token)
        return by_name
    finally:
        try: view.Destroy()
        except Exception: pass

def get_cached_power_map(si, vcenter_host):
    with _POWER_CACHE_LOCK:
        pm = _POWER_CACHE.get(vcenter_host)
        if pm: return pm
    pm = get_full_power_map(si)
    with _POWER_CACHE_LOCK:
        _POWER_CACHE[vcenter_host] = pm
    return pm


def get_cached_power_map(si, vcenter_host):
    with _POWER_CACHE_LOCK:
        pm = _POWER_CACHE.get(vcenter_host)
        if pm: return pm
    pm = get_full_power_map(si)
    with _POWER_CACHE_LOCK:
        _POWER_CACHE[vcenter_host] = pm
    return pm


def local_pick_iface(target, subnet_prefix="192.168.21."):
    # Try route first
    try:
        r = subprocess.check_output(["ip","-j","route","get",target], text=True)
        dev = json.loads(r)[0].get("dev")
    except Exception:
        dev = None
    # Validate that dev owns the right subnet
    if dev:
        a = subprocess.check_output(["ip","-j","addr","show",dev], text=True)
        infos = json.loads(a)[0].get("addr_info", [])
        if any(ai.get("local","").startswith(subnet_prefix) for ai in infos):
            return dev
    # Fallback: scan all ifaces for a matching local IP
    a = subprocess.check_output(["ip","-j","addr","show"], text=True)
    for link in json.loads(a):
        if any(ai.get("local","").startswith(subnet_prefix) for ai in link.get("addr_info", [])):
            return link["ifname"]
    return None

def local_arp_ok(target):
    iface = local_pick_iface(target, "192.168.21.")
    cmd = ["sudo","arping","-I",iface,"-c","2","-W","0.5","-w","2",target] if iface else ["sudo","arping","-c","2","-W","0.5","-w","2",target]
    return subprocess.run(cmd).returncode == 0

def _is_truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"true", "1", "yes", "y"}

# ============================================================
# Main
# ============================================================
def main(argv=None, print_lock=None):
    parser = argparse.ArgumentParser(description="CP Maestro-aware pod checks with local/remote nmap and ARP")
    parser.add_argument("-s","--start", type=int, required=True)
    parser.add_argument("-e","--end", type=int, required=True)
    parser.add_argument("-H","--host", required=True)
    parser.add_argument("-g","--group", required=True)
    parser.add_argument("-v","--verbose", action="store_true", help="Show summaries and progress info")
    parser.add_argument("--debug", action="store_true", help="Dump full permission rows (very verbose)")
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
    has_failures = False

    if args.verbose:
        print(f"🌐 Connecting to vCenter: {vcn}", flush=True)

    # ---------- 1) vCenter scans FIRST: build power_maps (+ rows) ----------
    with concurrent.futures.ThreadPoolExecutor(max_workers=VC_MAX_WORKERS) as exe:
        futures = [exe.submit(threaded_fn_test_cp, pod, vcn, args.group, args.verbose, args.debug, lock)
                   for pod in pods]
        for fut in futures:
            pod_id, rows, pmap = fut.result()
            power_maps[pod_id] = pmap or {}
            full_rows.extend(rows)

    # ---------- 2) Print summary of permissions / VM count ----------
    with lock:
        print("\n📊 Permissions and VM Count Summary")
        print(tabulate(full_rows,
                       headers=["Entity Type","Entity Name","Username","Role","Propagate","VM Count","Error"],
                       tablefmt="fancy_grid"))

    perm_error_rows = [r for r in full_rows if isinstance(r, list) and r[-1]]
    if perm_error_rows:
        has_failures = True

    # ---------- 3) Choose network-check function ----------
    is_maestro = args.group.lower().startswith("maestro-")
    func = perform_network_checks_local_nmap if is_maestro else perform_network_checks_over_ssh

    # ---------- 4) Network checks ----------
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe:
        futures = []
        for pod in pods:
            comps, _mongo_po = fetch_course_config(pod, args.group, args.verbose, lock)

            # Optional component filter applies to NETWORK checks only
            if args.component:
                want = set(c.strip() for c in args.component.split(","))
                comps = [c for c in comps if c[0] in want]
                if not comps:
                    with lock:
                        print(f"   - Pod {pod} filtered out; skipping net check")
                    continue

            futures.append(exe.submit(
                func, pod, comps, _mongo_po, args.host, power_maps.get(pod, {}), args.verbose, lock
            ))

        for fut in futures:
            res = fut.result()
            if res:
                net_results.extend(res)

    # ---------- 5) Suppress ARP DOWN if TCP UP for same component ----------
    if net_results:
        by_comp = {}
        for r in net_results:
            key = (r["pod"], r["component"])
            by_comp.setdefault(key, []).append(r)

        filtered = []
        for key, rows in by_comp.items():
            tcp_up = any(
                (isinstance(x["port"], int) or str(x["port"]).isdigit()) and x["status"] == "UP"
                for x in rows
            )
            for x in rows:
                if x["port"] == "arping" and x["status"] == "DOWN" and tcp_up:
                    continue
                filtered.append(x)
        net_results = filtered

    # ---------- 6) Print combined network summary & set exit code ----------
    if net_results:
        errors = False
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

    if has_failures:
        errors = True
        if perm_error_rows:
            print("\n🚨 Errors: Permissions / VM Count")
            print(tabulate(
                perm_error_rows,
                headers=["Entity Type","Entity Name","Username","Role","Propagate","VM Count","Error"],
                tablefmt="fancy_grid"
            ))

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

    print("\n✅ No errors found.")
    # after printing summaries:
    if errors == False:
        return [{"status": "success", "detail": "network checks OK"}]
    else:
        return [{"status": "failed", "detail": "network checks had errors"}]
    sys.exit(0)

# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    main()
