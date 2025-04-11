#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import time
import getpass
import datetime
import re
import shutil

# Global variables (set later in main)
debug = 0
name = os.path.basename(sys.argv[0])
home = "/usr/local/share/reded/f5"
repo = "lab"
host = None
lic = None
bak = None
log_file = None
tmp = None

def usage():
    """Print usage message and exit."""
    global name
    if debug:
        print("Debug mode on: printing more details")
    print(f"\nusage: {name} startpod endpod host [type]")
    print("type = apm|bigiq|lab, lab is the default\n")
    sys.exit(1)

def error(msg):
    """Log an error message and exit."""
    log_msg(msg)
    sys.exit(1)

def log_msg(msg):
    """Print message and append it to the log file."""
    print(msg)
    with open(log_file, "a") as lf:
        lf.write(msg + "\n")

def run_command(command, capture_output=False, quiet=False):
    """
    Helper to run a shell command.
    If quiet is True, stdout/stderr are suppressed.
    """
    if debug:
        print("Running command:", " ".join(command))
    try:
        result = subprocess.run(command,
                                stdout=subprocess.PIPE if capture_output or quiet else None,
                                stderr=subprocess.PIPE if capture_output or quiet else None,
                                check=True,
                                text=True)
        return result.stdout if capture_output else ""
    except subprocess.CalledProcessError as e:
        if not quiet:
            log_msg(f"Command failed: {' '.join(command)}")
        return None

def check_num(start, end):
    """Check if there are enough license keys available."""
    # Calculate number of keys required (assuming 1-indexed pods)
    num_required = (end - start + 1)
    try:
        with open(lic, "r") as f:
            avail = len(f.readlines())
    except Exception as e:
        error(f"Failed to read license file: {e}")
    if num_required > avail:
        return False
    return True

def check_pod(pod, bigip_path, repo):
    """
    Check if a license already exists on the remote bigip machine.
    Returns True if the check passes (i.e. license needs to be pushed).
    """
    # Check if local license file exists for the pod.
    if not os.path.exists(bigip_path + ".license"):
        log_msg(f"License does not exist for {bigip_path}... skipping!")
        return False

    # Download remote license file to tmp file
    remote_host = f"f5-{repo}-{pod}"
    tmp_file = tmp  # using global tmp
    scp_cmd = ["scp", "-q", f"{remote_host}:/config/bigip.license", tmp_file]
    result = run_command(scp_cmd, quiet=True)
    if result is not None:
        # Compare the downloaded file with the local one.
        try:
            if filecmp(tmp_file, bigip_path + ".license"):
                log_msg(f"License already exists on {bigip_path}.. skipping!")
                return False
        except Exception as e:
            log_msg(f"Error comparing license files: {e}")
    return True

def filecmp(file1, file2):
    """Compare two files and return True if they are the same."""
    try:
        with open(file1, "rb") as f1, open(file2, "rb") as f2:
            return f1.read() == f2.read()
    except Exception:
        return False

def alloc_license(start_pod, num_keys):
    """
    Lock the license file, extract keys, update the file, and write keys
    to the appropriate bigip files.
    """
    global tmp, lic, home, host
    lic_lock = lic + ".lock"
    # Wait until lock is released
    while os.path.exists(lic_lock):
        print("Waiting for licenses to be available...")
        time.sleep(10)
    # Create lock file
    open(lic_lock, "w").close()

    # Read current licenses
    with open(lic, "r") as f:
        lines = f.readlines()
    # Backup license file
    shutil.copy(lic, bak)
    if num_keys > len(lines):
        num_keys = len(lines)
    # Update license file by removing the allocated keys
    remaining = lines[num_keys:]
    with open(lic, "w") as f:
        f.writelines(remaining)
    # Remove lock file
    os.remove(lic_lock)

    # Write allocated keys to a temporary file
    allocated = lines[:num_keys]
    tmp_lic = tmp + ".lic"
    with open(tmp_lic, "w") as f:
        f.writelines(allocated)

    pod = start_pod
    # For each key, allocate license to the corresponding bigip file.
    with open(tmp_lic, "r") as f:
        for key in f:
            bigip_path = os.path.join(home, host, f"bigip{pod}")
            log_msg(f"Allocating {bigip_path} on VM host {host} the key {key.strip()}")
            # If a license already exists, back it up.
            if os.path.exists(bigip_path):
                shutil.copy(bigip_path, bigip_path + ".backup")
            with open(bigip_path, "w") as bf:
                bf.write(key)
            pod += 1

    # Clean up temporary files
    if os.path.exists(tmp):
        os.remove(tmp)
    if os.path.exists(tmp_lic):
        os.remove(tmp_lic)

def deploy_license(pod, extra_arg=None):
    """
    Deploy license on the remote machine.
    If extra_arg is "17", use special commands.
    """
    global home, host, repo
    if extra_arg == "17":
        remote_host = f"f5m17-{pod}"  # Here pod serves as extra host identifier
        # Move old license file remotely
        run_command(["ssh", remote_host, "mv", "bigip.license", "bigip.license.old"], quiet=True)
        local_file = os.path.join(home, host, "bigipf5m17-1.id")
        # Transfer new license
        scp_res = run_command(["scp", "-q", local_file, f"{remote_host}:/config/bigip.license"], quiet=True)
        if scp_res is None:
            log_msg(f"Problem with transferring license to {remote_host}... skipping!")
            return False
        # Reload license on remote machine
        if run_command(["ssh", "-q", remote_host, "reloadlic"], quiet=True) is None:
            log_msg(f"Problem with loading license on {remote_host}... skipping!")
            return False
        else:
            print("done.")
    else:
        remote_host = f"f5-{repo}-{pod}"
        run_command(["ssh", remote_host, "mv", "bigip.license", "bigip.license.old"], quiet=True)
        bigip_license = os.path.join(home, host, f"bigip{pod}.license")
        scp_res = run_command(["scp", "-q", bigip_license, f"{remote_host}:/config/bigip.license"], quiet=True)
        if scp_res is None:
            log_msg(f"Problem with transferring license to bigip{pod}... skipping!")
            return False
        if run_command(["ssh", "-q", remote_host, "reloadlic"], quiet=True) is None:
            log_msg(f"Problem with loading license on bigip{pod}... skipping!")
            return False
        else:
            print("done.")
    return True

def mkf5lic(start_pod, end_pod):
    """Activate license for each pod."""
    global home, host, repo
    for pod in range(start_pod, end_pod + 1):
        bigip_path = os.path.join(home, host, f"bigip{pod}")
        # Read the last line (key) from the file
        try:
            with open(bigip_path, "r") as bf:
                lines = bf.readlines()
                key = lines[-1].strip() if lines else ""
        except Exception as e:
            log_msg(f"Error reading {bigip_path}: {e}")
            continue
        print(f"Activating license for Pod {pod}")
        run_command(["ssh", f"f5-{repo}-{pod}", "SOAPLicenseClient", "--basekey", key], quiet=True)
        print(f"Copying license back to {bigip_path}.license")
        run_command(["scp", "-q", f"f5-{repo}-{pod}:/config/bigip.license", bigip_path + ".license"], quiet=True)

def savef5lic(start_pod, end_pod):
    """Copy license file from remote to local for each pod."""
    global home, host
    for pod in range(start_pod, end_pod + 1):
        bigip_path = os.path.join(home, host, f"bigip{pod}")
        print(f"Copying license from f5m{pod}:/config/bigip.license to {bigip_path}.license")
        # Check existence of remote license file
        ssh_cmd = ["ssh", f"f5m{pod}", "ls", "-l", "/config/bigip.license"]
        if run_command(ssh_cmd, quiet=True) is None:
            print(f"License file missing on f5m{pod}")
            continue
        scp_cmd = ["scp", "-q", f"f5m{pod}:/config/bigip.license", bigip_path + ".license"]
        if run_command(scp_cmd, quiet=True) is None:
            print(f"Failed to copy license for f5m{pod}")
            continue
        print(f"Successfully copied license for Pod {pod}.")

def pushlic(arg1, arg2=None):
    """
    Push license to remote machines.
    If first argument is "17", handle that branch.
    Otherwise, iterate over a range of pods.
    """
    global home, host, repo
    if arg1 == "17":
        # Here, arg2 is expected to be the identifier for the remote machine
        pod = None  # not used in this branch
        bigip_path = os.path.join(home, host, f"bigip{arg2}")
        tmp_file = os.path.join("/tmp", f".{host}.bigip{arg2}")
        log_file_local = f"bigipf5m17-{arg2}.log"
        # Remove remote license file
        run_command(["ssh", f"f5m17-{arg2}", "rm", "-rf", "/config/bigip.license"], quiet=True)
        # Check and deploy license if needed
        # (In this simplified version, we assume check_pod passes)
        deploy_license(arg2, extra_arg="17")
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
    else:
        try:
            start = int(arg1)
            end = int(arg2)
        except Exception as e:
            error("Invalid pod numbers for pushlic.")
        for pod in range(start, end + 1):
            print(f"Pushing license to bigip {pod}")
            bigip_path = os.path.join(home, host, f"bigip{pod}")
            tmp_file = os.path.join("/tmp", f".{host}.bigip{pod}")
            log_file_local = f"bigip{pod}.log"
            run_command(["ssh", f"f5m{pod}", "rm", "-rf", "/config/bigip.license"], quiet=True)
            # Check and deploy license
            if check_pod(pod, bigip_path, repo):
                deploy_license(pod)
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

def allocf5keys(start_pod, end_pod):
    """Allocate f5 license keys for a range of pods."""
    global lic
    num_keys = end_pod - start_pod + 1
    try:
        with open(lic, "r") as f:
            avail = len(f.readlines())
    except Exception as e:
        error(f"Failed to read license file: {e}")
    if num_keys > avail:
        num_keys = avail
        if num_keys > 0:
            print(f"Fetching {num_keys} keys for pods {start_pod} to {start_pod + num_keys - 1}")
        alloc_license(start_pod, num_keys)
        print(f"Insufficient licenses for pods {start_pod + num_keys} to {end_pod}")
    else:
        print(f"Fetching {num_keys} keys for pods {start_pod} to {end_pod}")
        alloc_license(start_pod, num_keys)
    print()

def pastef5keys_bigiq():
    """Paste text containing license keys now. Ends with EOF (Ctrl-D)."""
    global lic, bak
    print("\nPaste text containing license keys now")
    print("end with Enter, Control-D\n")
    # Backup current license file
    shutil.copy(lic, bak)
    # Read from standard input until EOF
    input_text = sys.stdin.read()
    # Extract license keys using regex
    pattern = re.compile(r".*(\w{5}-\w{6}-\w{3}-\w{7}-\w{7}).*")
    for line in input_text.splitlines():
        m = pattern.match(line)
        if m:
            key = m.group(1)
            print(key)
            with open(lic, "a") as f:
                f.write(key + "\n")

def pastef5keys_apm():
    """For apm, call lab version."""
    pastef5keys_lab()

def pastef5keys_lab():
    """Paste license keys with lab regex."""
    global lic, bak
    print("\nPaste text containing license keys now")
    print("end with Enter, Control-D\n")
    shutil.copy(lic, bak)
    input_text = sys.stdin.read()
    pattern = re.compile(r".*(\w{5}-\w{5}-\w{5}-\w{5}-\w{7}).*")
    for line in input_text.splitlines():
        m = pattern.match(line)
        if m:
            key = m.group(1)
            print(key)
            with open(lic, "a") as f:
                f.write(key + "\n")

def main():
    global debug, name, home, repo, host, lic, bak, log_file, tmp

    # Set umask to 000 as in the bash script
    os.umask(0)

    # Set debug based on user
    if getpass.getuser() == "daniel.storey":
        debug = 1

    # Determine the script name behavior
    script_basename = os.path.basename(sys.argv[0])
    # We use sys.argv[1:] for arguments
    args = sys.argv[1:]

    # Initialize log and tmp file names
    log_file = f"{script_basename}.log"
    tmp = f"/tmp/.{script_basename}.{os.getpid()}"
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    bak = os.path.join(home, "backup", f"license_keys.{repo}.{os.getpid()}.{date_str}")

    # For the "pastef5keys" mode the behavior is different
    if script_basename.startswith("pastef5keys"):
        # Optionally, repo can be overridden by first argument
        if len(args) >= 1:
            repo = args[0]
        lic = os.path.join(home, f"license_keys.{repo}")
        if not os.path.exists(lic):
            error("No license file found.")
        # Call the appropriate pastef5keys function based on repo.
        if repo == "bigiq":
            pastef5keys_bigiq()
        elif repo == "apm":
            pastef5keys_apm()
        else:
            pastef5keys_lab()
    else:
        # Otherwise, we expect at least 3 arguments: startpod, endpod, host (optional type)
        if len(args) < 3:
            usage()
        try:
            start_pod = int(args[0])
            end_pod = int(args[1])
        except ValueError:
            error("Invalid Pod Numbers")
        host = args[2] if args[2] else "cliffjumper"
        # If a fourth argument is provided, it overrides repo.
        if len(args) >= 4:
            repo = args[3]
        # Validate that the host directory exists
        host_dir = os.path.join(home, host)
        if not os.path.isdir(host_dir):
            error(f"Invalid vmware host {host}!")
        lic = os.path.join(home, f"license_keys.{repo}")
        if not os.path.exists(lic):
            error("No license file found.")

        # For demonstration, assume that the script name indicates the function to run.
        # For example, if the script is called allocf5keys, call that function.
        # Otherwise, call the function matching the script basename.
        if script_basename.startswith("allocf5keys"):
            allocf5keys(start_pod, end_pod)
        elif script_basename.startswith("pushlic"):
            # For pushlic, if only one argument is given or if first arg equals "17", handle accordingly.
            if args[0] == "17":
                # In this branch, we expect a second argument for the remote identifier.
                if len(args) < 2:
                    error("pushlic 17 requires a remote identifier")
                pushlic(args[0], args[1])
            else:
                pushlic(str(start_pod), str(end_pod))
        elif script_basename.startswith("mkf5lic"):
            mkf5lic(start_pod, end_pod)
        elif script_basename.startswith("savef5lic"):
            savef5lic(start_pod, end_pod)
        else:
            usage()

if __name__ == "__main__":
    main()
