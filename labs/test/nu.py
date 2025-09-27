# labs/test/nu.py

import argparse
import logging
import threading
import pexpect
from typing import Dict, Any, Optional, List

# Setup logger for this module
logger = logging.getLogger('labbuild.test.nu')

def check_cluster_status(pod_number: int, ip_or_hostname: str, print_lock: threading.Lock) -> Dict[str, Any]:
    """
    SSH into the Nutanix CVM and run 'cluster status'.
    Dynamically handles multiple possible shell prompts.
    """
    result = {
        'pod': pod_number,
        'component': 'Nutanix Cluster',
        'ip': ip_or_hostname,
        'port': 22, # SSH Port
        'status': 'ERROR',
        'test_status': 'failed'
    }
    
    command = f"ssh {ip_or_hostname}"
    
    # --- THIS IS THE DYNAMIC FIX ---
    # Define a list of possible prompts to expect.
    # pexpect will match if ANY of these are found.
    # The prompts are regular expressions for flexibility.
    possible_prompts = [
        r"nutanix@.*:~\$",       # The old prompt, e.g., nutanix@NTNX-SERIAL-CVM:~$
        r"\[root@.* ~\]#",       # The new prompt, e.g., [root@pod-vr ~]#
        pexpect.TIMEOUT,         # Include TIMEOUT as a possible outcome of expect()
        pexpect.EOF              # Include EOF (End of File) as a possible outcome
    ]
    # --- END OF FIX ---
    
    child = None
    try:
        with print_lock:
            logger.debug(f"NU Pod {pod_number}: Spawning SSH to {ip_or_hostname} with 90s timeout.")
        child = pexpect.spawn(command, timeout=90)
        
        # Expect one of the prompts from the list
        prompt_index = child.expect(possible_prompts)
        
        # Handle the outcome of the initial connection
        if prompt_index == 0 or prompt_index == 1: # Matched nutanix or root prompt
            with print_lock:
                logger.debug(f"NU Pod {pod_number}: SSH prompt received. Sending 'cluster status'.")
            
            child.sendline("cluster status")
            
            # Wait for one of the prompts to return after the command
            command_prompt_index = child.expect(possible_prompts)
            
            if command_prompt_index == 0 or command_prompt_index == 1:
                # Get the output of the 'cluster status' command
                output = child.before.decode('utf-8')
                
                # Check for success condition in the command output
                if "The state of the cluster: start" in output:
                    result['status'] = 'UP'
                    result['test_status'] = 'success'
                    with print_lock:
                        logger.info(f"Nutanix cluster for pod {pod_number} is UP.")
                else:
                    result['status'] = 'CLUSTER DOWN'
                    result['error'] = "Cluster status command did not report 'start' state."
                    with print_lock:
                        logger.error(f"Nutanix cluster for pod {pod_number} is not in a running state.")
            elif command_prompt_index == 2: # TIMEOUT after sending command
                raise pexpect.exceptions.TIMEOUT("Timeout after sending 'cluster status' command.")
            elif command_prompt_index == 3: # EOF after sending command
                raise pexpect.exceptions.EOF("Connection closed after sending 'cluster status' command.")

        elif prompt_index == 2: # TIMEOUT on initial connection
            raise pexpect.exceptions.TIMEOUT("Timeout waiting for initial SSH prompt.")
        elif prompt_index == 3: # EOF on initial connection
            raise pexpect.exceptions.EOF("Connection failed during initial SSH login.")

    except pexpect.exceptions.TIMEOUT as e:
        result['status'] = 'TIMEOUT'
        result['error'] = str(e)
        with print_lock:
            logger.error(f"Timeout checking cluster status for pod {pod_number} at {ip_or_hostname}. Details: {e}")
    except pexpect.exceptions.EOF as e:
        result['status'] = 'CONNECTION FAILED'
        result['error'] = str(e)
        with print_lock:
            logger.error(f"SSH connection failed for pod {pod_number} at {ip_or_hostname}. Details: {e}")
    except Exception as e:
        result['error'] = str(e)
        with print_lock:
            logger.error(f"An unexpected error occurred during Nutanix test for pod {pod_number}: {e}")
    finally:
        if child and child.isalive():
            child.close()
            
    return result

def run_nu_pod_tests(pod_config: Dict[str, Any], component: Optional[str] = None, print_lock: Optional[threading.Lock] = None) -> List[Dict[str, Any]]:
    """
    Runs connectivity and status tests for a Nutanix pod.
    """
    if print_lock is None:
        print_lock = threading.Lock() # Create a dummy lock if not provided

    pod_number = pod_config.get("pod_number")
    host_fqdn = pod_config.get("host_fqdn", "")
    results = []

    if pod_number is None:
        logger.error("Pod number is missing in the pod configuration for NU test.")
        return [{"status": "failed", "test_status": "failed", "error": "Missing pod_number"}]

    logger.info(f"Starting Nutanix tests for Pod {pod_number} on host {host_fqdn}.")
    
    # Determine the hostname based on host location (US vs AU)
    host_short = host_fqdn.split('.')[0].lower()
    if host_short in ("hotshot", "trypticon"):
        domain = "us"
    else:
        domain = "au"
    
    target_hostname = f"nuvr{pod_number}.{domain}"
    
    cluster_test_result = check_cluster_status(pod_number, target_hostname, print_lock)
    results.append(cluster_test_result)
    
    logger.info(f"Finished Nutanix tests for Pod {pod_number}.")
    return results

def main(argv: List[str], print_lock: Optional[threading.Lock] = None) -> List[Dict[str, Any]]:
    """
    Main entry point for Nutanix testing, designed to be called from the test worker.
    """
    parser = argparse.ArgumentParser(description="Run tests for Nutanix pods.")
    parser.add_argument("-s", "--start-pod", type=int, required=True)
    parser.add_argument("-e", "--end-pod", type=int, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("-g", "--group", required=True)
    parser.add_argument("-c", "--component", help="Test a specific component (not used for NU cluster test)")
    args = parser.parse_args(argv)

    all_results = []
    for pod_num in range(args.start_pod, args.end_pod + 1):
        pod_config = {
            "vendor": "nu",
            "pod_number": pod_num,
            "host_fqdn": args.host,
            "course_name": args.group,
        }
        pod_results = run_nu_pod_tests(pod_config, component=args.component, print_lock=print_lock)
        all_results.extend(pod_results)
    
    return all_results