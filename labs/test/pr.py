# labs/test/pr.py

import argparse
import logging
import threading
import nmap
from typing import Dict, Any, Optional, List

# Setup logger for this module
logger = logging.getLogger('labbuild.test.pr')

def run_nmap_test(pod_number: int, component_name: str, ip: str, port: int) -> Dict[str, Any]:
    """Runs a single nmap port scan and returns a structured result."""
    result = {
        'pod': pod_number,
        'component': component_name,
        'ip': ip,
        'port': port,
        'status': 'ERROR',  # Default to ERROR
    }
    try:
        nm = nmap.PortScanner()
        # Use -Pn to skip host discovery (ping), as it's often blocked.
        nm.scan(hosts=ip, ports=str(port), arguments='-Pn')
        
        # Check if the host was scanned and the port was found
        if ip not in nm.all_hosts() or 'tcp' not in nm[ip] or port not in nm[ip]['tcp']:
             result['status'] = 'DOWN / FILTERED'
        else:
            result['status'] = nm[ip]['tcp'][port]['state'].upper() # e.g., OPEN, CLOSED, FILTERED
        
    except KeyError:
        result['status'] = 'DOWN / FILTERED'
    except Exception as e:
        logger.error(f"Nmap test failed for {ip}:{port} - {e}")
        result['error'] = str(e)

    # --- THIS IS THE FIX ---
    # Add a final 'test_status' field for easy counting.
    # Only 'OPEN' is considered a success.
    if result['status'] == 'OPEN':
        result['test_status'] = 'success'
    else:
        result['test_status'] = 'failed'
    # --- END OF FIX ---
    
    return result


def run_pr_pod_tests(pod_config: Dict[str, Any], component: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Runs connectivity tests for a PR (PRTG) pod.
    This test verifies that RDP (port 3389) is open on the two primary VMs for the pod.
    """
    pod_number = pod_config.get("pod_number")
    host_fqdn = pod_config.get("host_fqdn", "")
    results = []

    if pod_number is None:
        logger.error("Pod number is missing in the pod configuration for PR test.")
        return [{"status": "failed", "test_status": "failed", "error": "Missing pod_number"}]

    logger.info(f"Starting PR tests for Pod {pod_number} on host {host_fqdn}.")

    host_short = host_fqdn.split('.')[0].lower()
    if host_short in ("hotshot", "trypticon"):
        base_ip = "172.26.9" # US subnet
    else:
        base_ip = "172.30.9" # AU subnet

    test_cases = [
        {"component": "PRTG_Win_10", "ip": f"{base_ip}.{100 + pod_number}", "port": 3389},
        {"component": "PRTG_Win_2012", "ip": f"{base_ip}.{200 + pod_number}", "port": 3389}
    ]

    components_to_test = test_cases
    if component:
        components_to_test = [c for c in test_cases if c['component'].lower() == component.lower()]
        if not components_to_test:
            logger.warning(f"Component '{component}' not found for PR pod test. Available: {[c['component'] for c in test_cases]}")
            return [{"status": "skipped", "test_status": "skipped", "message": f"Component {component} not applicable for PR test"}]

    for case in components_to_test:
        test_result = run_nmap_test(
            pod_number=pod_number,
            component_name=case['component'],
            ip=case['ip'],
            port=case['port']
        )
        results.append(test_result)

    logger.info(f"Finished PR tests for Pod {pod_number}.")
    return results

def main(argv: List[str], print_lock: Optional[threading.Lock] = None) -> List[Dict[str, Any]]:
    """
    Main entry point for PR testing, designed to be called from the test worker.
    """
    parser = argparse.ArgumentParser(description="Run tests for PR pods.")
    parser.add_argument("-s", "--start-pod", type=int, required=True)
    parser.add_argument("-e", "--end-pod", type=int, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("-g", "--group", required=True)
    parser.add_argument("-c", "--component", help="Test a specific component")
    args = parser.parse_args(argv)

    all_results = []
    for pod_num in range(args.start_pod, args.end_pod + 1):
        # Construct the pod_config dict needed by the test function
        pod_config = {
            "vendor": "pr",
            "pod_number": pod_num,
            "host_fqdn": args.host,
            "course_name": args.group,
        }
        pod_results = run_pr_pod_tests(pod_config, component=args.component)
        all_results.extend(pod_results)
    
    return all_results