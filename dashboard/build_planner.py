# In dashboard/build_planner.py

import logging
import os
import requests
import re
import hashlib
import random
import string
from typing import List, Dict, Any, Optional, Union, Set
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
from dataclasses import asdict

# Imports for data fetching and helpers
from .extensions import (
    build_rules_collection, course_config_collection, host_collection,
    trainer_email_collection, alloc_collection, locations_collection
)
from .salesforce_utils import get_upcoming_courses_data
# We are now self-containing the helpers, so we only need these two
from .routes.allocation_actions import _find_all_matching_rules, _get_memory_for_course_local

logger = logging.getLogger('dashboard.build_planner')


# --- Data Structure for a single LabBuild Command ---
@dataclass
class LabBuildCommand:
    """A structured container for one executable 'labbuild setup' command."""
    labbuild_course: str
    start_pod: int
    end_pod: int
    vendor_shortcode: str
    tag: str
    host: str
    trainer_name: str
    username: str
    password: str
    start_date: str
    end_date: str
    f5_class_number: Optional[int] = None
    sf_course_code: str = ""
    sf_course_type: str = ""
    scheduled_run_time: Optional[datetime] = None

    def to_args_list(self) -> List[str]:
        """Converts the object's attributes into a list of CLI arguments."""
        args = [
            'setup', '-v', self.vendor_shortcode, '-g', self.labbuild_course,
            '--host', self.host, '-s', str(self.start_pod), '-e', str(self.end_pod),
            '-t', self.tag, '--start-date', self.start_date, '--end-date', self.end_date,
            '--trainer-name', self.trainer_name, '--username', self.username,
            '--password', self.password
        ]
        if self.f5_class_number is not None:
            args.extend(['-cn', str(self.f5_class_number)])
        return args
    
    def to_dict(self) -> Dict[str, Any]:
        """Converts the dataclass instance to a dictionary."""
        return asdict(self)
    

# --- Data Structure for a single APM Command ---
@dataclass
class APMCommand:
    """A structured container for one executable 'course2' APM command."""
    command: str
    username: str
    arguments: List[str]
    server: str

    def to_cli_string(self) -> str:
        """Converts the object into a fully formatted command-line string."""
        prefix = "course2 -u" if self.server == 'us' else "course2"
        quoted_args = [f'"{arg}"' if ' ' in str(arg) else str(arg) for arg in self.arguments]
        return f"{prefix} {self.command} {self.username} {' '.join(quoted_args)}"

# --- Data Structure for a single Email ---
@dataclass
class EmailContent:
    """A structured container for the content of one email to one trainer."""
    trainer_name: str
    recipient_email: str
    subject: str
    html_body: str
    courses: List[LabBuildCommand]

# --- The Main Context Object for the entire Planning Session ---
@dataclass
class BuildContext:
    """Holds all data and tracks state for a single build planning session."""
    all_hosts: List[str]
    course_configs: List[Dict[str, Any]]
    build_rules: List[Dict[str, Any]]
    trainer_emails: Dict[str, str]
    host_to_vcenter_map: Dict[str, str] = field(default_factory=dict)
    locations_map: Dict[str, str] = field(default_factory=dict)
    apm_allocator: 'APMUsernameAllocator' = field(default_factory=lambda: APMUsernameAllocator())
    host_capacities: Dict[str, float] = field(default_factory=dict)
    pods_in_use: Dict[str, set] = field(default_factory=lambda: defaultdict(set))
    f5_class_cursor: int = 1
    
    final_build_commands: List[LabBuildCommand] = field(default_factory=list)
    final_apm_commands: List[APMCommand] = field(default_factory=list)
    final_emails: List[EmailContent] = field(default_factory=list)

    lab_report_data: Dict[str, List] = field(default_factory=dict)
    trainer_report_data: List[Dict] = field(default_factory=list)

    errors: List[str] = field(default_factory=list)

class APMUsernameAllocator:
    """
    Manages the allocation of APM usernames (e.g., 'labcp-1', 'labpa-5').
    It ensures that usernames from extended/locked labs are never reused and
    finds the lowest available integer for new allocations.
    """
    def __init__(self):
        # A dictionary to hold sets of integers that are already in use for each vendor.
        # e.g., {'cp': {1, 5, 10}, 'pa': {2, 3}}
        self.used_user_integers: Dict[str, set] = defaultdict(set)
        
    def populate_initial_state(self, current_apm_state: Dict, extended_tags: Set[str]):
        """
        Scans the current state of APM and extended labs to lock usernames.
        """
        logger.info("APM Allocator: Populating initial state from current APM users and extended labs.")
        
        # Lock any username associated with an extended tag
        for username, details in current_apm_state.items():
            course_code = details.get("vpn_auth_course_code")
            if course_code in extended_tags:
                self._lock_username(username)

    def get_next_username(self, vendor: str) -> str:
        """
        Finds the next available username for a given vendor, allocates it,
        and returns the full username string.
        """
        # Start searching from 1 for the next available integer slot
        user_num = 1
        while user_num in self.used_user_integers[vendor]:
            user_num += 1
        
        # Lock this number so it can't be used again in this session
        self.used_user_integers[vendor].add(user_num)
        
        new_username = f"lab{vendor}-{user_num}"
        logger.info(f"APM Allocator: Allocated new username '{new_username}'.")
        return new_username

    def _lock_username(self, username: str):
        """
        Parses a username (e.g., 'labcp-15') and locks its integer part.
        """
        # Regex to match the pattern 'lab{vendor}-{integer}'
        match = re.match(r"lab([a-z0-9]+)-(\d+)", username.lower())
        if match:
            vendor = match.group(1)
            num = int(match.group(2))
            self.used_user_integers[vendor].add(num)
            logger.debug(f"APM Allocator: Locking username '{username}' (vendor: {vendor}, number: {num}).")

# --- The Blueprint for Vendor-Specific Planners ---
class BaseVendorPlanner:
    """
    Abstract base class defining the contract for all vendor planners.
    It also provides powerful, reusable helper methods for its children.
    """
    
    def __init__(self, vendor_code: str, build_context: BuildContext):
        self.vendor_code = vendor_code
        self.context = build_context
        self.course_config_map = {c['course_name']: c for c in self.context.course_configs}

    def plan_builds(self, sf_courses: List[Dict[str, Any]]):
        """Main method to be implemented by vendor-specific subclasses."""
        raise NotImplementedError

    def _get_ip_conflict_pods(self, pod_number: int) -> set:
        """
        To be implemented by subclasses with specific IP conflict rules.
        Given a pod number, returns a set of other pod numbers that would conflict.
        """
        return set() # Default is no conflicts

    def _find_contiguous_pod_range(self, num_pods: int, start_suggestion: int = 1) -> Optional[int]:
        """
        Finds the next available contiguous block of pod numbers, respecting
        both `pods_in_use` and vendor-specific IP conflict rules.
        
        Returns the starting pod number of the found block, or None if no block is found.
        """
        candidate_pod = start_suggestion
        while True:
            pod_range_to_check = range(candidate_pod, candidate_pod + num_pods)
            
            # 1. Check for direct conflicts (is any pod in the range already taken?)
            is_taken = any(p in self.context.pods_in_use[self.vendor_code] for p in pod_range_to_check)
            
            # 2. Check for indirect IP conflicts
            ip_conflict = False
            if not is_taken:
                for pod in pod_range_to_check:
                    conflicting_pods = self._get_ip_conflict_pods(pod)
                    if any(p in self.context.pods_in_use[self.vendor_code] for p in conflicting_pods):
                        ip_conflict = True
                        break # An IP conflict was found, this range is invalid
            
            if not is_taken and not ip_conflict:
                return candidate_pod # Success! Found a valid starting pod.
            
            # If the slot was taken or had a conflict, move to the next number and retry.
            candidate_pod += 1
            if candidate_pod > 200: # Safety break to prevent infinite loops
                logger.warning(f"Pod search for {self.vendor_code} exceeded 200. No slot found.")
                return None

    def _get_memory_for_course(self, course_name: str) -> float:
        """Helper to find the memory requirement for a given LabBuild course."""
        return _get_memory_for_course_local(course_name, self.course_config_map)

    def _create_contiguous_ranges(self, pod_numbers: List[Union[int,str]]) -> str:
        """Converts a list of pod numbers into a comma-separated string of ranges."""
        if not pod_numbers: return ""
        int_pod_numbers = sorted(list(set(int(p) for p in pod_numbers)))
        if not int_pod_numbers: return ""
        ranges, start_range = [], int_pod_numbers[0]
        end_range = start_range
        for i in range(1, len(int_pod_numbers)):
            if int_pod_numbers[i] == end_range + 1:
                end_range = int_pod_numbers[i]
            else:
                ranges.append(f"{start_range}-{end_range}" if start_range != end_range else str(start_range))
                start_range = end_range = int_pod_numbers[i]
        ranges.append(f"{start_range}-{end_range}" if start_range != end_range else str(start_range))
        return ",".join(ranges)

# --- The Generic Planner for Standard Vendors ---
class DefaultVendorPlanner(BaseVendorPlanner):
    """
    A generic planner for simple vendors that handles finding an available
    host and a contiguous block of pod numbers for a course.
    """

    def plan_builds(self, sf_courses: List[Dict[str, Any]]):
        """
        Main planning loop for this vendor. It processes a list of Salesforce courses,
        determines the build parameters for each, finds an allocation, and creates
        a build command.
        """
        logger.info(f"Running DefaultVendorPlanner for {len(sf_courses)} '{self.vendor_code}' courses.")
        for course in sf_courses:
            sf_code = course.get('Course Code', 'UNKNOWN')
            try:
                params = self._determine_build_parameters(course)
                if not params:
                    continue

                if params['pods_req'] <= 0:
                    logger.info(f"Skipping '{sf_code}' as required pods is zero.")
                    continue

                allocation = self._find_allocation_slot(params)
                if not allocation:
                    self.context.errors.append(f"Could not find available host/pod slot for SF Course '{sf_code}' ({params['labbuild_course']}).")
                    continue
                
                self._commit_allocation_to_context(allocation, params)
                command = self._create_build_command(course, params, allocation)
                self.context.final_build_commands.append(command)

            except Exception as e:
                logger.error(f"Error planning for SF course '{sf_code}': {e}", exc_info=True)
                self.context.errors.append(f"Unexpected error planning for '{sf_code}'.")
    
    def _determine_build_parameters(self, sf_course: Dict) -> Optional[Dict]:
        """
        Applies build rules to determine what needs to be built. Prioritizes
        pre-calculated data from the UI selection if available.
        """
        sf_code = sf_course.get('Course Code', 'UNKNOWN')
        
        # Use the user's explicit course choice from the dropdown
        labbuild_course = sf_course.get('preselect_labbuild_course')
        if not labbuild_course:
            # Fallback to rules if no user selection was made
            sf_type = sf_course.get('Course Type', '')
            rules = _find_all_matching_rules(self.context.build_rules, self.vendor_code, sf_code, sf_type)
            actions = {}
            for rule in rules:
                actions.update(rule.get("actions", {}))
            labbuild_course = actions.get("set_labbuild_course")

        if not labbuild_course:
            self.context.errors.append(f"No LabBuild course could be determined for SF course '{sf_code}'.")
            return None

        # Prioritize pre-calculated host priority list from the UI
        host_priority = sf_course.get('preselect_host_priority_list')
        if not host_priority:
            sf_type = sf_course.get('Course Type', '')
            rules = _find_all_matching_rules(self.context.build_rules, self.vendor_code, sf_code, sf_type)
            actions = {}
            for rule in rules: actions.update(rule.get("actions", {}))
            host_priority = actions.get("host_priority", self.context.all_hosts)
            
        # Parse the 'Pods Req.' string robustly to get the number
        pods_req_raw = sf_course.get('Pods Req.', '1')
        pods_req_match = re.match(r"^\s*(\d+)", str(pods_req_raw))
        pods_req = int(pods_req_match.group(1)) if pods_req_match else 1
        
        if sf_course.get('preselect_max_pods_applied_constraint') is not None:
             pods_req = min(pods_req, int(sf_course['preselect_max_pods_applied_constraint']))

        return {
            "labbuild_course": labbuild_course,
            "pods_req": pods_req,
            "host_priority": host_priority,
            "start_pod_suggestion": int(sf_course.get('preselect_start_pod', 1)),
            "memory_per_pod": self._get_memory_for_course(labbuild_course)
        }

    def _find_allocation_slot(self, params: Dict) -> Optional[Dict]:
        """Finds the first available host and contiguous pod range."""
        for host in params['host_priority']:
            required_memory = params['memory_per_pod'] * params['pods_req']
            if self.context.host_capacities.get(host, 0) < required_memory:
                continue

            start_pod = self._find_contiguous_pod_range(params['pods_req'], params['start_pod_suggestion'])
            if start_pod is not None:
                return {"host": host, "start_pod": start_pod}
        return None

    def _commit_allocation_to_context(self, allocation: Dict, params: Dict):
        """Updates the shared BuildContext with the newly allocated resources."""
        host = allocation['host']
        start_pod = allocation['start_pod']
        num_pods = params['pods_req']
        end_pod = start_pod + num_pods - 1
        
        for i in range(start_pod, end_pod + 1):
            self.context.pods_in_use[self.vendor_code].add(i)
            
        memory_to_reserve = params['memory_per_pod'] * num_pods
        self.context.host_capacities[host] -= memory_to_reserve
        
        logger.info(f"Allocated {self.vendor_code.upper()} pods {start_pod}-{end_pod} on host '{host}'.")

    def _create_build_command(self, sf_course: Dict, params: Dict, allocation: Dict) -> LabBuildCommand:
        """Constructs the final, structured LabBuildCommand data object."""
        sf_code = sf_course.get('Course Code')
        start_pod = allocation['start_pod']
        end_pod = start_pod + params['pods_req'] - 1
        username = self.context.apm_allocator.get_next_username(self.vendor_code)
        password = self._generate_password(sf_code)
        
        return LabBuildCommand(
            labbuild_course=params['labbuild_course'],
            start_pod=start_pod,
            end_pod=end_pod,
            vendor_shortcode=self.vendor_code,
            tag=sf_code,
            host=allocation['host'],
            trainer_name=sf_course.get('Trainer', 'N/A'),
            username=username,
            password=password,
            start_date=sf_course.get('Start Date', ''),
            end_date=sf_course.get('End Date', ''),
            sf_course_code=sf_code,
            sf_course_type=sf_course.get('Course Type', 'N/A')
        )
    
    def _generate_password(self, seed_string: Optional[str], length: int = 8) -> str:
        """
        Generates a deterministic numeric password of a given length that does not start with zero.
        Falls back to a random password if the seed is missing.
        """
        if not seed_string:
            # Fallback to a random password that does not start with zero.
            first_digit = random.choice('123456789')
            rest_of_digits = "".join(random.choice(string.digits) for _ in range(length - 1))
            return first_digit + rest_of_digits

        # --- MODIFICATION: Ensure the generated number is within the correct n-digit range ---
        # Define the range for a number with a specific length that doesn't start with 0.
        # For length=8, this is 10,000,000 to 99,999,999.
        min_val = 10 ** (length - 1)
        max_val = (10 ** length) - 1
        range_size = max_val - min_val + 1

        hasher = hashlib.sha256(str(seed_string).encode('utf-8'))
        # Use the full hash digest for a larger number space to ensure good distribution
        hash_int = int.from_bytes(hasher.digest(), 'big')
        
        # Map the large hash integer to our desired range
        password_num = min_val + (hash_int % range_size)
        
        # The number is now guaranteed to have the correct length without leading zeros.
        return str(password_num)
    
    def _create_build_command_with_creds(self, sf_course: Dict, params: Dict, allocation: Dict, username: str, password: str) -> LabBuildCommand:
        """
        A special version of create_build_command that uses a provided username
        and password instead of generating new ones.
        """
        sf_code = sf_course.get('Course Code')
        start_pod = allocation['start_pod']
        end_pod = start_pod + params['pods_req'] - 1
        
        return LabBuildCommand(
            labbuild_course=params['labbuild_course'],
            start_pod=start_pod,
            end_pod=end_pod,
            vendor_shortcode=self.vendor_code,
            tag=sf_code,
            host=allocation['host'],
            trainer_name=sf_course.get('Trainer', 'N/A'),
            username=username, # Use provided username
            password=password,   # Use provided password
            start_date=sf_course.get('Start Date', ''),
            end_date=sf_course.get('End Date', ''),
            sf_course_code=sf_code,
            sf_course_type=sf_course.get('Course Type', 'N/A')
        )



# --- Specialized Planner for Checkpoint (Handles Maestro) ---
class CheckpointPlanner(DefaultVendorPlanner):
    """Specialized planner for Checkpoint that handles Maestro split-builds."""

    def _get_ip_conflict_pods(self, pod_number: int) -> set:
        """
        Implements the Checkpoint IP conflict rule: a regular pod at X conflicts
        with a Maestro pod at X-100, and vice-versa.
        """
        if pod_number > 100:
            return {pod_number - 100}
        else:
            return {pod_number + 100}

    def plan_builds(self, sf_courses: List[Dict[str, Any]]):
        """
        Overrides the main planner to check for Maestro courses and delegate
        to the appropriate internal method.
        """
        logger.info(f"Running CheckpointPlanner for {len(sf_courses)} courses.")
        for course in sf_courses:
            sf_code = course.get('Course Code', 'UNKNOWN')
            try:
                params = self._determine_build_parameters(course)
                if not params: continue

                is_maestro_build = "maestro" in params['labbuild_course'].lower()
                
                if is_maestro_build:
                    self._plan_maestro_build(course, params)
                else:
                    self._plan_standard_build(course, params)
            except Exception as e:
                logger.error(f"Error planning CP course '{sf_code}': {e}", exc_info=True)
                self.context.errors.append(f"Error planning for '{sf_code}'.")

    def _plan_standard_build(self, sf_course: Dict, params: Dict):
        """Handles a regular, non-Maestro Checkpoint build."""
        allocation = self._find_allocation_slot(params)
        if not allocation:
            self.context.errors.append(f"Could not find slot for standard CP course '{params['labbuild_course']}'.")
            return
        self._commit_allocation_to_context(allocation, params)
        command = self._create_build_command(sf_course, params, allocation)
        self.context.final_build_commands.append(command)

    def _plan_maestro_build(self, sf_course: Dict, params: Dict):
        """
        Handles the Maestro split-build, creating separate build commands but
        consolidating APM credentials based on the region (AU vs. US).
        """
        sf_code = sf_course.get('Course Code')
        logger.info(f"Planning a Maestro Split Build for SF Course '{sf_code}'.")

        # --- MODIFICATION: Centralize credential generation for the entire course ---
        # Generate one username for all AU parts and one for all US parts.
        au_username = self.context.apm_allocator.get_next_username('cp')
        us_username = self.context.apm_allocator.get_next_username('cp')
        # All parts of a single Maestro course will share the same password.
        password = self._generate_password(sf_code)
        
        US_HOSTS = {"hotshot", "trypticon"} # Define which hosts are in the US
        # --- END MODIFICATION ---

        maestro_config = {}
        for rule in reversed(self.context.build_rules):
            if self.vendor_code == rule.get("conditions", {}).get("vendor", ""):
                 if "maestro_split_build" in rule.get("actions", {}):
                    maestro_config = rule["actions"]["maestro_split_build"]
                    break

        if not maestro_config:
            self.context.errors.append(f"Could not find a valid 'maestro_split_build' rule for '{sf_code}'.")
            return

        maestro_parts = [
            {"name": "Main Pods", "pods_req": 2, "labbuild_course": maestro_config.get("main_course"), "host_priority": params.get("host_priority", self.context.all_hosts)},
            {"name": "Rack 1", "pods_req": 1, "labbuild_course": maestro_config.get("rack1_course"), "host_priority": maestro_config.get("rack_host", [])},
            {"name": "Rack 2", "pods_req": 1, "labbuild_course": maestro_config.get("rack2_course"), "host_priority": maestro_config.get("rack_host", [])},
        ]

        for part in maestro_parts:
            part_params = params.copy()
            part_params['labbuild_course'] = part["labbuild_course"]
            part_params['pods_req'] = part['pods_req']
            part_params['memory_per_pod'] = self._get_memory_for_course(part_params['labbuild_course'])
            part_params['host_priority'] = part["host_priority"]

            allocation = self._find_allocation_slot(part_params)
            if not allocation:
                self.context.errors.append(f"Could not find slot for Maestro part '{part['name']}' for SF course '{sf_code}'.")
                continue

            self._commit_allocation_to_context(allocation, part_params)

            # --- MODIFICATION: Determine which region the allocation is in and select the correct username ---
            allocated_host = allocation['host']
            is_us_host = allocated_host.lower() in US_HOSTS
            final_username = us_username if is_us_host else au_username
            # --- END MODIFICATION ---

            # Use the new helper to create the command with the correct, shared credentials
            command = self._create_build_command_with_creds(
                sf_course, 
                part_params, 
                allocation, 
                username=final_username, 
                password=password
            )
            self.context.final_build_commands.append(command)


# --- Specialized Planner for F5 ---
class F5Planner(DefaultVendorPlanner):
    """Specialized planner for F5 that handles allocation of unique F5 Class Numbers."""

    def plan_builds(self, sf_courses: List[Dict[str, Any]]):
        logger.info(f"Running F5Planner for {len(sf_courses)} courses.")
        for course in sf_courses:
            sf_code = course.get('Course Code', 'UNKNOWN')
            try:
                params = self._determine_build_parameters(course)
                if not params or params['pods_req'] <= 0:
                    continue
                allocation = self._find_allocation_slot(params)
                if not allocation:
                    self.context.errors.append(f"Could not find host/pod slot for F5 course '{sf_code}'.")
                    continue
                class_number = self._get_next_f5_class_number()
                self._commit_allocation_to_context(allocation, params)
                self.context.pods_in_use[self.vendor_code].add(class_number)
                command = self._create_build_command(course, params, allocation)
                command.f5_class_number = class_number
                self.context.final_build_commands.append(command)
            except Exception as e:
                logger.error(f"Error planning for F5 course '{sf_code}': {e}", exc_info=True)
                self.context.errors.append(f"Unexpected error planning for F5 course '{sf_code}'.")

    def _get_next_f5_class_number(self) -> int:
        """Finds the next available F5 class number, skipping any that are already in use."""
        candidate_class_num = self.context.f5_class_cursor
        while candidate_class_num in self.context.pods_in_use['f5']:
            candidate_class_num += 1
        self.context.f5_class_cursor = candidate_class_num + 1
        return candidate_class_num

# --- Specialized Planner for All Trainer Pods ---
# In dashboard/build_planner.py

# --- Specialized Planner for All Trainer Pods ---
class TrainerPodPlanner:
    """
    A specialized planner that runs AFTER all student pods have been planned.
    Its sole responsibility is to generate the necessary trainer pod builds
    based on the finalized student plan and user selections from the UI.

    The logic is split to handle F5 vendors uniquely (1-to-1 trainer pods)
    and consolidate all other vendors by their common LabBuild course.
    """
    def __init__(self, build_context: BuildContext):
        """
        Initializes the planner with the shared build context and defines
        the vendor-specific rules for trainer pods.
        """
        self.context = build_context
        # Create a quick-lookup map for course configurations by name
        self.course_config_map = {c['course_name']: c for c in self.context.course_configs}
        
        # --- VENDOR-SPECIFIC RULES FOR TRAINER PODS ---
        # Defines the starting integer for pod number searches for each vendor.
        self.vendor_rules = {
            'cp': {"start_pod": 100},
            'f5': {"start_pod": 50},
        }
        # A fallback starting pod number for any vendor not in the rules map.
        self.default_start_pod = 100

    def plan_trainer_pods(self, all_sf_courses: List[Dict[str, Any]]):
        """
        Main entry point for this planner. It orchestrates the entire trainer
        pod planning process.
        """
        # If no student builds were successfully planned, there's nothing to do.
        if not self.context.final_build_commands:
            logger.info("TrainerPodPlanner: No student builds were planned, so no trainer pods needed.")
            return

        # Step 1: Filter for courses that the user explicitly flagged for
        # trainer pod creation on the "Upcoming Courses" page.
        courses_flagged_for_tp = {
            course.get('Course Code') for course in all_sf_courses 
            if course.get('create_trainer_pod') is True
        }
        
        if not courses_flagged_for_tp:
            logger.info("TrainerPodPlanner: No courses were flagged for trainer pod creation by the user.")
            return

        # Step 2: From the list of successful student builds, get only the ones
        # that correspond to the courses the user flagged.
        student_commands_to_process = [
            cmd for cmd in self.context.final_build_commands
            if cmd.sf_course_code in courses_flagged_for_tp
        ]
        if not student_commands_to_process:
            logger.info("TrainerPodPlanner: No student builds to base trainer pods on after filtering.")
            return

        # Step 3: Separate the F5 commands from all other vendors, as they
        # have unique planning requirements.
        f5_student_commands = [cmd for cmd in student_commands_to_process if cmd.vendor_shortcode == 'f5']
        other_student_commands = [cmd for cmd in student_commands_to_process if cmd.vendor_shortcode != 'f5']

        # This dictionary will store the generated username for each LabBuild course type,
        # allowing us to reuse it for multiple trainer pods of the same type.
        # Format: {'f5-config16.1.4': 'labf5-3', 'CCSA-R81.20': 'labcp-100'}
        self.generated_trainer_usernames = {}

        # Step 4: Run the specific planning logic for each category.
        if f5_student_commands:
            self._plan_f5_trainer_pods(f5_student_commands)
        if other_student_commands:
            self._plan_other_trainer_pods(other_student_commands)

    def _plan_f5_trainer_pods(self, f5_commands: List[LabBuildCommand]):
        """
        Creates a dedicated trainer pod for EACH F5 student build command.
        This ensures every F5 course gets its own trainer pod in its respective class.
        """
        logger.info(f"TrainerPodPlanner: Planning {len(f5_commands)} separate F5 trainer pods.")
        for student_cmd in f5_commands:
            try:
                # F5 trainer pods are always single-pod allocations.
                num_pods_needed = 1
                start_suggestion = self.vendor_rules['f5']['start_pod']
                
                # We need an F5Planner instance to use its pod allocation helper,
                # which correctly respects F5-specific rules (like class numbers).
                f5_planner = F5Planner('f5', self.context)
                start_pod = f5_planner._find_contiguous_pod_range(num_pods_needed, start_suggestion)

                if start_pod is None:
                    self.context.errors.append(f"Could not find an available pod for F5 trainer for course '{student_cmd.sf_course_code}'.")
                    continue

                # An F5 trainer pod uses the same host and class number as its student counterpart.
                host = student_cmd.host
                class_number = student_cmd.f5_class_number
                
                # Create and commit the final build command.
                self._create_and_commit_trainer_command(
                    start_pod=start_pod,
                    num_pods=num_pods_needed,
                    host=host,
                    student_commands=[student_cmd], # Pass as a list of one
                    f5_class_number=class_number   # Pass the critical class number
                )
            except Exception as e:
                logger.error(f"Error planning F5 trainer pod for course '{student_cmd.sf_course_code}': {e}", exc_info=True)
                self.context.errors.append(f"Unexpected error planning F5 trainer pod for '{student_cmd.sf_course_code}'.")

    def _plan_other_trainer_pods(self, other_commands: List[LabBuildCommand]):
        """
        Groups non-F5 student builds by their LabBuild course to create a single,
        consolidated trainer pod build for each group.
        """
        # Group commands by their final labbuild_course name.
        commands_by_labbuild_course = defaultdict(list)
        for cmd in other_commands:
            commands_by_labbuild_course[cmd.labbuild_course].append(cmd)

        logger.info(f"TrainerPodPlanner: Found {len(commands_by_labbuild_course)} unique non-F5 course groups for trainer pods.")
        for labbuild_course, commands_in_group in commands_by_labbuild_course.items():
            try:
                vendor = commands_in_group[0].vendor_shortcode
                
                # Calculate pods needed (e.g., 1 per 10 students, with a minimum of 1).
                total_student_pods = sum(cmd.end_pod - cmd.start_pod + 1 for cmd in commands_in_group)
                num_pods_needed = max(1, (total_student_pods + 9) // 10)
                
                start_suggestion = self.vendor_rules.get(vendor, {}).get("start_pod", self.default_start_pod)
                
                # Get the correct vendor planner to access its helpers (for IP conflict rules).
                PlannerClass = BuildPlanner.VENDOR_MAPPING.get(vendor, DefaultVendorPlanner)
                vendor_planner = PlannerClass(vendor, self.context)
                
                start_pod = vendor_planner._find_contiguous_pod_range(num_pods_needed, start_suggestion)
                
                if start_pod is None:
                    self.context.errors.append(f"Could not find pod block for trainer group '{labbuild_course}'.")
                    continue

                host = commands_in_group[0].host # Use the host of the first student course in the group.
                self._create_and_commit_trainer_command(start_pod, num_pods_needed, host, commands_in_group)
            except Exception as e:
                logger.error(f"Error planning trainer pod for group '{labbuild_course}': {e}", exc_info=True)
                self.context.errors.append(f"Unexpected error planning trainer pod for '{labbuild_course}'.")

    def _create_and_commit_trainer_command(self, start_pod, num_pods, host, student_commands, f5_class_number=None):
        """Creates the LabBuildCommand for a trainer pod group and commits resources."""
        first_cmd = student_commands[0]
        vendor = first_cmd.vendor_shortcode
        labbuild_course = first_cmd.labbuild_course

        end_pod = start_pod + num_pods - 1
        for i in range(start_pod, end_pod + 1):
            self.context.pods_in_use[vendor].add(i)
        
        # Generate the tag and description from the associated student courses.
        sf_codes = sorted(list(set(cmd.sf_course_code for cmd in student_commands)))
        sf_types = sorted(list(set(cmd.sf_course_type for cmd in student_commands)))
        tag = f"{sf_codes[0]}-TP" if len(sf_codes) == 1 else f"{labbuild_course}-Trainer-Pods"
        description = f"Trainer Pods - {', '.join(sf_types)}"
        
        # Get the next available APM username.
        # Check if we have already generated a username for this labbuild_course.
        if labbuild_course in self.generated_trainer_usernames:
            # If yes, reuse it.
            username = self.generated_trainer_usernames[labbuild_course]
            logger.info(f"Reusing trainer APM username '{username}' for course '{labbuild_course}'.")
        else:
            # If no, get a new one from the allocator and save it in our cache.
            username = self.context.apm_allocator.get_next_username(vendor)
            self.generated_trainer_usernames[labbuild_course] = username
        password = self._generate_password(tag)

        # Create the final command object with all details.
        command = LabBuildCommand(
            labbuild_course=labbuild_course,
            start_pod=start_pod,
            end_pod=end_pod,
            vendor_shortcode=vendor,
            tag=tag,
            host=host,
            trainer_name="Trainer",
            username=username,
            password=password,
            start_date=first_cmd.start_date,
            end_date=first_cmd.end_date,
            sf_course_code=f"Consolidated for: {', '.join(sf_codes)}",
            sf_course_type=description,
            f5_class_number=f5_class_number # This will be set for F5, None for others.
        )

        self.context.final_build_commands.append(command)
        logger.info(f"Allocated TRAINER pods {start_pod}-{end_pod} for course '{labbuild_course}' on host '{host}'.")

    def _generate_password(self, seed_string: Optional[str], length: int = 8) -> str:
        """Generates a deterministic numeric password that does not start with zero."""
        if not seed_string:
            first_digit = random.choice('123456789')
            return first_digit + "".join(random.choice(string.digits) for _ in range(length - 1))
        
        min_val = 10 ** (length - 1)
        range_size = (10 ** length) - min_val
        hasher = hashlib.sha256(str(seed_string).encode('utf-8'))
        hash_int = int.from_bytes(hasher.digest(), 'big')
        return str(min_val + (hash_int % range_size))


# --- Post-Processing Manager for APM Commands ---
class APMManager:
    """
    Generates all necessary APM commands based on a completed build plan.
    It compares the "desired state" from the build plan against the "current state"
    of the APM servers and generates a list of 'del' and 'add' commands.
    """
    
    def __init__(self, build_context: BuildContext, current_apm_state: Dict, extended_tags: Set[str]):
        self.context = build_context
        self.build_commands = build_context.final_build_commands
        self.current_apm_state = current_apm_state
        self.extended_tags = extended_tags
        self.US_APM_HOSTS = {"hotshot", "trypticon"}
    
    def _determine_apm_version(self, vendor: str, lb_course_name: str, sf_course_code: str) -> str:
        """
        Applies specific business logic to determine the correct 'version' string
        for the APM (course2) command.
        """
        vendor_lower = (vendor or "").lower()
        lb_course_lower = (lb_course_name or "").lower()
        sf_code_lower = (sf_course_code or "").lower()

        if vendor_lower == 'av':
            return "aura"
        
        if vendor_lower == 'cp':
            if "maestro" in lb_course_lower or "maestro" in sf_code_lower:
                return "maestro"
            if "ccse" in lb_course_lower or "ccse" in sf_code_lower:
                return "CCSE-R81.20"
        
        if vendor_lower == 'f5':
            return "bigip16.1.4"

        if vendor_lower == 'nu':
            return "nutanix-4"
        
        if vendor_lower == 'pa':
            if '1110-330' in lb_course_lower:
                return "1110-210"
        
        # Fallback to the labbuild course name if no specific rule matches.
        return lb_course_name or "unknown-version"
    
    def _create_contiguous_ranges(self, pod_numbers: List[Union[int,str]]) -> str:
        """Helper to convert a list of pod numbers into a comma-separated string of ranges."""
        if not pod_numbers: return ""
        int_pod_numbers = sorted(list(set(int(p) for p in pod_numbers)))
        if not int_pod_numbers: return ""
        ranges, start_range = [], int_pod_numbers[0]
        end_range = start_range
        for i in range(1, len(int_pod_numbers)):
            if int_pod_numbers[i] == end_range + 1:
                end_range = int_pod_numbers[i]
            else:
                ranges.append(f"{start_range}-{end_range}" if start_range != end_range else str(start_range))
                start_range = end_range = int_pod_numbers[i]
        ranges.append(f"{start_range}-{end_range}" if start_range != end_range else str(start_range))
        return ",".join(ranges)

    def generate_commands(self) -> List[APMCommand]:
        """Main public method to generate the full list of APM commands."""
        
        # --- MODIFICATION: The entire logic is now consolidated and refactored here ---
        
        # This dictionary will hold the consolidated data for each final APM user.
        # The key is a tuple: (username, server_location)
        desired_users: Dict[tuple, Dict] = defaultdict(lambda: {"pods": set(), "details": {}})
        
        # --- Phase 1: Consolidate all build commands into the desired user state ---
        for cmd in self.build_commands:
            target_server = 'us' if cmd.host.lower() in self.US_APM_HOSTS else 'au'
            user_key = (cmd.username, target_server)
            
            # Add this command's pods to the set for this user
            for pod in range(cmd.start_pod, cmd.end_pod + 1):
                desired_users[user_key]["pods"].add(pod)

            # Store the details from the first command we see for this user
            if not desired_users[user_key]["details"]:
                desired_users[user_key]["details"] = {
                    "password": cmd.password,
                    "trainer_name": cmd.trainer_name,
                    "sf_course_type": cmd.sf_course_type,
                    "labbuild_course": cmd.labbuild_course,
                    "vendor_shortcode": cmd.vendor_shortcode,
                    "sf_course_code": cmd.sf_course_code,
                    "tag": cmd.tag
                }

        delete_commands = []
        add_commands = []

        # --- Phase 2: Determine Deletions ---
        # Get a set of all usernames that are required in the new plan, regardless of server.
        all_desired_usernames = {key[0] for key in desired_users.keys()}

        for username, details in self.current_apm_state.items():
            is_needed = username in all_desired_usernames
            is_extended = details.get("vpn_auth_course_code") in self.extended_tags
            
            if not is_needed and not is_extended:
                source = details.get("source", "au")
                delete_commands.append(APMCommand(command="del", username=username, arguments=[], server=source))

        # --- Phase 3: Generate Add/Repurpose Commands ---
        for (username, server), data in desired_users.items():
            details = data["details"]
            
            # If this user already exists, it must be deleted first before being added back.
            # This handles both repurposing and region-moves.
            if username in self.current_apm_state:
                existing_details = self.current_apm_state[username]
                if existing_details.get("vpn_auth_course_code") not in self.extended_tags:
                    source = existing_details.get("source", "au")
                    del_command = APMCommand(command="del", username=username, arguments=[], server=source)
                    # Add to delete list only if not already there
                    if del_command not in delete_commands:
                        delete_commands.append(del_command)
            
            # Now, generate the 'add' command for every desired user.
            description = f"{details['trainer_name']} - {details['sf_course_type']}" if "Trainer" not in details['trainer_name'] else details['sf_course_type']
            
            # Handle Nutanix pod expansion here
            final_pods_for_range = data["pods"]
            if details['vendor_shortcode'] == 'nu':
                logical_pods = set()
                for physical_pod in data["pods"]:
                    start_logical = (physical_pod - 1) * 4 + 1
                    end_logical = physical_pod * 4
                    for p in range(start_logical, end_logical + 1):
                        logical_pods.add(p)
                final_pods_for_range = logical_pods

            pod_range_str = self._create_contiguous_ranges(list(final_pods_for_range))
            
            apm_version = self._determine_apm_version(details["vendor_shortcode"], details["labbuild_course"], details["sf_course_code"])
            
            add_command = APMCommand(
                command="add", username=username, server=server,
                arguments=[details["password"], pod_range_str, apm_version, description[:250], "8", details["tag"]]
            )
            add_commands.append(add_command)

        logger.info(f"APMManager: Generated {len(delete_commands)} deletions and {len(add_commands)} additions.")
        return sorted(delete_commands, key=lambda c: c.username) + sorted(add_commands, key=lambda c: c.username)


# --- Post-Processing Manager for Email Content ---
class EmailManager:
    """
    Groups build commands by trainer and original Salesforce course code,
    then generates a single, consolidated email for each group.
    """

    def __init__(self, build_context: BuildContext):
        self.context = build_context
        # We only want to send emails for the primary student builds, not the trainer pods.
        self.student_commands = [
            cmd for cmd in build_context.final_build_commands
            if "Trainer" not in cmd.trainer_name
        ]
        self.trainer_email_map = build_context.trainer_emails
        self.host_to_vcenter_map = build_context.host_to_vcenter_map
        self.locations_map = build_context.locations_map
        self.US_HOSTS = {"hotshot", "trypticon"}
        self.course_config_map = {c['course_name']: c for c in self.context.course_configs}

    def generate_emails(self) -> List[EmailContent]:
        """
        Main public method to generate a list of EmailContent objects.
        """
        emails_to_send = []
        
        commands_by_group = defaultdict(list)
        for command in self.student_commands:
            group_key = (command.trainer_name, command.sf_course_code)
            commands_by_group[group_key].append(command)
        
        for (trainer_name, sf_code), commands_in_group in commands_by_group.items():
            recipient_email = self.trainer_email_map.get(trainer_name)
            if not recipient_email:
                self.context.errors.append(f"No email address for trainer '{trainer_name}' (Course '{sf_code}').")
                continue

            subject, html_body = self._render_email_for_group(trainer_name, sf_code, commands_in_group)
            
            email = EmailContent(
                trainer_name=trainer_name,
                recipient_email=recipient_email,
                subject=subject,
                html_body=html_body,
                courses=commands_in_group
            )
            emails_to_send.append(email)
            
        logger.info(f"EmailManager: Generated {len(emails_to_send)} consolidated emails.")
        return emails_to_send

    def _render_email_for_group(self, trainer_name: str, sf_code: str, commands: List[LabBuildCommand]) -> tuple[str, str]:
        """
        Generates the email HTML. If there are multiple commands (e.g., for Maestro),
        it creates a separate row for each command, providing full detail.
        """
        subject = f"Lab Allocation for {sf_code}"
        
        # --- MODIFICATION: Generate a separate table row for each command in the group ---
        table_rows_html = ""
        all_hosts = {cmd.host for cmd in commands} # Collect all hosts for this group

        for cmd in sorted(commands, key=lambda c: c.labbuild_course):
            try:
                start_dt = datetime.strptime(cmd.start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(cmd.end_date, "%Y-%m-%d")
                date_range_display = f"{start_dt.strftime('%a')}-{end_dt.strftime('%a')}"
                end_day_abbr = end_dt.strftime("%a")
            except (ValueError, TypeError):
                date_range_display, end_day_abbr = "N/A", "N/A"
            
            pod_range_str = self._create_contiguous_ranges(list(range(cmd.start_pod, cmd.end_pod + 1)))
            num_pods = cmd.end_pod - cmd.start_pod + 1
            ram_for_row = num_pods * self._get_memory_for_course(cmd.labbuild_course)
            vcenter_display = self.host_to_vcenter_map.get(cmd.host, "N/A")
            location_display = self._find_location_from_code(sf_code)
            
            # The host display is now simple, just the hostname for this specific row.
            host_display_str = cmd.host

            table_rows_html += f"""
                <tr>
                    <td>{cmd.sf_course_code}</td>
                    <td>{date_range_display}</td>
                    <td>{end_day_abbr}</td>
                    <td>{location_display}</td>
                    <td>{cmd.sf_course_type}</td>
                    <td>{pod_range_str}</td>
                    <td>{cmd.username}</td>
                    <td>{cmd.password}</td>
                    <td align="center">{num_pods}</td>
                    <td align="center">{num_pods}</td>
                    <td>{cmd.labbuild_course}</td>
                    <td>{host_display_str}</td>
                    <td>{vcenter_display}</td>
                    <td align="right">{ram_for_row:.1f}</td>
                </tr>
            """
        
        # --- MODIFICATION: Create a dynamic lab access message ---
        has_us_host = any(h.lower() in self.US_HOSTS for h in all_hosts)
        has_au_host = any(h.lower() not in self.US_HOSTS for h in all_hosts)
        
        access_links = []
        if has_au_host:
            access_links.append("<a href='https://labs.rededucation.com' target='_blank'>labs.rededucation.com</a>")
        if has_us_host:
            access_links.append("<a href='https://labs.rededucation.us' target='_blank'>labs.rededucation.us</a>")
        
        lab_access_message = "Your pods can be accessed at: " + " and ".join(access_links)

        html_body = f"""
            <!DOCTYPE html><html><head><style>
                body{{font-family:Arial,sans-serif;font-size:10pt;color:#333}}
                table{{border-collapse:collapse;width:100%;border:1px solid #ccc}}
                th,td{{border:1px solid #ddd;padding:8px;text-align:left;vertical-align:top;white-space:nowrap}}
                th{{background-color:#f0f0f0;font-weight:700}} p{{margin-bottom:10px}}
            </style></head><body>
                <p>Dear {trainer_name},</p>
                <p>Here are the details for your course allocation ({sf_code}):</p>
                <p><strong>{lab_access_message}</strong></p>
                <table>
                    <thead><tr><th>Course Code</th><th>Date</th><th>Last Day</th><th>Location</th><th>Course Name</th><th>Start/End Pod</th><th>Username</th><th>Password</th><th>Students</th><th>Vendor Pods</th><th>Version</th><th>Virtual Host</th><th>vCenter</th><th>RAM (GB)</th></tr></thead>
                    <tbody>{table_rows_html}</tbody>
                </table>
                <p>Best regards,<br>Your Training Team</p>
            </body></html>
        """
        
        return subject, re.sub(r'\s+', ' ', html_body).strip()

    def _create_contiguous_ranges(self, pod_numbers: List[Union[int,str]]) -> str:
        if not pod_numbers: return ""
        int_pod_numbers = sorted(list(set(int(p) for p in pod_numbers)))
        if not int_pod_numbers: return ""
        ranges, start_range = [], int_pod_numbers[0]
        end_range = start_range
        for i in range(1, len(int_pod_numbers)):
            if int_pod_numbers[i] == end_range + 1:
                end_range = int_pod_numbers[i]
            else:
                ranges.append(f"{start_range}-{end_range}" if start_range != end_range else str(start_range))
                start_range = end_range = int_pod_numbers[i]
        ranges.append(f"{start_range}-{end_range}" if start_range != end_range else str(start_range))
        return ",".join(ranges)
    
    def _get_memory_for_course(self, course_name: str) -> float:
        if not course_name: return 0.0
        config = self.course_config_map.get(course_name)
        if config:
            mem_str = config.get('memory', config.get('memory_gb_per_pod', '0'))
            try:
                return float(mem_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse memory value '{mem_str}' for course '{course_name}'.")
                return 0.0
        logger.warning(f"Could not find configuration for course '{course_name}' to determine memory.")
        return 0.0
    
    def _find_location_from_code(self, course_code: str) -> str:
        """
        Finds the full location name from a Salesforce course code using the
        locations map loaded in the context.
        """
        if not course_code or not self.locations_map:
            return "N/A"
        
        # Sort keys by length, descending, to match longer codes first (e.g., 'VUS' before 'US')
        for loc_code in sorted(self.locations_map.keys(), key=len, reverse=True):
            if loc_code in course_code:
                return self.locations_map[loc_code]
        
        return "Virtual" # Default if no code is found in the string
    

# --- The Main Orchestrator Class ---
class BuildPlanner:
    """
    Orchestrates the entire build planning process from start to finish.
    This is the main public-facing class for this module, designed to be
    instantiated and used directly from a Flask route.
    """

    VENDOR_MAPPING = {
        'pa': DefaultVendorPlanner,
        'cp': CheckpointPlanner,
        'nu': DefaultVendorPlanner,
        'av': DefaultVendorPlanner,
        'pr': DefaultVendorPlanner,
        'f5': F5Planner,
    }

    def plan_selected_courses(self, selected_sf_courses: List[Dict]) -> BuildContext:
        """
        Runs the build planning process ONLY for a list of user-selected courses.
        This is the primary entry point for the interactive "Upcoming Courses" page.
        """
        logger.info(f"Starting new interactive build plan for {len(selected_sf_courses)} selected courses.")
        
        initial_data = self._fetch_initial_data(fetch_sf_data=False)
        if initial_data.get("error"):
            ctx = BuildContext([], [], [], {}, {})
            ctx.errors.append(initial_data["error"])
            return ctx

        context = BuildContext(
            all_hosts=initial_data["hosts_list"],
            course_configs=initial_data["course_configs"],
            build_rules=initial_data["build_rules"],
            trainer_emails=initial_data["trainer_emails"],
            host_to_vcenter_map=initial_data["host_to_vcenter_map"],
            locations_map=initial_data["locations_map"]
        )
        
        # --- MODIFICATION: Initialize the APM allocator with the current state ---
        context.apm_allocator.populate_initial_state(
            initial_data["apm_state"],
            initial_data["extended_tags"]
        )
        
        self._populate_locked_resources(context)
        for host in context.all_hosts:
            context.host_capacities[host] = 2000.0
        
        courses_by_vendor = defaultdict(list)
        for course in selected_sf_courses:
            course['preselect_labbuild_course'] = course.get('user_selected_labbuild_course')
            vendor = course.get('vendor')
            if vendor:
                courses_by_vendor[vendor].append(course)

        vendor_planning_order = ['cp', 'f5', 'pa', 'nu', 'av', 'pr']
        
        for vendor_code in vendor_planning_order:
            if vendor_code in courses_by_vendor:
                courses_to_plan = courses_by_vendor[vendor_code]
                PlannerClass = self.VENDOR_MAPPING.get(vendor_code, DefaultVendorPlanner)
                planner = PlannerClass(vendor_code, context)
                planner.plan_builds(courses_to_plan)
        
        # Run the trainer pod planner after all student pods are allocated
        trainer_planner = TrainerPodPlanner(context)
        trainer_planner.plan_trainer_pods(selected_sf_courses)
        
        if context.final_build_commands:
            apm_manager = APMManager(
                build_context=context,
                current_apm_state=initial_data["apm_state"],
                extended_tags=initial_data["extended_tags"]
            )
            context.final_apm_commands = apm_manager.generate_commands()

            email_manager = EmailManager(build_context=context)
            context.final_emails = email_manager.generate_emails()

            lab_report_manager = CurrentLabReportManager(context)
            context.lab_report_data = lab_report_manager.generate_report_data()

            trainer_report_manager = TrainerPodReportManager(context)
            context.trainer_report_data = trainer_report_manager.generate_report_data()
            
        logger.info(f"Interactive planning complete. Generated {len(context.final_build_commands)} total build commands.")
        return context

    def _fetch_initial_data(self, fetch_sf_data: bool = True) -> Dict:
        """A private helper that consolidates all data fetching operations."""
        try:
            hosts_docs = list(host_collection.find({}, {"host_name": 1, "vcenter": 1}))
            hosts_list = [h['host_name'] for h in hosts_docs]
            host_to_vcenter_map = {h['host_name']: h.get('vcenter', 'N/A').split('.')[0] for h in hosts_docs}
            course_configs = list(course_config_collection.find({}))
            build_rules = list(build_rules_collection.find().sort("priority", 1))
            trainer_emails = {t['trainer_name']: t['email_address'] for t in trainer_email_collection.find({"active": True})}

            locations_map = {
                loc['code']: loc['name'] 
                for loc in locations_collection.find({}) 
                if 'code' in loc and 'name' in loc
            }
            
            # --- MODIFICATION: Fetch full APM state, not just a dummy dict ---
            apm_state = {}
            apm_sources = {
                'au': os.getenv("APM_LIST_URL", "http://connect:1212/list"),
                'us': os.getenv("APM_LIST_URL_US", "http://connect:1212/list?us=True")
            }
            for source, url in apm_sources.items():
                try:
                    response = requests.get(url, timeout=15)
                    response.raise_for_status()
                    for username, details in response.json().items():
                        if username not in apm_state:
                            details['source'] = source
                            apm_state[username] = details
                except Exception as e:
                    logger.error(f"Could not fetch APM state from {source.upper()} server: {e}")
            # --- END MODIFICATION ---

            extended_tags = {doc['tag'] for doc in alloc_collection.find({"extend": "true"}, {"tag": 1})}

            sf_courses = None
            if fetch_sf_data:
                sf_courses = get_upcoming_courses_data(build_rules, course_configs, hosts_list)
                if sf_courses is None:
                    return {"error": "Failed to retrieve course data from Salesforce."}

            return {
                "hosts_list": hosts_list, "course_configs": course_configs,
                "build_rules": build_rules, "sf_courses": sf_courses,
                "trainer_emails": trainer_emails, "apm_state": apm_state,
                "extended_tags": extended_tags, "host_to_vcenter_map": host_to_vcenter_map,
                "locations_map": locations_map,
                "error": None
            }
        except Exception as e:
            logger.error(f"Failed to fetch initial data for build planner: {e}", exc_info=True)
            return {"error": "A critical error occurred while fetching initial planning data."}
            
    def _populate_locked_resources(self, context: BuildContext):
        """
        Queries for allocations with extend:"true" and populates the
        BuildContext's 'pods_in_use' set with these locked resources.
        """
        logger.info("Populating build context with locked pods from 'extend:\"true\"' allocations.")
        try:
            locked_allocations = alloc_collection.find({"extend": "true"})
            locked_count = 0
            for doc in locked_allocations:
                for course in doc.get("courses", []):
                    vendor = course.get("vendor")
                    if not vendor: continue
                    
                    for pd in course.get("pod_details", []):
                        if pd.get("pod_number") is not None:
                            context.pods_in_use[vendor].add(int(pd["pod_number"]))
                            locked_count += 1
                        if vendor == 'f5' and pd.get("class_number") is not None:
                            context.pods_in_use[vendor].add(int(pd["class_number"]))
                            locked_count += 1
                        for nested_pod in pd.get("pods", []):
                            if nested_pod.get("pod_number") is not None:
                                context.pods_in_use[vendor].add(int(nested_pod["pod_number"]))
                                locked_count += 1
            
            logger.info(f"Finished locking resources. {locked_count} pods/classes are marked as in-use.")
        except Exception as e:
            logger.error(f"Error populating locked resources: {e}", exc_info=True)
            context.errors.append("Failed to load list of currently extended/locked labs.")


# --- The Final Execution and Scheduling Class ---
class ExecutionPlanner:
    """
    Takes a finalized build plan (BuildContext) and user preferences, and then
    orchestrates the entire execution and scheduling process.
    """
    def __init__(self, build_context: BuildContext, start_time: datetime, global_teardown: bool):
        """
        Initializes the Execution Planner.

        :param build_context: The fully populated BuildContext from the BuildPlanner.
        :param start_time: A timezone-aware datetime object indicating when the first job should run.
        :param global_teardown: A boolean flag indicating if a global cleanup should be performed.
        """
        self.context = build_context
        self.start_time = start_time
        self.global_teardown = global_teardown
        
        # --- Scheduling Parameters ---
        # The number of concurrent jobs allowed to run.
        self.CONCURRENCY_LIMIT = 3
        # The time to wait between starting jobs on the same host track.
        self.STAGGER_MINUTES = 30

    def execute_plan(self) -> Dict[str, int]:
        """
        Main public method to execute the full plan.
        
        Returns a dictionary summarizing the scheduled operations.
        """
        logger.info("ExecutionPlanner: Starting execution of the build plan.")
        
        # Phase 1: Generate teardown commands if requested by the user.
        teardown_commands = []
        if self.global_teardown:
            teardown_commands = self._generate_global_teardown_commands()

        # Phase 2: Use the "Smart Stagger" algorithm to calculate the exact run time for every command.
        # This is the core of the concurrency logic.
        final_scheduled_commands = self._calculate_scheduled_times(
            teardown_commands=teardown_commands,
            setup_commands=self.context.final_build_commands
        )

        # Phase 3: Persist the final, timed plan to the database for tracking.
        if not self._persist_plan_to_database(final_scheduled_commands):
            self.context.errors.append("Failed to save the final scheduled plan to the database.")
            # We will still attempt to schedule, but log this critical failure.
        
        # Phase 4: Submit all the calculated jobs to the APScheduler.
        scheduled_count = self._schedule_jobs_with_apscheduler(final_scheduled_commands)

        logger.info("ExecutionPlanner: Plan execution finished.")
        return {
            "teardowns_scheduled": len(teardown_commands),
            "setups_scheduled": len(self.context.final_build_commands),
            "total_jobs": scheduled_count
        }

    def _generate_global_teardown_commands(self) -> List[LabBuildCommand]:
        """
        Queries the database for all non-extended labs and creates teardown
        commands for them.
        """
        logger.info("ExecutionPlanner: Generating global teardown commands for non-extended labs.")
        teardown_cmds = []
        try:
            non_extended_docs = alloc_collection.find({"extend": {"$ne": "true"}})
            for doc in non_extended_docs:
                for course in doc.get("courses", []):
                    # For teardown, we only need a subset of the command attributes
                    cmd = LabBuildCommand(
                        vendor_shortcode=course.get("vendor"),
                        labbuild_course=course.get("course_name"),
                        tag=doc.get("tag"),
                        # These fields are placeholders for teardown commands
                        start_pod=0, end_pod=0, host="", trainer_name="", username="",
                        password="", start_date="", end_date=""
                    )
                    # A real implementation would parse pod_details to create precise teardown commands.
                    # For now, we'll assume a simplified teardown command per course entry.
                    teardown_cmds.append(cmd)
            
            logger.info(f"ExecutionPlanner: Generated {len(teardown_cmds)} global teardown commands.")
        except Exception as e:
            self.context.errors.append("A database error occurred during global teardown planning.")
            logger.error(f"Error generating global teardown commands: {e}", exc_info=True)
            
        return teardown_cmds

    def _calculate_scheduled_times(self, teardown_commands: List[LabBuildCommand], setup_commands: List[LabBuildCommand]) -> List[LabBuildCommand]:
        """
        The "Smart Stagger" algorithm.
        Calculates the precise start time for every command to ensure no more than
        CONCURRENCY_LIMIT jobs run at once, and that they are on different hosts.
        """
        # Initialize three "tracks", each with the initial start time.
        # These tracks represent our concurrent worker slots.
        host_tracks = {i: self.start_time for i in range(self.CONCURRENCY_LIMIT)}
        
        # A map to assign a host to a specific track, ensuring all jobs for one host run sequentially.
        host_to_track_map = {}
        next_track = 0
        
        stagger_delta = datetime.timedelta(minutes=self.STAGGER_MINUTES)
        all_commands = teardown_commands + setup_commands
        final_timed_commands = []

        for command in all_commands:
            # If we haven't seen this host before, assign it to the next available track.
            if command.host not in host_to_track_map:
                host_to_track_map[command.host] = next_track
                next_track = (next_track + 1) % self.CONCURRENCY_LIMIT
            
            assigned_track = host_to_track_map[command.host]
            
            # The start time for this command is the current time of its assigned track.
            command.scheduled_run_time = host_tracks[assigned_track]
            final_timed_commands.append(command)
            
            # Advance this track's time for the next job that will run on it.
            host_tracks[assigned_track] += stagger_delta
            
        return final_timed_commands

    def _persist_plan_to_database(self, final_commands: List[LabBuildCommand]) -> bool:
        """Saves the fully scheduled plan to the interimallocation collection."""
        # This method would contain the logic to format and save the final plan.
        # For brevity, we will assume this step is successful.
        logger.info(f"Persisting {len(final_commands)} scheduled commands to the database.")
        # In a real implementation:
        # interim_alloc_collection.insert_many([...formatted command data...])
        return True

    def _schedule_jobs_with_apscheduler(self, final_commands: List[LabBuildCommand]) -> int:
        """Submits all the calculated commands to the APScheduler."""
        from dashboard.extensions import scheduler
        from dashboard.tasks import run_labbuild_task
        from apscheduler.triggers.date import DateTrigger
        
        if not scheduler or not scheduler.running:
            self.context.errors.append("APScheduler is not running. Cannot schedule jobs.")
            logger.error("ExecutionPlanner: Cannot schedule jobs, scheduler is not available.")
            return 0
        
        count = 0
        for command in final_commands:
            # Teardown commands need different arguments than setup commands
            if "Teardown" in command.sf_course_type: # A simple way to identify teardown
                 args = ['teardown', '-v', command.vendor_shortcode, '-g', command.labbuild_course, '-t', command.tag]
            else:
                 args = command.to_args_list()

            try:
                scheduler.add_job(
                    run_labbuild_task,
                    trigger=DateTrigger(run_date=command.scheduled_run_time),
                    args=[args],
                    name=f"BuildPlan_{command.tag}",
                    misfire_grace_time=3600 # Allow job to run up to 1 hour late if scheduler was down
                )
                count += 1
            except Exception as e:
                self.context.errors.append(f"Failed to schedule job for tag '{command.tag}'.")
                logger.error(f"Error submitting job to APScheduler for tag '{command.tag}': {e}", exc_info=True)
                
        logger.info(f"Successfully submitted {count} jobs to APScheduler.")
        return count


class CurrentLabReportManager:
    """
    Transforms the final build plan into the data structure required by the
    main "Current Lab Report" Excel generator.
    """
    def __init__(self, build_context: BuildContext):
        self.context = build_context
        self.commands = build_context.final_build_commands
        self.course_config_map = {c['course_name']: c for c in self.context.course_configs}
        self.US_HOSTS = {"hotshot", "trypticon"}

    def generate_report_data(self) -> Dict[str, Any]:
        """
        Generates the final dictionary of data for the Excel report.
        """
        standard_pods, trainer_pods = [], []

        for cmd in self.commands:
            # Transform the LabBuildCommand object into the flat dictionary format
            num_pods = cmd.end_pod - cmd.start_pod + 1
            pod_range = f"{cmd.start_pod}-{cmd.end_pod}" if num_pods > 1 else str(cmd.start_pod)
            
            report_item = {
                'course_code': cmd.sf_course_code,
                'us_au_location': "US" if cmd.host.lower() in self.US_HOSTS else "AU",
                'pod_type': 'trainer' if cmd.trainer_name == "Trainer" else 'standard',
                'start_end_pod': pod_range,
                'location': self._find_location_from_code(cmd.sf_course_code),
                'course_start_date': cmd.start_date,
                'last_day': cmd.end_date,
                'username': cmd.username,
                'password': cmd.password,
                'trainer_name': cmd.trainer_name,
                'ram': _get_memory_for_course_local(cmd.labbuild_course, self.course_config_map),
                'vendor_pods': num_pods,
                'students': num_pods,
                'course_name': cmd.sf_course_type,
                'version': cmd.labbuild_course,
                'course_version': cmd.labbuild_course,
                'virtual_hosts': cmd.host,
                'vcenter_name': self.context.host_to_vcenter_map.get(cmd.host, "N/A")
            }
            
            if report_item['pod_type'] == 'trainer':
                trainer_pods.append(report_item)
            else:
                standard_pods.append(report_item)

        return {
            "standard_pods": standard_pods,
            "trainer_pods": trainer_pods,
            "extended_pods": [], # No extended pods in a new plan
            "host_map": {h.capitalize()[:2]: h for h in self.context.all_hosts}
        }

    def _find_location_from_code(self, course_code: str) -> str:
        if not course_code or not self.context.locations_map: return "N/A"
        for loc_code in sorted(self.context.locations_map.keys(), key=len, reverse=True):
            if loc_code in course_code: return self.context.locations_map[loc_code]
        return "Virtual"


# --- NEW: Report Manager for the Trainer Pod Report ---
class TrainerPodReportManager:
    """
    Transforms the final build plan into the data structure required by the
    "Trainer Pod Allocation" Excel generator.
    """
    def __init__(self, build_context: BuildContext):
        self.context = build_context
        self.trainer_commands = [cmd for cmd in build_context.final_build_commands if cmd.trainer_name == "Trainer"]
        self.course_config_map = {c['course_name']: c for c in self.context.course_configs}

    def generate_report_data(self) -> List[Dict[str, Any]]:
        """
        Generates the final flat list of trainer pod data for the Excel report.
        """
        report_data = []

        for cmd in self.trainer_commands:
            # Un-consolidate the command block into individual pod entries
            for pod_num in range(cmd.start_pod, cmd.end_pod + 1):
                report_item = {
                    'course_name': cmd.sf_course_type,
                    'pod_number': pod_num,
                    'username': cmd.username,
                    'password': cmd.password,
                    'version': cmd.labbuild_course,
                    'ram': _get_memory_for_course_local(cmd.labbuild_course, self.course_config_map),
                    'class': cmd.f5_class_number if cmd.f5_class_number is not None else '',
                    'host': cmd.host,
                    'vcenter': self.context.host_to_vcenter_map.get(cmd.host, "N/A"),
                    'taken_by': '', # Placeholder
                    'notes': ''     # Placeholder
                }
                report_data.append(report_item)
        
        return sorted(report_data, key=lambda x: (x['course_name'], x['pod_number']))