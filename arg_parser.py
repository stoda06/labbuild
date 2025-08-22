# In arg_parser.py

import argparse
import argcomplete
from commands import (
    setup_environment, teardown_environment, manage_environment, 
    test_environment, move_environment, migrate_environment
)

def create_parser():
    """
    Creates and configures the argparse object for the labbuild tool.
    """
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")

    parser.add_argument('--verbose', action='store_true', help='Enable debug logging.')

    # --- Subparsers for Commands ---
    subparsers = parser.add_subparsers(dest='command', title='commands',
                                       help='Action command (setup, manage, teardown, test, move, migrate)')

    # --- Common Arguments for Subparsers ---
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-g', '--course', help='Course name or "?".')
    common_parser.add_argument('-t', '--tag', help='A unique tag for the allocation group.')
    common_parser.add_argument('-th', '--thread', type=int, default=4, help='Concurrency thread count.')
    common_parser.add_argument('-v', '--vendor', required=True, help='Vendor code (e.g., pa, cp, f5). Required.')
    common_parser.add_argument('-y', '--yes', action='store_true', 
                               help='Automatically answer yes to all confirmation prompts (non-interactive mode).')

    pod_range_parser = argparse.ArgumentParser(add_help=False)
    pod_range_parser.add_argument('-s', '--start-pod', type=int, help='Start pod # for action.')
    pod_range_parser.add_argument('-e', '--end-pod', type=int, help='End pod # for action.')

    f5_parser = argparse.ArgumentParser(add_help=False)
    f5_parser.add_argument('-cn', '--class_number', type=int, help='Class number (required for F5).')

    # --- Help Text Definitions ---
    setup_help = (
        "Set up a lab environment. Run in one of two modes:\n"
        "1. Manual: Requires --course, --tag, and --host.\n"
        "2. Auto-Lookup: Omit --course and --tag; provide --vendor, --start-pod, and --end-pod to run on existing allocations."
    )
    manage_help = (
        "Manage VM power states. Run in one of two modes:\n"
        "1. Manual: Requires --course, --tag, and --host.\n"
        "2. Auto-Lookup: Omit --course and --tag; provide --vendor, --start-pod, and --end-pod to run on existing allocations."
    )
    teardown_help = (
        "Tear down a lab environment. Run in one of two modes:\n"
        "1. Manual: Requires --course, --tag, and --host.\n"
        "2. Auto-Lookup: Omit --course and --tag; provide --vendor, --start-pod, and --end-pod to run on existing allocations."
    )

    # --- Setup Subparser ---
    setup_parser = subparsers.add_parser('setup', help=setup_help,
                                         formatter_class=argparse.RawTextHelpFormatter,
                                         parents=[common_parser, pod_range_parser, f5_parser])
    setup_parser.add_argument('-c', '--component', help='Specify components or use "?" to list.')
    setup_parser.add_argument('-ds', '--datastore', default="vms", help='Target datastore.')
    setup_parser.add_argument('-r', '--re-build', action='store_true', help='Force delete existing components before build.')
    setup_parser.add_argument('-mem', '--memory', type=int, help='Specify memory for specific components.')
    setup_parser.add_argument('--full', action='store_true', help='Perform a full clone instead of linked.')
    setup_parser.add_argument('--monitor-only', action='store_true', help='Only create/update monitoring entries.')
    setup_parser.add_argument('--prtg-server', help='Specify target PRTG server for monitoring.')
    setup_parser.add_argument('--perm', action='store_true', help='Only apply permissions (specific vendors).')
    setup_parser.add_argument('--db-only', action='store_true', help='Only update the database allocation record.')
    setup_parser.add_argument('--clonefrom', type=int, metavar='SOURCE_POD',
                            help='Source pod number to clone VMs from (primarily for Checkpoint).')
    setup_parser.add_argument('--start-date', help='Course start date (YYYY-MM-DD).')
    setup_parser.add_argument('--end-date', help='Course end date (YYYY-MM-DD).')
    setup_parser.add_argument('--host', help='Specify the host on which the pods need to be built.')
    setup_parser.add_argument('--trainer-name', help='Name of the trainer.')
    setup_parser.add_argument('--username', help='APM username for the allocation.')
    setup_parser.add_argument('--password', help='APM password for the allocation.')
    setup_parser.set_defaults(func=setup_environment)

    # --- Manage Subparser ---
    manage_parser = subparsers.add_parser('manage', help=manage_help,
                                          formatter_class=argparse.RawTextHelpFormatter,
                                          parents=[common_parser, pod_range_parser, f5_parser])
    manage_parser.add_argument('-c', '--component', help='Specify components or use "?" to list.')
    manage_parser.add_argument('--host', help='Specify the host on which the pods need to be built.')
    manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True, help='Power operation to perform.')
    manage_parser.set_defaults(func=manage_environment)
 
    # --- Teardown Subparser ---
    teardown_parser = subparsers.add_parser('teardown', help=teardown_help,
                                            formatter_class=argparse.RawTextHelpFormatter,
                                            parents=[common_parser, pod_range_parser, f5_parser])
    teardown_parser.add_argument('--host', help='Specify the host on which the pods need to be built.')
    teardown_parser.add_argument('--monitor-only', action='store_true', help='Only remove monitoring entries.')
    teardown_parser.add_argument('--db-only', action='store_true', help='Only remove database allocation record.')
    teardown_parser.set_defaults(func=teardown_environment)

    # --- Test Subparser ---
    test_parser = subparsers.add_parser("test", help="Run test suite for labs", parents=[f5_parser])
    test_parser.add_argument("--db", action='store_true', help="List allocations from the database with test-related info.")
    test_parser.add_argument("-t", "--tag", help="Run tests for a specific allocation by tag name.")
    test_parser.add_argument("-v", "--vendor", help="Vendor name. Used for vendor-wide tests or to filter --db list.")
    test_parser.add_argument("-s", "--start_pod", type=int, help="Start pod/class number. Used for range tests or to filter --db list.")
    test_parser.add_argument("-e", "--end_pod", type=int, help="End pod/class number. Used for range tests or to filter --db list.")
    test_parser.add_argument("-H", "--host", help="ESXi host name (optional, used to filter manual range tests).")
    test_parser.add_argument("-g", "--group", help="Course group/section (optional, used to filter manual range tests).")
    test_parser.add_argument("-c", "--component", help="Test specific component(s), or '?' to list available.")
    test_parser.add_argument("-x", "--exclude", help="Exclude pods/classes from vendor-wide test. E.g., '1-5,10,22-25'")
    test_parser.set_defaults(func=test_environment)

    # --- Move Subparser ---
    move_parser = subparsers.add_parser('move', help='Move pod VMs to correct folder and resource pool.',
                                        parents=[common_parser, pod_range_parser, f5_parser])
    move_parser.set_defaults(func=move_environment)

    # --- Migrate Subparser ---
    migrate_parser = subparsers.add_parser('migrate', help='Migrate pod VMs from a source host to a destination host.',
                                           parents=[common_parser, pod_range_parser])
    migrate_parser.add_argument('--host', help='Specify the host on which the pods need to be built.')
    migrate_parser.add_argument('-d', '--destination-host', required=True, help='The destination host for the migration.')
    migrate_parser.set_defaults(func=migrate_environment)

    argcomplete.autocomplete(parser)
    return parser