# LabBuild: Lab Environment Management Tool

LabBuild is a powerful, integrated tool for orchestrating the lifecycle of virtual lab environments. It provides both a comprehensive Command-Line Interface (CLI) for automation and a user-friendly web-based Dashboard for management and planning.

The tool is designed to interact with VMware vCenter and other services to automate the setup, management, testing, and teardown of complex, multi-VM lab pods for various technology vendors like Palo Alto, Checkpoint, F5, and more.

## ✨ Key Features

*   **Dual Interface**: Manage labs via a powerful CLI or an intuitive web dashboard.
*   **Full Lifecycle Management**: Automate `setup`, `teardown`, `manage` (power), `test`, `move`, and `migrate` operations.
*   **Configuration Driven**: Lab definitions are stored in a MongoDB database, allowing for consistent and repeatable builds.
*   **Flexible Operations**:
    *   **Manual Mode**: Build specific pods with precise control over course, tag, and host.
    *   **Auto-Lookup Mode**: Perform operations on existing allocations based on vendor and pod range without needing to specify course or tag.
*   **Batch Processing**: Execute operations on ranges of pods or entire vendor allocations in parallel.
*   **Dashboard for Planning & Visibility**:
    *   View and filter all current lab allocations.
    *   Plan future builds based on data from Salesforce.
    *   Review, edit, and schedule build plans.
    *   Monitor recent and scheduled jobs.
    *   View detailed operational logs in real-time.

## 📋 Table of Contents

1.  [Prerequisites](#-prerequisites)
2.  [Installation](#-installation)
3.  [Configuration](#-configuration)
4.  [CLI Usage](#-cli-usage)
    -   [Global Options](#global-options)
    -   [Commands](#commands)
        -   [`setup`](#setup-command)
        -   [`manage`](#manage-command)
        -   [`teardown`](#teardown-command)
        -   [`test`](#test-command)
        -   [`move`](#move-command)
        -   [`migrate`](#migrate-command)
5.  [Dashboard Usage](#-dashboard-usage)
    -   [Running the Dashboard](#running-the-dashboard)
    -   [Dashboard Features](#dashboard-features)
        -   [Home/Dashboard](#homedashboard)
        -   [Allocations](#allocations)
        -   [Upcoming Courses & Build Planner](#upcoming-courses--build-planner)
        -   [Terminal](#terminal)
        -   [Logs](#logs)
        -   [Settings](#settings)

## ⚙️ Prerequisites

*   Python 3.10+
*   MongoDB Server
*   Redis Server (for real-time log streaming in the dashboard)
*   Access to a VMware vCenter instance

## 🔧 Installation

1.  Clone the repository:
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## 📝 Configuration

The tool uses a `.env` file in the root directory for configuration. Create this file and populate it with the necessary credentials and endpoints.

```dotenv
# MongoDB Configuration
MONGO_HOST=your_mongodb_host
MONGO_USER=your_mongo_user
MONGO_PASSWORD=your_mongo_password

# vCenter Configuration
VC_USER=your_vcenter_user
VC_PASS=your_vcenter_password
# Set to 'true' to disable SSL cert verification (use with caution)
VC_DISABLE_SSL_VERIFY=False

# Flask Dashboard Configuration
FLASK_SECRET_KEY=a_strong_and_secret_key_for_sessions
FLASK_HOST=0.0.0.0
FLASK_PORT=5001
FLASK_DEBUG=True

# Redis for Real-time Logging
REDIS_URL=redis://localhost:6379/0

# Salesforce Integration (for Dashboard Build Planner)
SF_USERNAME=your_salesforce_email
SF_PASSWORD=your_salesforce_password
SF_SECURITY_TOKEN=your_salesforce_security_token
SF_REPORT_ID=the_id_of_the_salesforce_report

# SMTP for Email Notifications (from Dashboard)
SMTP_HOST=your_smtp_server.com
SMTP_PORT=587
SMTP_USER=your_smtp_user
SMTP_PASSWORD=your_smtp_password
SMTP_SENDER_EMAIL=notifications@example.com
SMTP_SENDER_DISPLAY_NAME="LabBuild Notifications"
SMTP_USE_TLS=True
SMTP_TEST_RECIPIENT=test-recipient@example.com
```

##  CLI Usage

The CLI is the core automation engine of LabBuild.

### Global Options

*   `--verbose`: Enables detailed debug logging for the operation.

### Commands

The general syntax is `python labbuild.py <command> [options]`.

---

#### `setup` Command

Sets up a new lab environment. It can be run in two primary modes.

**Arguments:**

*   **Common:**
    *   `-v, --vendor`: **(Required)** Vendor code (e.g., `pa`, `cp`, `f5`).
    *   `-g, --course`: Course name (e.g., `CCSA-R81.20`) or `?` to list available courses for the vendor.
    *   `-t, --tag`: A unique tag for the allocation group.
    *   `-s, --start-pod`: The starting pod number for the action.
    *   `-e, --end-pod`: The ending pod number for the action.
    *   `-cn, --class_number`: **(Required for F5)** The class number.
    *   `--host`: **(Required in Manual Mode)** The target ESXi host.
*   **Setup-Specific:**
    *   `-c, --component`: Specify components to build, or `?` to list available components for the course.
    *   `-r, --re-build`: Force deletes existing components before building.
    *   `--full`: Perform a full clone instead of a linked clone.
    *   `--clonefrom SOURCE_POD`: (Primarily for Checkpoint) Source pod number to clone VMs from.
    *   `--db-only`: Only creates or updates the database allocation record; does not touch vCenter.
    *   `--monitor-only`: Only creates or updates monitoring entries (e.g., in PRTG).
    *   `--perm`: (Vendor-specific) Only applies permissions.
*   **Metadata:**
    *   `--start-date`, `--end-date`: Course dates in `YYYY-MM-DD` format.
    *   `--trainer-name`: Name of the trainer.
    *   `--username`, `--password`: APM credentials for the allocation.

**Modes of Operation:**

1.  **Manual Mode:** Manually specify all details for a new build. Requires `--course`, `--tag`, and `--host`.
    ```bash
    # Build pods 1-5 for a Checkpoint course on host 'hydra'
    python labbuild.py setup -v cp -g CCSA-R81.20 -t SF-COURSE-123 --host hydra -s 1 -e 5

    # Build a specific F5 class with pods 1-2
    python labbuild.py setup -v f5 -g F5-LTM-101 -t F5-CLASS-456 --host nightbird -cn 22 -s 1 -e 2
    ```

2.  **Auto-Lookup Mode:** Re-run setup on an *existing* allocation without specifying the course or tag. LabBuild looks up the details from the database.
    ```bash
    # Re-run setup for existing Palo Alto pods 10-12
    python labbuild.py setup -v pa -s 10 -e 12
    ```

---

#### `manage` Command

Manages the power state of virtual machines in an environment.

**Arguments:**

*   **Required Operation:**
    *   `-o, --operation`: The power operation to perform. Choices: `start`, `stop`, `reboot`.
*   **Common:**
    *   All common arguments (`-v`, `-g`, `-t`, `-s`, `-e`, `-cn`, `--host`) are supported.
*   **Manage-Specific:**
    *   `-c, --component`: Specify components to target. If omitted, all components are targeted.

**Modes of Operation:**

1.  **Manual Mode:** Target a specific allocation you know the details of.
    ```bash
    # Stop all VMs in pods 1-5 for a specific tag and course
    python labbuild.py manage -v cp -g CCSA-R81.20 -t SF-COURSE-123 --host hydra -s 1 -e 5 -o stop

    # Reboot only the 'fw1' component in pod 3
    python labbuild.py manage -v pa -g PCNSA -t SF-COURSE-789 --host unicron -s 3 -e 3 -o reboot -c fw1
    ```

2.  **Auto-Lookup Mode:** Target existing allocations by vendor and pod range.
    ```bash
    # Start all VMs in existing F5 class 22, pods 1-2
    python labbuild.py manage -v f5 -cn 22 -s 1 -e 2 -o start
    ```

---

#### `teardown` Command

Tears down a lab environment, deleting VMs and cleaning up resources.

**Arguments:**

*   **Common:**
    *   All common arguments (`-v`, `-g`, `-t`, `-s`, `-e`, `-cn`, `--host`) are supported.
*   **Teardown-Specific:**
    *   `--db-only`: Only removes the database allocation record.
    *   `--monitor-only`: Only removes the monitoring entries.

**Modes of Operation:**

1.  **Manual Mode:** Tear down a specific, known allocation.
    ```bash
    # Tear down pods 1-5 for a specific tag and course
    python labbuild.py teardown -v cp -g CCSA-R81.20 -t SF-COURSE-123 --host hydra -s 1 -e 5
    ```

2.  **Auto-Lookup Mode:** Tear down existing allocations by vendor and pod range.
    ```bash
    # Tear down all resources for existing Palo Alto pods 10-12
    python labbuild.py teardown -v pa -s 10 -e 12
    ```

---

#### `test` Command

Runs a suite of tests against lab environments to verify their status and connectivity.

**Arguments:**

*   `--db`: List allocations from the database with test-related info. Can be filtered with `-v`, `-s`, `-e`.
*   `-t, --tag`: Run tests for a specific allocation by its tag name.
*   `-v, --vendor`: Run tests for all allocations of a specific vendor.
*   `-s, --start_pod` & `-e, --end_pod`: Define a range of pods/classes to test.
*   `-cn, --class_number`: Specify the class number for F5 tests.
*   `-H, --host`: (Optional) Filter manual range tests by ESXi host.
*   `-g, --group`: (Optional) Filter manual range tests by course group/name.
*   `-c, --component`: Test specific component(s), or `?` to list available.
*   `-x, --exclude`: Exclude specific pods/classes from a vendor-wide test (e.g., `-x "1-5,10,22-25"`).

**Modes of Operation:**

1.  **Database Listing Mode:**
    ```bash
    # List all Checkpoint allocations in the database
    python labbuild.py test --db -v cp
    ```

2.  **Tag-Based Test:**
    ```bash
    # Test all resources associated with a specific tag
    python labbuild.py test -t SF-COURSE-123
    ```

3.  **Vendor-Wide Test:**
    ```bash
    # Test all Checkpoint allocations, excluding pods 1 through 5
    python labbuild.py test -v cp -x "1-5"
    ```

4.  **Manual Range Test:**
    ```bash
    # Manually specify parameters to test a range of pods, even without a known tag
    python labbuild.py test -v pa -g PCNSA -H unicron -s 10 -e 12
    ```

---

#### `move` Command

Moves pod VMs to their correct folder and resource pool in vCenter. Useful for cleanup and organization.

**Arguments:**

*   All common arguments (`-v`, `-g`, `-t`, `-s`, `-e`, `-cn`, `--host`) are supported.

**Example:**

```bash
# Move VMs for pods 1-5 of a specific course to their correct containers
python labbuild.py move -v pa -g PCNSA -t SF-COURSE-789 --host unicron -s 1 -e 5
```

---

#### `migrate` Command

Migrates pod VMs from a source host to a destination host, supporting both intra- and inter-vCenter migrations.

**Arguments:**

*   `-d, --destination-host`: **(Required)** The destination host for the migration.
*   All common arguments (`-v`, `-g`, `-t`, `-s`, `-e`, `--host`) are supported.

**Example:**

```bash
# Migrate pods 1-5 from host 'hydra' to 'trypticon'
python labbuild.py migrate -v cp -g CCSA-R81.20 -t SF-COURSE-123 --host hydra -s 1 -e 5 -d trypticon
```

## 🖥️ Dashboard Usage

The Dashboard provides a web-based UI for at-a-glance visibility, manual control, and powerful build planning workflows.

### Running the Dashboard

Start the Flask web server from the project's root directory:

```bash
python run.py
```

The dashboard will be available at `http://<your-host>:<your-port>` (default: `http://0.0.0.0:5001`).

### Dashboard Features

#### Home/Dashboard

*   **Execute Operation**: A form that provides a UI for all the major CLI commands (`setup`, `manage`, `teardown`). Includes advanced options that map to CLI flags.
*   **Schedule Batch from File**: Upload a text file with one `labbuild` command per line to schedule a sequence of operations.
*   **Scheduled Jobs**: View, manage, and delete jobs that have been scheduled to run in the future via APScheduler.
*   **Recent Runs**: A list of the most recent operations, their status, and links to detailed logs.

#### Allocations

*   View all active lab allocations grouped by tag.
*   Filter allocations by tag, vendor, course, host, or pod/class number.
*   **In-line Editing**: Edit summary details like dates, trainer name, and APM credentials directly from the UI.
*   **Interactive Controls**:
    *   Toggle the power state of an entire tag group or individual pods/classes.
    *   Teardown an entire tag group or individual items (VMs + DB entry).
    *   Delete just the database entry for an item.
    *   Toggle the "Extend" flag to prevent a tag group from being included in bulk teardown operations.
    *   Run a `test` command against an entire tag group.

#### Upcoming Courses & Build Planner

This is a powerful multi-step workflow for planning and executing new builds based on external data (e.g., Salesforce).

1.  **Upcoming Courses Page**:
    *   Displays a list of courses scheduled for the upcoming week, fetched from Salesforce.
    *   Build Rules (configured in Settings) are automatically applied to pre-select the appropriate `LabBuild Course`, `Host`, and number of `Pods Req.`.
    *   Users can override these pre-selections and choose whether to create a trainer pod for each course.
2.  **Intermediate Review Page**:
    *   After selecting courses, users are taken to a review page where the system proposes assignments (host and pod numbers) based on available resources.
    *   For complex builds like Checkpoint Maestro, the system automatically splits the course into multiple build items.
    *   Users can review, modify, or manually enter all assignments.
3.  **Trainer Pod Review Page**:
    *   After confirming student pod assignments, the system proposes consolidated trainer pod builds.
    *   Users can review and modify these trainer pod assignments.
4.  **Final Review & Schedule Page**:
    *   Displays a final, consolidated plan of all student and trainer builds.
    *   Generates and displays all necessary APM (course2) commands for user creation and modification.
    *   Generates and displays HTML email previews for trainers with their allocation details.
    *   Provides options to schedule the entire build plan to run immediately, at a specific time, or in a staggered sequence.

#### Terminal

*   A pseudo-terminal interface that allows you to execute `labbuild` commands directly and see the output streamed in real-time.

#### Logs

*   **All Logs**: A filterable and paginated view of all historical operation logs.
*   **Log Detail**: A detailed view for a single run, showing the summary, arguments, status of each pod/class, and a real-time stream of the detailed log output.

#### Settings

*   **Build Rules**: Create, edit, and manage the priority-based rules that govern the auto-selection logic on the "Upcoming Courses" page.
*   **Course Configs**: A UI to view and manage the raw JSON configurations for each LabBuild course stored in the database.
*   **Trainer Emails**: Manage the mapping of trainer names to email addresses used for notifications.