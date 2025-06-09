// dashboard/static/js/upcoming_courses.js

document.addEventListener('DOMContentLoaded', function () {
    const table = document.getElementById('upcoming-courses-table');
    const tbody = document.getElementById('upcoming-courses-tbody');
    const headers = table ? table.querySelectorAll('th.sortable-header') : [];
    const selectAllCheckbox = document.getElementById('selectAllCourses');
    const bulkBuildButton = document.getElementById('bulkBuildBtn');
    const coursesDataScript = document.getElementById('courses-data'); // From Python, includes preselects
    const courseConfigsDataScript = document.getElementById('course-configs-data'); // For dropdown
    const loadingIndicator = document.getElementById('buildLoadingIndicator');
    const loadingText = document.getElementById('buildLoadingText');

    let courseData = []; // Holds the full data passed from Python (Salesforce + rule preselects)
    let courseConfigsList = []; // Holds LabBuild course configurations for dropdowns
    let sortState = { column: 'Start Date', direction: 'asc' }; // Default sort

    function parseInitialData() {
        try {
            if (coursesDataScript?.textContent) {
                courseData = JSON.parse(coursesDataScript.textContent);
                console.log("upcoming_courses.js: Parsed courseData:", courseData); // Log parsed data
            } else {
                console.warn("upcoming_courses.js: Courses data script missing or empty.");
                courseData = [];
            }
            if (courseConfigsDataScript?.textContent) {
                courseConfigsList = JSON.parse(courseConfigsDataScript.textContent);
            } else {
                console.warn("upcoming_courses.js: Course configs data script missing or empty.");
                courseConfigsList = [];
            }
            return true;
        } catch (e) {
            console.error("upcoming_courses.js: Error parsing embedded JSON data:", e);
            if (tbody) tbody.innerHTML = '<tr><td colspan="9" class="text-danger">Error loading page data.</td></tr>';
        }
        return false;
    }

    if (!table || !tbody || !headers.length || !selectAllCheckbox || !bulkBuildButton) {
        console.error("upcoming_courses.js: Required UI elements missing.");
        if (tbody) tbody.innerHTML = '<tr><td colspan="9" class="text-danger">UI Error: Table structure incomplete.</td></tr>';
        return;
    }

    function renderTable(dataToRender) {
        tbody.innerHTML = ''; // Clear existing rows
        const colCount = 9; // Checkbox + Start, End, Code, Type, Trainer, Pax, Pods Req, LB Course Dropdown
        if (!dataToRender || dataToRender.length === 0) {
            tbody.innerHTML = `<tr><td colspan="${colCount}">No upcoming courses found.</td></tr>`;
            return;
        }

        dataToRender.forEach((courseItem, index) => { // courseItem is one entry from courseData
            const row = tbody.insertRow();

            // --- Extract data from courseItem (which includes SF data and preselects) ---
            const sfCourseCode = courseItem['Course Code'] || '';
            const derivedVendor = courseItem['vendor'] || (sfCourseCode ? sfCourseCode.substring(0, 2).toLowerCase() : '');
            const preselectedLabbuildCourse = courseItem.preselect_labbuild_course || null;

            // --- Store all necessary data on the row's dataset for later collection ---
            // This index allows us to retrieve the *full original courseItem* later, including all preselects
            const originalDataIndex = courseData.findIndex(cd =>
                cd['Course Code'] === sfCourseCode &&
                cd['Start Date'] === courseItem['Start Date'] && // Add more keys if uniqueness isn't guaranteed
                cd['Trainer'] === courseItem['Trainer']
            );
            row.dataset.originalDataIndex = originalDataIndex !== -1 ? originalDataIndex : index; // Fallback to current index if not found (should be found)

            row.dataset.sfCourseCode = sfCourseCode;
            row.dataset.derivedVendor = derivedVendor;
            // Store the original SF fields passed from Python (which were processed by salesforce_utils)
            row.dataset.sfCourseType = courseItem['Course Type'] || 'N/A';
            row.dataset.sfStartDate = courseItem['Start Date'] || 'N/A';
            row.dataset.sfEndDate = courseItem['End Date'] || 'N/A';
            row.dataset.sfTrainerName = courseItem['Trainer'] || 'N/A';
            row.dataset.sfPaxCount = String(courseItem['Pax'] ?? '0'); // Store as string, parse later
            row.dataset.sfPodsReq = String(courseItem['Pods Req.'] ?? '1'); // Store original string (e.g., "4 (Split)")

            // 1. Checkbox Cell
            const checkboxCell = row.insertCell(0);
            checkboxCell.className = 'col-checkbox';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input course-checkbox';
            checkboxCell.appendChild(checkbox);

            // 2. Standard Info Cells
            row.insertCell().textContent = courseItem['Start Date'] || 'N/A';
            row.insertCell().textContent = courseItem['End Date'] || 'N/A';
            row.insertCell().textContent = sfCourseCode;
            row.insertCell().textContent = courseItem['Course Type'] || 'N/A';
            row.insertCell().textContent = courseItem['Trainer'] || 'N/A';
            const paxCell = row.insertCell(); paxCell.textContent = courseItem['Pax'] ?? 'N/A'; paxCell.classList.add('col-numbers');
            const podsReqCell = row.insertCell(); podsReqCell.textContent = courseItem['Pods Req.'] ?? 'N/A'; podsReqCell.classList.add('col-numbers');

            // 3. LabBuild Course Dropdown Cell
            const courseSelectCell = row.insertCell();
            const courseSelect = document.createElement('select');
            courseSelect.className = 'form-select form-select-sm labbuild-course-select';
            courseSelect.innerHTML = '<option value="">Select LabBuild Course...</option>';

            let optionsFound = 0;
            let preselectOptionFoundInDropdown = false;
            courseConfigsList.forEach(config => {
                if (config.vendor_shortcode?.toLowerCase() === derivedVendor) {
                    const option = document.createElement('option');
                    option.value = config.course_name;
                    option.textContent = config.course_name;
                    if (preselectedLabbuildCourse && config.course_name === preselectedLabbuildCourse) {
                        option.selected = true;
                        preselectOptionFoundInDropdown = true;
                    }
                    courseSelect.appendChild(option);
                    optionsFound++;
                }
            });

            if (!preselectedLabbuildCourse && optionsFound === 1 && courseSelect.options[1]) {
                courseSelect.value = courseSelect.options[1].value; // Auto-select if only one option
            }

            if (optionsFound === 0) {
                courseSelect.disabled = true;
                const noOpt = document.createElement('option');
                noOpt.textContent = 'No courses for vendor'; noOpt.disabled = true;
                courseSelect.insertBefore(noOpt, courseSelect.firstChild);
                courseSelect.value = "";
            } else if (preselectedLabbuildCourse && !preselectOptionFoundInDropdown) {
                const invalidPreselectOpt = document.createElement('option');
                invalidPreselectOpt.value = ""; // Make it unselectable
                invalidPreselectOpt.textContent = `Rule Error: '${preselectedLabbuildCourse}' invalid`;
                invalidPreselectOpt.disabled = true;
                invalidPreselectOpt.selected = true; // Show this error state
                courseSelect.insertBefore(invalidPreselectOpt, courseSelect.firstChild);
                // courseSelect.value = ""; // Dropdown shows error
            }
            courseSelectCell.appendChild(courseSelect);

            // Set initial checkbox state based on dropdown AFTER population
            checkbox.disabled = !courseSelect.value;
            checkbox.title = courseSelect.value ? "Select this course for build" : "Cannot select: Please choose a LabBuild Course.";
            if (!courseSelect.value) {
                row.classList.add('table-warning');
            }

            courseSelect.addEventListener('change', function () {
                checkbox.disabled = !this.value;
                checkbox.title = this.value ? "Select this course for build" : "Cannot select: Please choose a LabBuild Course.";
                if (!this.value) {
                    checkbox.checked = false; row.classList.add('table-warning');
                } else {
                    row.classList.remove('table-warning');
                }
                updateSelectAllState();
            });
        }); // End forEach dataToRender
        addCheckboxListeners();
        updateSelectAllState();
    }

    function addCheckboxListeners() {
        tbody.querySelectorAll('.course-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', updateSelectAllState);
        });
    }

    function updateSelectAllState() {
        if (!selectAllCheckbox) return;
        const enabledCheckboxes = tbody.querySelectorAll('.course-checkbox:not(:disabled)');
        const checkedEnabledCheckboxes = tbody.querySelectorAll('.course-checkbox:checked:not(:disabled)');
        selectAllCheckbox.checked = enabledCheckboxes.length > 0 && (enabledCheckboxes.length === checkedEnabledCheckboxes.length);
    }

    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', function () {
            tbody.querySelectorAll('.course-checkbox:not(:disabled)').forEach(checkbox => {
                checkbox.checked = selectAllCheckbox.checked;
            });
        });
    }

    if (bulkBuildButton) {
        bulkBuildButton.addEventListener('click', function() {
            const selectedRowsData = [];
            const selectedCheckboxes = tbody.querySelectorAll('.course-checkbox:checked:not(:disabled)');

            if (selectedCheckboxes.length === 0) {
                alert('Please select at least one course with a LabBuild Course chosen.');
                return;
            }

            bulkBuildButton.disabled = true;
            if (loadingIndicator) loadingIndicator.style.display = 'inline-block';
            if (loadingText) loadingText.style.display = 'inline';

            selectedCheckboxes.forEach(checkbox => {
                const row = checkbox.closest('tr');
                const courseSelect = row.querySelector('.labbuild-course-select');
                const selectedLabCourse = courseSelect?.value;

                if (row && selectedLabCourse) {
                    // Get the full original course item using the stored index
                    const originalIdx = parseInt(row.dataset.originalDataIndex, 10);
                    const originalCourseItem = (originalIdx >= 0 && originalIdx < courseData.length) ? courseData[originalIdx] : {};

                    selectedRowsData.push({
                         sf_course_code: row.dataset.sfCourseCode,
                         labbuild_course: selectedLabCourse, // Current dropdown value
                         vendor: row.dataset.derivedVendor,
                         // Pass original SF data from row dataset (which came from Python)
                         sf_course_type: row.dataset.sfCourseType,
                         sf_start_date: row.dataset.sfStartDate,
                         sf_end_date: row.dataset.sfEndDate,
                         sf_trainer_name: row.dataset.sfTrainerName,
                         sf_pax_count: parseInt(row.dataset.sfPaxCount, 10) || 0,
                         sf_pods_req: row.dataset.sfPodsReq,
                         // Pass through preselects from the *original full course item*
                         // These were applied by salesforce_utils.py
                         preselect_host: originalCourseItem.preselect_host || null,
                         preselect_host_priority_list: originalCourseItem.preselect_host_priority_list || [],
                         preselect_allow_spillover: originalCourseItem.preselect_allow_spillover !== undefined ? originalCourseItem.preselect_allow_spillover : true,
                         preselect_start_pod: originalCourseItem.preselect_start_pod || 1,
                         preselect_max_pods_applied_constraint: originalCourseItem.preselect_max_pods_applied_constraint || null,
                         maestro_split_config_details: originalCourseItem.maestro_split_config_details || null,
                         preselect_note: originalCourseItem.preselect_note || null
                     });
                } else {
                     console.warn("Skipping selected row - missing data attribute or LabBuild Course selection:", row.dataset.sfCourseCode);
                }
            });

            if (selectedRowsData.length === 0) {
                 alert('None of the selected rows had complete build information.');
                 bulkBuildButton.disabled = false;
                 if (loadingIndicator) loadingIndicator.style.display = 'none';
                 if (loadingText) loadingText.style.display = 'none';
                 return;
            }

            console.log("Data collected for review to be sent to /intermediate-build-review:", selectedRowsData);

            const form = document.createElement('form');
            form.method = 'POST';
            form.action = '/intermediate-build-review'; // Your target route
            const input = document.createElement('input');
            input.type = 'hidden';
            input.name = 'selected_courses';
            input.value = JSON.stringify(selectedRowsData);
            form.appendChild(input);
            document.body.appendChild(form);
            form.submit();
            // Loading indicator will be hidden by page navigation
        });
    }


    function updateSortIndicators() { /* ... (same as before) ... */ }
    function sortTable(columnName, dataType) { /* ... (same as before, ensure it uses `courseData` and calls `renderTable(courseData)`) ... */
        if (!columnName || !courseData) return;
        let direction = (sortState.column === columnName && sortState.direction === 'asc') ? 'desc' : 'asc';
        sortState.column = columnName; sortState.direction = direction;

        courseData.sort((a, b) => {
            let valA = a[columnName]; let valB = b[columnName];
            if (dataType === 'date') {
                // Ensure correct parsing, especially if dates can be DD/MM/YYYY from SF
                // and YYYY-MM-DD after Python processing
                const parseDateSafely = (dateStr) => {
                    if (!dateStr || dateStr === "N/A") return new Date(0); // Epoch start for invalid/N/A
                    // Try YYYY-MM-DD first (likely after Python processing)
                    let d = new Date(dateStr + 'T00:00:00Z'); // Assume UTC if parsed from YYYY-MM-DD
                    if (!isNaN(d.getTime())) return d;
                    // Try DD/MM/YYYY (common SF format)
                    const parts = String(dateStr).split('/');
                    if (parts.length === 3) {
                        d = new Date(parts[2] + '-' + parts[1] + '-' + parts[0] + 'T00:00:00Z');
                        if (!isNaN(d.getTime())) return d;
                    }
                    return new Date(0); // Fallback if all parsing fails
                };
                valA = parseDateSafely(valA);
                valB = parseDateSafely(valB);
            } else if (dataType === 'number') {
                valA = parseFloat(String(valA).match(/(\d+)/)?.[0] || 0);
                valB = parseFloat(String(valB).match(/(\d+)/)?.[0] || 0);
            } else { // string
                valA = String(valA || '').toLowerCase();
                valB = String(valB || '').toLowerCase();
            }
            if (valA < valB) return direction === 'asc' ? -1 : 1;
            if (valA > valB) return direction === 'asc' ? 1 : -1;
            return 0;
        });
        renderTable(courseData); // Re-render with sorted global courseData
        updateSortIndicators();
    }

    headers.forEach(header => {
        header.addEventListener('click', () => {
            sortTable(header.dataset.column, header.dataset.type);
        });
    });

    // --- Initial Load ---
    if (parseInitialData()) {
        sortTable('Start Date', 'date'); // Initial sort and render
    } else {
        if (tbody) tbody.innerHTML = '<tr><td colspan="9">Failed to load course data for table.</td></tr>';
    }
});