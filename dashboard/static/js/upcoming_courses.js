document.addEventListener('DOMContentLoaded', function () {
    // --- DOM Element Selection ---
    const table = document.getElementById('upcoming-courses-table');
    const tbody = document.getElementById('upcoming-courses-tbody');
    const headers = table ? table.querySelectorAll('th.sortable-header') : [];
    const selectAllCheckbox = document.getElementById('selectAllCourses');
    const bulkBuildButton = document.getElementById('bulkBuildBtn');
    const coursesDataScript = document.getElementById('courses-data');
    const courseConfigsDataScript = document.getElementById('course-configs-data');
    const loadingIndicator = document.getElementById('buildLoadingIndicator'); // Get indicator
    const loadingText = document.getElementById('buildLoadingText');

    // --- Data Initialization ---
    let courseData = [];
    let courseConfigsList = [];
    let sortState = { column: 'Start Date', direction: 'asc' };

    // --- Helper: Parse Embedded Data ---
    function parseInitialData() {
        try {
            if (coursesDataScript?.textContent) {
                courseData = JSON.parse(coursesDataScript.textContent); // Includes preselects
            } else { console.warn("Courses data script missing."); courseData = []; }

            if (courseConfigsDataScript?.textContent) {
                courseConfigsList = JSON.parse(courseConfigsDataScript.textContent);
            } else { console.error("Course configs data script missing."); courseConfigsList = []; }
            return true;
        } catch (e) {
            console.error("Error parsing embedded JSON data:", e);
            if (tbody) tbody.innerHTML = '<tr><td colspan="9" class="text-danger">Error loading page data.</td></tr>';
        }
        return false;
    }

    // --- Check if essential elements exist ---
    if (!table || !tbody || !headers.length || !selectAllCheckbox || !bulkBuildButton) {
        console.error("Required UI elements missing.");
        if (tbody) tbody.innerHTML = '<tr><td colspan="9" class="text-danger">UI Error.</td></tr>';
        return;
    }

    // --- Render Table Function ---
    function renderTable(data) {
        tbody.innerHTML = '';
        const colCount = 9; // Checkbox + 7 info + 1 dropdown
        if (!data || data.length === 0) { tbody.innerHTML = `<tr><td colspan="${colCount}">No courses found.</td></tr>`; return; }

        data.forEach((course, index) => {
            const row = tbody.insertRow();
            const sfCourseCode = course['Course Code'] || '';
            const derivedVendor = sfCourseCode.substring(0, 2).toLowerCase();
            const preselectedCourse = course.preselect_labbuild_course || null;

            // Store necessary data on row for bulk action
            row.dataset.rowIndex = index;
            row.dataset.sfCourseCode = sfCourseCode;
            row.dataset.derivedVendor = derivedVendor;
            // We store the *originally* preselected course in case needed later,
            // but the bulk build will use the dropdown's current value.
            row.dataset.preselectedCourse = preselectedCourse || '';

            // 1. Standard Info Cells (7 columns) - RENDER THESE FIRST
            row.insertCell().textContent = course['Start Date'] || 'N/A';
            row.insertCell().textContent = course['End Date'] || 'N/A';
            row.insertCell().textContent = sfCourseCode;
            row.insertCell().textContent = course['Course Type'] || 'N/A';
            row.insertCell().textContent = course['Trainer'] || 'N/A';
            const paxCell = row.insertCell(); paxCell.textContent = course['Pax'] ?? 'N/A'; paxCell.classList.add('col-numbers');
            const podsReqCell = row.insertCell(); podsReqCell.textContent = course['Pods Req.'] ?? 'N/A'; podsReqCell.classList.add('col-numbers');

            // 2. Render LabBuild Course Dropdown Cell - RENDER THIS NEXT
            const courseSelectCell = row.insertCell();
            const courseSelect = document.createElement('select');
            courseSelect.className = 'form-select form-select-sm labbuild-course-select';
            courseSelect.innerHTML = '<option value="">Select LabBuild Course...</option>'; // Placeholder

            let optionsFound = 0;
            let preselectOptionFound = false;
            courseConfigsList.forEach(config => {
                if (config.vendor_shortcode?.toLowerCase() === derivedVendor) {
                    const option = document.createElement('option');
                    option.value = config.course_name; option.textContent = config.course_name;
                    if (preselectedCourse && config.course_name === preselectedCourse) {
                        option.selected = true; preselectOptionFound = true;
                    }
                    courseSelect.appendChild(option); optionsFound++;
                }
            });
            // Auto-select single option if nothing was preselected
            if (!preselectedCourse && optionsFound === 1 && courseSelect.options[1]) {
                courseSelect.value = courseSelect.options[1].value;
            }
            // Handle no options or missing preselect
            if (optionsFound === 0) { courseSelect.disabled = true; const noOpt = document.createElement('option'); noOpt.textContent = 'No courses found'; noOpt.disabled = true; courseSelect.insertBefore(noOpt, courseSelect.firstChild); courseSelect.value = ""; }
            else if (preselectedCourse && !preselectOptionFound) { console.warn(`Preselected course "${preselectedCourse}" not found.`); courseSelect.value = ""; } // Reset if preselect invalid
            courseSelectCell.appendChild(courseSelect);

            // 3. Checkbox Cell (Insert at beginning, disable based on initial dropdown value) - RENDER THIS LAST
            const checkboxCell = row.insertCell(0); // Index 0
            checkboxCell.className = 'col-checkbox';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input course-checkbox';
            // *** Set initial disabled state based on the dropdown's value AFTER population/preselection ***
            checkbox.disabled = !courseSelect.value;
            checkbox.title = courseSelect.value ? "Select this course for build" : "Cannot select: Please choose a LabBuild Course.";
            if (!courseSelect.value) {
                row.classList.add('table-warning');
            }
            checkboxCell.appendChild(checkbox);

            // --- ADD DROPDOWN LISTENER ---
            // Add listener to THIS dropdown to update THIS row's checkbox state
            courseSelect.addEventListener('change', function () {
                checkbox.disabled = !this.value; // Disable checkbox if dropdown value is empty
                checkbox.title = this.value ? "Select this course for build" : "Cannot select: Please choose a LabBuild Course.";
                if (!this.value) {
                    checkbox.checked = false; // Uncheck if disabled
                    row.classList.add('table-warning');
                } else {
                    row.classList.remove('table-warning');
                }
                // Update the main "Select All" checkbox state whenever a row checkbox
                // might change its enabled state or checked state.
                updateSelectAllState();
            });
            // --- END DROPDOWN LISTENER ---

        }); // End forEach row

        addCheckboxListeners(); // Add listeners for row checkboxes themselves
        updateSelectAllState(); // Set initial state of selectAll checkbox
    } // --- End of renderTable ---

    // --- Checkbox Interaction Logic ---
    function addCheckboxListeners() {
        tbody.querySelectorAll('.course-checkbox').forEach(checkbox => {
            // When a row checkbox changes, just update the selectAll state
            checkbox.addEventListener('change', updateSelectAllState);
        });
    }
    function updateSelectAllState() {
        const enabledCheckboxes = tbody.querySelectorAll('.course-checkbox:not(:disabled)');
        const checkedEnabledCheckboxes = tbody.querySelectorAll('.course-checkbox:checked:not(:disabled)');
        // Only check selectAll if there are enabled checkboxes AND all of them are checked
        selectAllCheckbox.checked = enabledCheckboxes.length > 0 && (enabledCheckboxes.length === checkedEnabledCheckboxes.length);
    }
    selectAllCheckbox.addEventListener('change', function () {
        // When selectAll changes, update all ENABLED row checkboxes
        tbody.querySelectorAll('.course-checkbox:not(:disabled)').forEach(checkbox => {
            checkbox.checked = selectAllCheckbox.checked;
        });
    });

    // --- Bulk Build Button Logic (Collects current dropdown value) ---
    bulkBuildButton.addEventListener('click', function() {
        const selectedRowsData = [];
        const selectedCheckboxes = tbody.querySelectorAll('.course-checkbox:checked:not(:disabled)');

        if (selectedCheckboxes.length === 0) {
            alert('Please select at least one course with a LabBuild Course chosen.');
            return; // Don't show loading indicator if nothing is selected
        }

        // --- Show Loading Indicator ---
        bulkBuildButton.disabled = true; // Disable button
        if (loadingIndicator) loadingIndicator.style.display = 'inline-block';
        if (loadingText) loadingText.style.display = 'inline';

        selectedCheckboxes.forEach(checkbox => {
            const row = checkbox.closest('tr');
            const courseSelect = row.querySelector('.labbuild-course-select');
            const selectedLabCourse = courseSelect?.value;

            // Check if all required data is present FOR THIS ROW
            // Note: We rely on backend pre-selects being stored correctly now
            if (row && selectedLabCourse) {
                 selectedRowsData.push({
                     sf_course_code: row.dataset.sfCourseCode,
                     labbuild_course: selectedLabCourse,
                     vendor: row.dataset.derivedVendor,
                     // Also include original SF data for saving in interim
                     sf_course_type: courseData[parseInt(row.dataset.rowIndex, 10)]['Course Type'] || '',
                     sf_start_date: courseData[parseInt(row.dataset.rowIndex, 10)]['Start Date'] || '',
                     sf_end_date: courseData[parseInt(row.dataset.rowIndex, 10)]['End Date'] || '',
                     sf_trainer: courseData[parseInt(row.dataset.rowIndex, 10)]['Trainer'] || '',
                     sf_pax: courseData[parseInt(row.dataset.rowIndex, 10)]['Pax'] ?? 'N/A',
                     sf_pods_req: courseData[parseInt(row.dataset.rowIndex, 10)]['Pods Req.'] ?? 'N/A'
                 });
            } else {
                 console.warn("Skipping selected row - missing required data attribute or LabBuild Course selection:", row);
            }
        });

        if (selectedRowsData.length === 0) {
             alert('None of the selected rows had complete build information.');
             // --- Hide Loading Indicator on client-side error ---
             bulkBuildButton.disabled = false;
             if (loadingIndicator) loadingIndicator.style.display = 'none';
             if (loadingText) loadingText.style.display = 'none';
             return;
        }

        console.log("Data collected for review:", selectedRowsData);

        // --- Create a hidden form and submit data via POST ---
        const form = document.createElement('form');
        form.method = 'POST';
        form.action = '/intermediate-build-review'; // URL for the review route

        const input = document.createElement('input');
        input.type = 'hidden';
        input.name = 'selected_courses';
        input.value = JSON.stringify(selectedRowsData);
        form.appendChild(input);

        // Add CSRF token if needed (example commented out)
        // ...

        document.body.appendChild(form);
        // The browser navigation on submit will automatically "hide" the indicator
        // as the new page loads. No explicit hiding needed here for success.
        form.submit();

        // We don't re-enable the button here, as the page will navigate away.
        // If the submission *fails* and the user stays on the page (e.g., network error),
        // we might need error handling in a `catch` block if using fetch,
        // but form submission just navigates.

    }); // End bulkBuildButton listener


    // --- Keep Sorting Logic ---
    // --- Sort Indicators Function ---
    function updateSortIndicators() {
        headers.forEach(header => {
            const icon = header.querySelector('.sort-icon');
            if (!icon) return; // Skip if icon element isn't found within the header

            // Reset classes and ARIA attribute first
            header.classList.remove('sort-asc', 'sort-desc');
            header.removeAttribute('aria-sort');
            // Reset icon to the base 'fa-sort' state
            icon.className = 'sort-icon ms-1 fas fa-sort'; // Ensure Font Awesome classes are correct

            // Apply active sort state if this header matches the current sort column
            if (header.dataset.column === sortState.column) {
                if (sortState.direction === 'asc') {
                    header.classList.add('sort-asc');
                    // Replace the neutral sort icon class with the ascending one
                    icon.classList.replace('fa-sort', 'fa-sort-up');
                    header.setAttribute('aria-sort', 'ascending');
                } else if (sortState.direction === 'desc') {
                    header.classList.add('sort-desc');
                    // Replace the neutral sort icon class with the descending one
                    icon.classList.replace('fa-sort', 'fa-sort-down');
                    header.setAttribute('aria-sort', 'descending');
                }
                // If direction is somehow invalid, it remains neutral (fa-sort)
            }
            // No 'else' needed because non-active columns should already have the neutral 'fa-sort' icon
        });
    }
    function sortTable(columnName, dataType) {
        if (!columnName || !courseData) return;
        let direction = (sortState.column === columnName && sortState.direction === 'asc') ? 'desc' : 'asc';
        sortState.column = columnName; sortState.direction = direction;
        courseData.sort((a, b) => { /* ... sorting logic ... */ }); // Unchanged
        renderTable(courseData); updateSortIndicators();
    }
    headers.forEach(header => { header.addEventListener('click', () => { /* ... */ }); }); // Unchanged

    // --- Initial Load ---
    if (parseInitialData()) {
        sortTable(sortState.column, 'date'); // Sort and render initially
    } else {
        if (tbody) tbody.innerHTML = '<tr><td colspan="9">Failed to load course data.</td></tr>'; // Correct colspan
    }
});