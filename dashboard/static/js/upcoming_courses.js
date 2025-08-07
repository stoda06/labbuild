// In dashboard/static/js/upcoming_courses.js

document.addEventListener('DOMContentLoaded', function () {
    // --- Element References ---
    const table = document.getElementById('upcoming-courses-table');
    const tbody = document.getElementById('upcoming-courses-tbody');
    const headers = table ? table.querySelectorAll('th.sortable-header') : [];
    const selectAllCheckbox = document.getElementById('selectAllCourses');
    const reviewButton = document.getElementById('reviewSelectedBtn');
    const form = document.getElementById('courseSelectionForm');

    // --- Data Stores ---
    const coursesDataScript = document.getElementById('courses-data-json');
    const courseConfigsDataScript = document.getElementById('course-configs-data-json');
    let allCoursesData = [];
    let courseConfigsList = [];
    let sortState = { column: 'Start Date', direction: 'asc' };

    /**
     * Parses the initial data embedded in the HTML.
     * @returns {boolean} - True if parsing was successful, false otherwise.
     */
    function parseInitialData() {
        try {
            if (coursesDataScript?.textContent) {
                allCoursesData = JSON.parse(coursesDataScript.textContent);
            }
            if (courseConfigsDataScript?.textContent) {
                courseConfigsList = JSON.parse(courseConfigsDataScript.textContent);
            }
            return true;
        } catch (e) {
            console.error("Error parsing embedded JSON data:", e);
            if (tbody) tbody.innerHTML = '<tr><td colspan="8" class="text-danger text-center p-3">Error loading page data.</td></tr>';
            return false;
        }
    }

    /**
     * Renders the table rows based on the provided data array.
     * @param {Array<Object>} dataToRender - The array of course objects to display.
     */
    function renderTable(dataToRender) {
        tbody.innerHTML = '';
        if (!dataToRender || dataToRender.length === 0) {
            tbody.innerHTML = `<tr><td colspan="8" class="text-center p-3">No upcoming courses found.</td></tr>`;
            return;
        }

        dataToRender.forEach((courseItem) => {
            const row = tbody.insertRow();
            // Store the full, original data object on the row for later submission.
            row.dataset.originalCourseItem = JSON.stringify(courseItem);
            
            const sfCourseCode = courseItem['Course Code'] || '';
            const derivedVendor = courseItem['vendor'] || (sfCourseCode ? sfCourseCode.substring(0, 2).toLowerCase() : '');
            const preselectedLabbuildCourse = courseItem.preselect_labbuild_course || null;

            // --- Cell 0: Selection Checkbox ---
            const checkboxCell = row.insertCell();
            checkboxCell.className = 'col-checkbox';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input course-checkbox';
            checkboxCell.appendChild(checkbox);

            // --- Data Cells (Cells 1-5) ---
            row.insertCell().textContent = courseItem['Start Date'] || 'N/A';
            row.insertCell().textContent = sfCourseCode;
            row.insertCell().textContent = courseItem['Course Type'] || 'N/A';
            row.insertCell().textContent = courseItem['Trainer'] || 'N/A';
            const podsReqCell = row.insertCell();
            podsReqCell.textContent = courseItem['Pods Req.'] ?? 'N/A';
            podsReqCell.className = 'text-center';

            // --- Cell 6: "Add Trainer Pod?" Checkbox ---
            const trainerToggleCell = row.insertCell();
            trainerToggleCell.className = 'col-trainer-toggle';
            const trainerCheckbox = document.createElement('input');
            trainerCheckbox.type = 'checkbox';
            trainerCheckbox.className = 'form-check-input trainer-pod-checkbox';
            trainerCheckbox.checked = true; // Default to checked

            // Apply special rules: uncheck by default for Maestro, Nutanix, and Avaya
            if (['nu', 'av'].includes(derivedVendor) || (courseItem['Course Type'] || '').toLowerCase().includes('maestro')) {
                trainerCheckbox.checked = false;
            }
            trainerToggleCell.appendChild(trainerCheckbox);

            // --- Cell 7: LabBuild Course Dropdown ---
            const courseSelectCell = row.insertCell();
            const courseSelect = document.createElement('select');
            courseSelect.className = 'form-select form-select-sm labbuild-course-select';
            courseSelect.innerHTML = '<option value="">Select LabBuild Course...</option>';

            const optionsFound = courseConfigsList.filter(config => config.vendor_shortcode?.toLowerCase() === derivedVendor)
                .map(config => {
                    const option = document.createElement('option');
                    option.value = config.course_name;
                    option.textContent = config.course_name;
                    if (preselectedLabbuildCourse && config.course_name === preselectedLabbuildCourse) {
                        option.selected = true;
                    }
                    courseSelect.appendChild(option);
                    return true;
                }).length > 0;

            if (!optionsFound) {
                courseSelect.disabled = true;
            }

            courseSelectCell.appendChild(courseSelect);

            // Link the main selection checkbox to the dropdown's state
            checkbox.disabled = !courseSelect.value;
            row.classList.toggle('table-warning', !courseSelect.value);

            courseSelect.addEventListener('change', function () {
                checkbox.disabled = !this.value;
                if (!this.value) checkbox.checked = false; // Uncheck if dropdown is cleared
                row.classList.toggle('table-warning', !this.value);
                updateSelectAllState();
            });
        });
        
        // Re-attach listeners to the new checkboxes
        addCheckboxListeners();
        updateSelectAllState();
    }
    
    /**
     * Attaches event listeners to all row checkboxes.
     */
    function addCheckboxListeners() {
        tbody.querySelectorAll('.course-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', updateSelectAllState);
        });
    }

    /**
     * Updates the state of the "Select All" checkbox based on row checkboxes.
     */
    function updateSelectAllState() {
        if (!selectAllCheckbox) return;
        const enabledCheckboxes = Array.from(tbody.querySelectorAll('.course-checkbox:not(:disabled)'));
        const checkedEnabled = enabledCheckboxes.filter(cb => cb.checked);
        selectAllCheckbox.checked = enabledCheckboxes.length > 0 && enabledCheckboxes.length === checkedEnabled.length;
        selectAllCheckbox.indeterminate = checkedEnabled.length > 0 && checkedEnabled.length < enabledCheckboxes.length;
    }

    // --- Main Event Listeners ---

    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', function () {
            tbody.querySelectorAll('.course-checkbox:not(:disabled)').forEach(checkbox => {
                checkbox.checked = this.checked;
            });
        });
    }

    if (form) {
        form.addEventListener('submit', function(event) {
            event.preventDefault(); // We handle submission manually
            
            const selectedCourses = [];
            const rows = tbody.querySelectorAll('tr');

            rows.forEach((row) => {
                const checkbox = row.querySelector('.course-checkbox');
                if (checkbox && checkbox.checked) {
                    const labbuildCourseSelect = row.querySelector('.labbuild-course-select');
                    const trainerCheckbox = row.querySelector('.trainer-pod-checkbox');
                    
                    // Retrieve the full original data object stored on the row
                    const originalCourseData = JSON.parse(row.dataset.originalCourseItem);
                    
                    // Add the user's current selections to the object
                    originalCourseData.user_selected_labbuild_course = labbuildCourseSelect.value;
                    originalCourseData.create_trainer_pod = trainerCheckbox.checked;
                    
                    selectedCourses.push(originalCourseData);
                }
            });

            if (selectedCourses.length === 0) {
                alert("Please select at least one course to review.");
                return;
            }

            // Create a hidden input to hold the JSON data
            const hiddenInput = document.createElement('input');
            hiddenInput.type = 'hidden';
            hiddenInput.name = 'selected_courses'; // This name MUST match the backend route
            hiddenInput.value = JSON.stringify(selectedCourses);
            
            this.appendChild(hiddenInput);
            this.submit();
        });
    }

    // --- Sorting Logic (Unchanged) ---
    function sortTable(columnName, dataType) {
        let direction = (sortState.column === columnName && sortState.direction === 'asc') ? 'desc' : 'asc';
        sortState = { column: columnName, direction: direction };
        allCoursesData.sort((a, b) => {
            let valA = a[columnName]; let valB = b[columnName];
            if (dataType === 'date') {
                valA = new Date(valA || 0); valB = new Date(valB || 0);
            } else if (dataType === 'number') {
                valA = parseFloat(String(valA).match(/(\d+)/)?.[0] || 0);
                valB = parseFloat(String(valB).match(/(\d+)/)?.[0] || 0);
            } else {
                valA = String(valA || '').toLowerCase(); valB = String(valB || '').toLowerCase();
            }
            if (valA < valB) return direction === 'asc' ? -1 : 1;
            if (valA > valB) return direction === 'asc' ? 1 : -1;
            return 0;
        });
        renderTable(allCoursesData);
        updateSortIndicators();
    }
    
    function updateSortIndicators() {
        headers.forEach(header => {
            header.classList.remove('sort-asc', 'sort-desc');
            const icon = header.querySelector('.sort-icon');
            if(icon) icon.innerHTML = '';
            if (header.dataset.column === sortState.column) {
                header.classList.add(sortState.direction === 'asc' ? 'sort-asc' : 'sort-desc');
                if(icon) icon.innerHTML = sortState.direction === 'asc' ? '<i class="fas fa-sort-up"></i>' : '<i class="fas fa-sort-down"></i>';
            }
        });
    }

    headers.forEach(header => {
        header.addEventListener('click', () => sortTable(header.dataset.column, header.dataset.type));
    });

    // --- Initial Load ---
    if (parseInitialData()) {
        sortTable('Start Date', 'date'); // Sort and render the initial data
    }
});