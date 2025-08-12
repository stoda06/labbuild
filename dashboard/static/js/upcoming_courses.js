// In dashboard/static/js/upcoming_courses.js

/**
 * Manages the interactivity of the "Upcoming Courses" page.
 * This class handles data parsing, table rendering, sorting, user selections,
 * and form submission for the build planning process.
 */
class UpcomingCoursesPlanner {
    /**
     * Initializes the planner by finding all necessary DOM elements and
     * parsing the initial data embedded in the page.
     */
    constructor() {
        // --- 1. DOM Element References ---
        // Find all the HTML elements we need to interact with.
        this.form = document.getElementById('courseSelectionForm');
        this.table = document.getElementById('upcoming-courses-table');
        this.tbody = document.getElementById('upcoming-courses-tbody');
        this.selectAllCheckbox = document.getElementById('selectAllCourses');
        this.reviewButton = document.getElementById('reviewSelectedBtn');
        this.selectedCoursesInput = document.getElementById('selectedCoursesInput'); // Hidden input
        this.headers = this.table ? Array.from(this.table.querySelectorAll('th.sortable-header')) : [];
        
        // --- 2. Data Stores ---
        // Parse the JSON data embedded in the <script> tags.
        const coursesDataScript = document.getElementById('courses-data-json');
        const courseConfigsDataScript = document.getElementById('course-configs-data-json');
        this.allCoursesData = this._parseJsonData(coursesDataScript, 'courses');
        this.courseConfigsList = this._parseJsonData(courseConfigsDataScript, 'course configs');
        
        // --- 3. State Management ---
        // Keeps track of the current column and direction for sorting.
        this.sortState = { column: 'Start Date', direction: 'asc' };

        // --- 4. Initialization Check ---
        // If any critical element is missing, log an error and stop.
        if (!this.form || !this.tbody || !this.selectAllCheckbox || !this.selectedCoursesInput) {
            console.error("UpcomingCoursesPlanner: A required UI element is missing. The page will not be interactive.");
            return;
        }

        // --- 5. Initial Setup ---
        this._attachEventListeners();
        this.sortAndRender(); // Perform the initial sort and render the table.
    }

    /**
     * A helper to safely parse JSON from a script tag.
     * @param {HTMLElement} scriptElement - The <script> tag containing the JSON.
     * @param {string} dataType - A friendly name for the data type for error logging.
     * @returns {Array} - The parsed data or an empty array on failure.
     */
    _parseJsonData(scriptElement, dataType) {
        try {
            if (scriptElement?.textContent) {
                return JSON.parse(scriptElement.textContent);
            }
        } catch (e) {
            console.error(`Error parsing embedded ${dataType} JSON data:`, e);
            if (this.tbody) this.tbody.innerHTML = `<tr><td colspan="8" class="text-center p-3">Error loading page data.</td></tr>`;
        }
        return [];
    }

    /**
     * Attaches all necessary event listeners to the DOM elements.
     */
    _attachEventListeners() {
        // Handle sorting when a table header is clicked.
        this.headers.forEach(header => {
            header.addEventListener('click', () => {
                const column = header.dataset.column;
                const type = header.dataset.type;
                this.sortAndRender(column, type);
            });
        });

        // Handle the "Select All" checkbox.
        this.selectAllCheckbox.addEventListener('change', () => {
            this.tbody.querySelectorAll('.course-checkbox:not(:disabled)').forEach(checkbox => {
                checkbox.checked = this.selectAllCheckbox.checked;
            });
        });

        // Handle the main form submission.
        this.form.addEventListener('submit', (event) => this._handleFormSubmit(event));
    }

    /**
     * Clears and re-renders the entire table body based on the provided data.
     * @param {Array<Object>} dataToRender - The array of course objects to display.
     */
    renderTable(dataToRender) {
        this.tbody.innerHTML = '';
        if (!dataToRender || dataToRender.length === 0) {
            this.tbody.innerHTML = `<tr><td colspan="8" class="text-center p-3">No upcoming courses found.</td></tr>`;
            return;
        }

        dataToRender.forEach((courseItem) => {
            const row = this.tbody.insertRow();
            row.dataset.originalCourseItem = JSON.stringify(courseItem);
            
            const sfCourseCode = courseItem['Course Code'] || '';
            const derivedVendor = courseItem['vendor'] || (sfCourseCode ? sfCourseCode.substring(0, 2).toLowerCase() : '');
            
            // --- Cell 0: Selection Checkbox ---
            const checkboxCell = row.insertCell();
            checkboxCell.className = 'col-checkbox';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input course-checkbox';
            checkbox.addEventListener('change', () => this._updateSelectAllState());
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
            trainerCheckbox.checked = !(['nu', 'av'].includes(derivedVendor) || (courseItem['Course Type'] || '').toLowerCase().includes('maestro'));
            trainerToggleCell.appendChild(trainerCheckbox);

            // --- Cell 7: LabBuild Course Dropdown ---
            const courseSelectCell = row.insertCell();
            const courseSelect = this._createLabBuildCourseDropdown(derivedVendor, courseItem.preselect_labbuild_course);
            courseSelectCell.appendChild(courseSelect);

            // Link the main selection checkbox to the dropdown's state
            checkbox.disabled = !courseSelect.value;
            row.classList.toggle('table-warning', !courseSelect.value);
            courseSelect.addEventListener('change', () => {
                checkbox.disabled = !courseSelect.value;
                if (!courseSelect.value) checkbox.checked = false;
                row.classList.toggle('table-warning', !courseSelect.value);
                this._updateSelectAllState();
            });
        });
        
        this._updateSelectAllState();
    }

    /**
     * Creates and configures the LabBuild Course dropdown for a table row.
     * @param {string} derivedVendor - The vendor code for filtering courses.
     * @param {string|null} preselectedCourse - The course that should be pre-selected.
     * @returns {HTMLSelectElement} - The configured select element.
     */
    _createLabBuildCourseDropdown(derivedVendor, preselectedCourse) {
        const courseSelect = document.createElement('select');
        courseSelect.className = 'form-select form-select-sm labbuild-course-select';
        courseSelect.innerHTML = '<option value="">Select LabBuild Course...</option>';

        const optionsFound = this.courseConfigsList
            .filter(config => config.vendor_shortcode?.toLowerCase() === derivedVendor)
            .map(config => {
                const option = document.createElement('option');
                option.value = config.course_name;
                option.textContent = config.course_name;
                if (preselectedCourse && config.course_name === preselectedCourse) {
                    option.selected = true;
                }
                courseSelect.appendChild(option);
                return true;
            }).length > 0;

        if (!optionsFound) courseSelect.disabled = true;
        return courseSelect;
    }

    /**
     * Updates the state of the "Select All" checkbox based on row checkboxes.
     */
    _updateSelectAllState() {
        const enabledCheckboxes = Array.from(this.tbody.querySelectorAll('.course-checkbox:not(:disabled)'));
        const checkedEnabled = enabledCheckboxes.filter(cb => cb.checked);
        this.selectAllCheckbox.checked = enabledCheckboxes.length > 0 && enabledCheckboxes.length === checkedEnabled.length;
        this.selectAllCheckbox.indeterminate = checkedEnabled.length > 0 && checkedEnabled.length < enabledCheckboxes.length;
    }

    /**
     * Handles the final form submission, packaging all selected data.
     * @param {Event} event - The form submission event.
     */
    _handleFormSubmit(event) {
        const selectedCourses = [];
        this.tbody.querySelectorAll('tr').forEach((row) => {
            const checkbox = row.querySelector('.course-checkbox');
            if (checkbox && checkbox.checked) {
                const courseSelect = row.querySelector('.labbuild-course-select');
                const trainerCheckbox = row.querySelector('.trainer-pod-checkbox');
                
                const originalCourseData = JSON.parse(row.dataset.originalCourseItem);
                originalCourseData.user_selected_labbuild_course = courseSelect.value;
                originalCourseData.create_trainer_pod = trainerCheckbox.checked;
                
                selectedCourses.push(originalCourseData);
            }
        });

        if (selectedCourses.length === 0) {
            event.preventDefault(); // Stop the submission
            alert("Please select at least one course to review.");
            return;
        }

        // Populate the hidden input field and allow the form to submit.
        this.selectedCoursesInput.value = JSON.stringify(selectedCourses);
    }

    /**
     * Sorts the main data array and triggers a re-render of the table.
     * @param {string} [columnName=this.sortState.column] - The column to sort by.
     * @param {string} [dataType='string'] - The type of data ('string', 'number', 'date').
     */
    sortAndRender(columnName = this.sortState.column, dataType = 'string') {
        let direction = (this.sortState.column === columnName && this.sortState.direction === 'asc') ? 'desc' : 'asc';
        this.sortState = { column: columnName, direction: direction };
        
        this.allCoursesData.sort((a, b) => {
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
        
        this.renderTable(this.allCoursesData);
        this._updateSortIndicators();
    }
    
    /**
     * Updates the visual sort indicators (up/down arrows) in the table headers.
     */
    _updateSortIndicators() {
        this.headers.forEach(header => {
            header.classList.remove('sort-asc', 'sort-desc');
            const icon = header.querySelector('.sort-icon');
            if(icon) icon.innerHTML = '';
            if (header.dataset.column === this.sortState.column) {
                header.classList.add(this.sortState.direction === 'asc' ? 'sort-asc' : 'sort-desc');
                if(icon) icon.innerHTML = this.sortState.direction === 'asc' ? '<i class="fas fa-sort-up"></i>' : '<i class="fas fa-sort-down"></i>';
            }
        });
    }
}

// --- Initialize the planner once the DOM is fully loaded ---
document.addEventListener('DOMContentLoaded', () => {
    new UpcomingCoursesPlanner();
});