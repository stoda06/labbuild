// dashboard/static/js/upcoming_courses.js

document.addEventListener('DOMContentLoaded', function () {
    const table = document.getElementById('upcoming-courses-table');
    const tbody = document.getElementById('upcoming-courses-tbody');
    const headers = table ? table.querySelectorAll('th.sortable-header') : [];
    const selectAllCheckbox = document.getElementById('selectAllCourses');
    const bulkBuildButton = document.getElementById('bulkBuildBtn');
    const coursesDataScript = document.getElementById('courses-data');
    const courseConfigsDataScript = document.getElementById('course-configs-data');
    const loadingIndicator = document.getElementById('buildLoadingIndicator');
    const loadingText = document.getElementById('buildLoadingText');

    let courseData = [];
    let courseConfigsList = [];
    let sortState = { column: 'Start Date', direction: 'asc' };

    function parseInitialData() {
        try {
            if (coursesDataScript?.textContent) {
                courseData = JSON.parse(coursesDataScript.textContent);
            }
            if (courseConfigsDataScript?.textContent) {
                courseConfigsList = JSON.parse(courseConfigsDataScript.textContent);
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
        tbody.innerHTML = '';
        const colCount = 9;
        if (!dataToRender || dataToRender.length === 0) {
            tbody.innerHTML = `<tr><td colspan="${colCount}">No upcoming courses found.</td></tr>`;
            return;
        }

        dataToRender.forEach((courseItem, index) => {
            const row = tbody.insertRow();
            const sfCourseCode = courseItem['Course Code'] || '';
            const derivedVendor = courseItem['vendor'] || (sfCourseCode ? sfCourseCode.substring(0, 2).toLowerCase() : '');
            const preselectedLabbuildCourse = courseItem.preselect_labbuild_course || null;
            const originalDataIndex = courseData.findIndex(cd => cd['Course Code'] === sfCourseCode && cd['Start Date'] === courseItem['Start Date'] && cd['Trainer'] === courseItem['Trainer']);
            
            row.dataset.originalDataIndex = originalDataIndex !== -1 ? originalDataIndex : index;
            row.dataset.sfCourseCode = sfCourseCode;
            row.dataset.derivedVendor = derivedVendor;
            row.dataset.sfCourseType = courseItem['Course Type'] || 'N/A';
            row.dataset.sfStartDate = courseItem['Start Date'] || 'N/A';
            row.dataset.sfEndDate = courseItem['End Date'] || 'N/A';
            row.dataset.sfTrainerName = courseItem['Trainer'] || 'N/A';
            row.dataset.sfPaxCount = String(courseItem['Pax'] ?? '0');
            row.dataset.sfPodsReq = String(courseItem['Pods Req.'] ?? '1');

            const checkboxCell = row.insertCell(0);
            checkboxCell.className = 'col-checkbox';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input course-checkbox';
            checkboxCell.appendChild(checkbox);

            row.insertCell().textContent = courseItem['Start Date'] || 'N/A';
            row.insertCell().textContent = courseItem['End Date'] || 'N/A';
            row.insertCell().textContent = sfCourseCode;
            row.insertCell().textContent = courseItem['Course Type'] || 'N/A';
            row.insertCell().textContent = courseItem['Trainer'] || 'N/A';
            const paxCell = row.insertCell(); paxCell.textContent = courseItem['Pax'] ?? 'N/A'; paxCell.classList.add('col-numbers');
            const podsReqCell = row.insertCell(); podsReqCell.textContent = courseItem['Pods Req.'] ?? 'N/A'; podsReqCell.classList.add('col-numbers');

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
                courseSelect.value = courseSelect.options[1].value;
            }

            if (optionsFound === 0) {
                courseSelect.disabled = true;
                const noOpt = document.createElement('option');
                noOpt.textContent = 'No courses for vendor'; noOpt.disabled = true;
                courseSelect.insertBefore(noOpt, courseSelect.firstChild);
                courseSelect.value = "";
            } else if (preselectedLabbuildCourse && !preselectOptionFoundInDropdown) {
                const invalidPreselectOpt = document.createElement('option');
                invalidPreselectOpt.value = "";
                invalidPreselectOpt.textContent = `Rule Error: '${preselectedLabbuildCourse}' invalid`;
                invalidPreselectOpt.disabled = true;
                invalidPreselectOpt.selected = true;
                courseSelect.insertBefore(invalidPreselectOpt, courseSelect.firstChild);
            }
            courseSelectCell.appendChild(courseSelect);

            checkbox.disabled = !courseSelect.value;
            checkbox.title = courseSelect.value ? "Select this course for build" : "Cannot select: Please choose a LabBuild Course.";
            row.classList.toggle('table-warning', !courseSelect.value);

            courseSelect.addEventListener('change', function () {
                checkbox.disabled = !this.value;
                checkbox.title = this.value ? "Select this course for build" : "Cannot select: Please choose a LabBuild Course.";
                if (!this.value) {
                    checkbox.checked = false;
                }
                row.classList.toggle('table-warning', !this.value);
                updateSelectAllState();
            });
        });
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
        selectAllCheckbox.indeterminate = checkedEnabledCheckboxes.length > 0 && checkedEnabledCheckboxes.length < enabledCheckboxes.length;
    }

    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', function () {
            tbody.querySelectorAll('.course-checkbox:not(:disabled)').forEach(checkbox => {
                checkbox.checked = this.checked;
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
                    const originalIdx = parseInt(row.dataset.originalDataIndex, 10);
                    const originalCourseItem = (originalIdx >= 0 && originalIdx < courseData.length) ? courseData[originalIdx] : {};
                    selectedRowsData.push({
                        sf_course_code: row.dataset.sfCourseCode,
                        labbuild_course: selectedLabCourse,
                        vendor: row.dataset.derivedVendor,
                        sf_course_type: row.dataset.sfCourseType,
                        sf_start_date: row.dataset.sfStartDate,
                        sf_end_date: row.dataset.sfEndDate,
                        sf_trainer_name: row.dataset.sfTrainerName,
                        sf_pax_count: parseInt(row.dataset.sfPaxCount, 10) || 0,
                        sf_pods_req: row.dataset.sfPodsReq,
                        preselect_host: originalCourseItem.preselect_host || null,
                        preselect_host_priority_list: originalCourseItem.preselect_host_priority_list || [],
                        preselect_allow_spillover: originalCourseItem.preselect_allow_spillover !== undefined ? originalCourseItem.preselect_allow_spillover : true,
                        preselect_start_pod: originalCourseItem.preselect_start_pod || 1,
                        preselect_max_pods_applied_constraint: originalCourseItem.preselect_max_pods_applied_constraint || null,
                        maestro_split_config_details: originalCourseItem.maestro_split_config_details || null,
                        preselect_note: originalCourseItem.preselect_note || null
                    });
                }
            });

            if (selectedRowsData.length === 0) {
                 alert('None of the selected rows had complete build information.');
                 bulkBuildButton.disabled = false;
                 if (loadingIndicator) loadingIndicator.style.display = 'none';
                 if (loadingText) loadingText.style.display = 'none';
                 return;
            }

            console.log("Data collected for review to be sent:", selectedRowsData);

            const form = document.createElement('form');
            form.method = 'POST';
            // --- HIGHLIGHTED FIX: Use the URL from the button's data attribute ---
            form.action = this.dataset.buildReviewUrl;
            // --- END HIGHLIGHTED FIX ---
            
            const input = document.createElement('input');
            input.type = 'hidden';
            input.name = 'selected_courses';
            input.value = JSON.stringify(selectedRowsData);
            form.appendChild(input);
            document.body.appendChild(form);
            form.submit();
        });
    }

    function sortTable(columnName, dataType) {
        if (!columnName || !courseData) return;
        let direction = (sortState.column === columnName && sortState.direction === 'asc') ? 'desc' : 'asc';
        sortState.column = columnName; sortState.direction = direction;

        courseData.sort((a, b) => {
            let valA = a[columnName];
            let valB = b[columnName];
            if (dataType === 'date') {
                const parseDateSafely = (dateStr) => {
                    if (!dateStr || dateStr === "N/A") return new Date(0);
                    let d = new Date(dateStr + 'T00:00:00Z');
                    if (!isNaN(d.getTime())) return d;
                    const parts = String(dateStr).split('/');
                    if (parts.length === 3) {
                        d = new Date(parts[2] + '-' + parts[1] + '-' + parts[0] + 'T00:00:00Z');
                        if (!isNaN(d.getTime())) return d;
                    }
                    return new Date(0);
                };
                valA = parseDateSafely(valA);
                valB = parseDateSafely(valB);
            } else if (dataType === 'number') {
                valA = parseFloat(String(valA).match(/(\d+)/)?.[0] || 0);
                valB = parseFloat(String(valB).match(/(\d+)/)?.[0] || 0);
            } else {
                valA = String(valA || '').toLowerCase();
                valB = String(valB || '').toLowerCase();
            }
            if (valA < valB) return direction === 'asc' ? -1 : 1;
            if (valA > valB) return direction === 'asc' ? 1 : -1;
            return 0;
        });
        renderTable(courseData);
        updateSortIndicators();
    }
    
    function updateSortIndicators() {
        headers.forEach(header => {
            header.classList.remove('sort-asc', 'sort-desc');
            const icon = header.querySelector('.sort-icon');
            if (icon) icon.innerHTML = '';
            if (header.dataset.column === sortState.column) {
                header.classList.add(sortState.direction === 'asc' ? 'sort-asc' : 'sort-desc');
                if (icon) icon.innerHTML = sortState.direction === 'asc' ? '<i class="fas fa-sort-up"></i>' : '<i class="fas fa-sort-down"></i>';
            }
        });
    }

    headers.forEach(header => {
        header.addEventListener('click', () => {
            sortTable(header.dataset.column, header.dataset.type);
        });
    });

    if (parseInitialData()) {
        sortTable('Start Date', 'date');
    } else {
        if (tbody) tbody.innerHTML = '<tr><td colspan="9">Failed to load course data for table.</td></tr>';
    }
});