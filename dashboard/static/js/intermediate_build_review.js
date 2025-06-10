// dashboard/static/js/intermediate_build_review.js

document.addEventListener('DOMContentLoaded', function() {
    const studentReviewTableEl = document.getElementById('studentReviewTable');
    if (!studentReviewTableEl) {
        console.warn("Student review table element not found.");
        return;
    }

    // --- Load data from script tags ---
    let ALL_HOSTS_LIST_STUDENT = []; 
    let COURSE_MEMORY_STORE_STUDENT = []; 
    let MEMORY_FACTOR_STUDENT = 0.5; 

    try { 
        ALL_HOSTS_LIST_STUDENT = JSON.parse(document.getElementById('allHostsDataStudentReview')?.textContent || '[]');
        COURSE_MEMORY_STORE_STUDENT = JSON.parse(document.getElementById('courseMemoryDataStudentReview')?.textContent || '[]');
        MEMORY_FACTOR_STUDENT = JSON.parse(document.getElementById('memoryFactorDataStudentReview')?.textContent || '{"factor":0.5}').factor;
    } catch (e) { console.error("Error parsing embedded data for student review page:", e); }

    // --- Helper to get memory for a LabBuild course ---
    function getLbCourseMemoryStudent(lbCourseName) {
        if (!lbCourseName || !COURSE_MEMORY_STORE_STUDENT.length) return 0;
        const config = COURSE_MEMORY_STORE_STUDENT.find(c => c.course_name === lbCourseName);
        if (config) {
            const memValStr = config.memory_gb_per_pod !== undefined ? String(config.memory_gb_per_pod) : String(config.memory);
            const parsedMem = parseFloat(memValStr);
            return !isNaN(parsedMem) && parsedMem >= 0 ? parsedMem : 0;
        }
        return 0;
    }

    // --- Calculate memory for a single assignment sub-row ---
    function calculateSubRowMemoryStudent(subRowEl) {
        const mainRow = subRowEl.closest('tr.student-course-row');
        if (!mainRow) return 0; 
        const memoryPerSinglePod = parseFloat(mainRow.dataset.memoryOnePod);
        const startPodInput = subRowEl.querySelector('.start-pod-input');
        const endPodInput = subRowEl.querySelector('.end-pod-input');
        
        if (!startPodInput || !endPodInput || isNaN(memoryPerSinglePod) || memoryPerSinglePod <= 0) return 0;

        let startVal = parseInt(startPodInput.value, 10);
        let endVal = parseInt(endPodInput.value, 10);
        let isValid = true;
        
        startPodInput.classList.toggle('is-invalid', isNaN(startVal) || startVal < 1);
        if (isNaN(startVal) || startVal < 1) isValid = false;
        
        endPodInput.classList.toggle('is-invalid', isNaN(endVal) || endVal < 1 || (!isNaN(startVal) && endVal < startVal));
        if (isNaN(endVal) || endVal < 1 || (!isNaN(startVal) && endVal < startVal)) isValid = false;

        if (!isValid) return 0;
        const numPods = endVal - startVal + 1;
        return numPods >= 1 ? (memoryPerSinglePod + (numPods - 1) * memoryPerSinglePod * MEMORY_FACTOR_STUDENT) : 0;
    }

    // --- Update total memory display for a main course row ---
    function updateTotalCourseMemoryStudent(mainCourseRowEl) {
        let totalCourseMem = 0;
        mainCourseRowEl.querySelectorAll('.assignment-sub-row').forEach(subRow => {
            totalCourseMem += calculateSubRowMemoryStudent(subRow); 
        });
        const totalDisplayEl = mainCourseRowEl.querySelector('.total-course-memory-display');
        if (totalDisplayEl) totalDisplayEl.textContent = totalCourseMem.toFixed(2);
    }

    // --- Manage visibility of add/remove buttons in sub-rows ---
    function updateActionButtonsVisibilityStudent(interactiveCellEl) {
        const subRowsContainer = interactiveCellEl.querySelector('.assignment-sub-rows-container');
        if (!subRowsContainer) return;
        const subRows = subRowsContainer.querySelectorAll('.assignment-sub-row');
        const mainAddBtn = interactiveCellEl.querySelector('.add-assignment-btn-main');

        if (subRows.length === 0) {
            if (mainAddBtn) mainAddBtn.style.display = 'block';
            return;
        }
        if (mainAddBtn) mainAddBtn.style.display = 'none';

        subRows.forEach((subRow, index) => {
            const removeBtn = subRow.querySelector('.remove-assignment-btn');
            const addBtnInline = subRow.querySelector('.add-assignment-btn-inline');
            if (removeBtn) removeBtn.style.display = subRows.length > 1 ? 'inline-flex' : 'none';
            if (addBtnInline) addBtnInline.style.display = (index === subRows.length - 1) ? 'inline-flex' : 'none';
        });
    }
    
    // --- Create a new sub-row (template) ---
    function createNewSubRowStudent(mainCourseRowEl, newIndex) {
        let hostOptionsHtml = '<option value="">-- Host --</option>';
        ALL_HOSTS_LIST_STUDENT.forEach(host => {
            hostOptionsHtml += `<option value="${host}">${host}</option>`;
        });
        const newSubRowDiv = document.createElement('div');
        newSubRowDiv.className = 'assignment-sub-row';
        newSubRowDiv.innerHTML = `
            <select name="host_assignment_${newIndex}" class="form-select form-select-sm sub-host-select">${hostOptionsHtml}</select>
            <input type="number" name="start_pod_${newIndex}" class="form-control form-control-sm sub-pod-input start-pod-input" value="1" min="1" placeholder="Start">
            <input type="number" name="end_pod_${newIndex}" class="form-control form-control-sm sub-pod-input end-pod-input" value="1" min="1" placeholder="End">
            <div class="sub-row-actions">
                <button type="button" class="btn remove-assignment-btn" title="Remove this assignment segment"><i class="fa-solid fa-circle-xmark"></i></button>
                <button type="button" class="btn add-assignment-btn-inline" title="Add another assignment segment"><i class="fa-solid fa-circle-plus"></i></button>
            </div>
        `;
        return newSubRowDiv;
    }

    // --- Add event listeners to a sub-row's elements ---
    function addSubRowListenersStudent(subRowElement) {
        subRowElement.querySelectorAll('.sub-pod-input, .sub-host-select').forEach(input => {
            input.addEventListener('input', () => {
                const mainTr = subRowElement.closest('tr.student-course-row');
                if (mainTr) updateTotalCourseMemoryStudent(mainTr);
            });
        });
        
        const removeBtn = subRowElement.querySelector('.remove-assignment-btn');
        if (removeBtn) {
            removeBtn.addEventListener('click', function() {
                const mainTr = this.closest('tr.student-course-row'); 
                const interactiveCell = this.closest('.interactive-assignments-cell');
                this.closest('.assignment-sub-row').remove(); 
                if (mainTr && interactiveCell) {
                    updateTotalCourseMemoryStudent(mainTr); 
                    updateActionButtonsVisibilityStudent(interactiveCell);
                }
            });
        }

        const addBtnInline = subRowElement.querySelector('.add-assignment-btn-inline');
        if (addBtnInline) {
            addBtnInline.addEventListener('click', function() {
                const mainTr = this.closest('tr.student-course-row');
                const interactiveCell = this.closest('.interactive-assignments-cell');
                const subRowsContainer = interactiveCell.querySelector('.assignment-sub-rows-container');
                if (!subRowsContainer || !mainTr) return;
                
                const newSubRowIndex = subRowsContainer.querySelectorAll('.assignment-sub-row').length;
                const newSubRow = createNewSubRowStudent(mainTr, newSubRowIndex);
                
                subRowsContainer.appendChild(newSubRow);
                addSubRowListenersStudent(newSubRow); 
                updateTotalCourseMemoryStudent(mainTr); 
                updateActionButtonsVisibilityStudent(interactiveCell); 
            });
        }
    }
    
    // --- Initialize all course rows ---
    studentReviewTableEl.querySelectorAll('tbody tr.student-course-row').forEach(mainCourseRow => {
        const interactiveCell = mainCourseRow.querySelector('.interactive-assignments-cell');
        if (!interactiveCell) return;
        const subRowsContainer = interactiveCell.querySelector('.assignment-sub-rows-container');
        const mainAddBtn = interactiveCell.querySelector('.add-assignment-btn-main');

        if (subRowsContainer) {
            subRowsContainer.querySelectorAll('.assignment-sub-row').forEach(subRow => {
                addSubRowListenersStudent(subRow);
            });
        }
        if (mainAddBtn) {
            mainAddBtn.addEventListener('click', function() {
                const mainTr = this.closest('tr.student-course-row');
                if (subRowsContainer && mainTr) {
                     const newSubRow = createNewSubRowStudent(mainTr, 0);
                     subRowsContainer.appendChild(newSubRow);
                     addSubRowListenersStudent(newSubRow);
                     updateTotalCourseMemoryStudent(mainTr);
                     updateActionButtonsVisibilityStudent(interactiveCell);
                }
            });
        }
        updateTotalCourseMemoryStudent(mainCourseRow);
        updateActionButtonsVisibilityStudent(interactiveCell); 
    });

    // --- Form Submission Logic ---
    const confirmStudentAssignmentsFormEl = document.getElementById('confirmStudentAssignmentsForm');
    if (confirmStudentAssignmentsFormEl) {
        confirmStudentAssignmentsFormEl.addEventListener('submit', function(event) {
            const finalStudentBuildPlan = [];
            let isOverallFormValid = true;

            document.querySelectorAll('#studentReviewTable tbody tr.student-course-row').forEach(mainCourseRow => {
                if (!isOverallFormValid) return;

                const sfCode = mainCourseRow.dataset.sfCourseCode;
                const courseInteractiveAssignments = [];
                let courseTotalMemory = 0;

                mainCourseRow.querySelectorAll('.assignment-sub-row').forEach(subRow => {
                    if (!isOverallFormValid) return;
                    const hostSel = subRow.querySelector('.sub-host-select');
                    const startPodIn = subRow.querySelector('.start-pod-input');
                    const endPodIn = subRow.querySelector('.end-pod-input');
                    
                    const subRowMemory = calculateSubRowMemoryStudent(subRow);
                    courseTotalMemory += subRowMemory;

                    if (!hostSel.value) {
                        alert(`Host not selected for an assignment in SF Course: ${sfCode}`);
                        hostSel.focus(); isOverallFormValid = false; return;
                    }
                    if (startPodIn.classList.contains('is-invalid') || endPodIn.classList.contains('is-invalid')) {
                        alert(`Invalid pod range for an assignment in SF Course: ${sfCode}. Please correct highlighted fields.`);
                        if (startPodIn.classList.contains('is-invalid')) startPodIn.focus();
                        else if (endPodIn.classList.contains('is-invalid')) endPodIn.focus();
                        isOverallFormValid = false; return;
                    }
                    const startVal = parseInt(startPodIn.value, 10); 
                    const endVal = parseInt(endPodIn.value, 10);
                    courseInteractiveAssignments.push({ host: hostSel.value, start_pod: startVal, end_pod: endVal });
                });

                if (isOverallFormValid) {
                     // --- MODIFICATION: Read the f5_class_number from the dataset ---
                     const f5ClassNumberStr = mainCourseRow.dataset.f5ClassNumber;
                     const f5ClassNumber = f5ClassNumberStr ? parseInt(f5ClassNumberStr, 10) : null;

                     finalStudentBuildPlan.push({
                        interim_doc_id: mainCourseRow.dataset.interimDocId,
                        sf_course_code: sfCode,
                        labbuild_course: mainCourseRow.dataset.labbuildCourse,
                        vendor: mainCourseRow.dataset.vendor,
                        f5_class_number: f5ClassNumber, // NEW: Add to payload
                        memory_gb_one_pod: parseFloat(mainCourseRow.dataset.memoryOnePod),
                        interactive_assignments: courseInteractiveAssignments,
                        estimated_memory_gb: parseFloat(mainCourseRow.querySelector('.total-course-memory-display').textContent) || 0,
                        assignment_warning: mainCourseRow.querySelector('.assignment-warning-icon') ? mainCourseRow.querySelector('.assignment-warning-icon').title : null,
                    });
                }
            });

            if (!isOverallFormValid) { 
                event.preventDefault(); 
                console.warn("Student assignment validation errors prevented form submission."); 
                return; 
            }
            if (finalStudentBuildPlan.length === 0) { 
                event.preventDefault(); 
                alert("No student assignments defined or all are invalid. Nothing to proceed with."); 
                return; 
            }
            
            const finalPlanInputEl = document.getElementById('finalStudentBuildPlanInput');
            if (finalPlanInputEl) {
                finalPlanInputEl.value = JSON.stringify(finalStudentBuildPlan);
                console.log("Finalized Student Build Plan (to be POSTed):", finalStudentBuildPlan);
            } else {
                event.preventDefault();
                console.error("Hidden input #finalStudentBuildPlanInput not found!");
                alert("Internal error: Cannot prepare data for submission.");
            }
        });
    }
});