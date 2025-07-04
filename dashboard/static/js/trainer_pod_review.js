// dashboard/static/js/trainer_pod_review.js

document.addEventListener('DOMContentLoaded', function() {
    const trainerReviewTable = document.getElementById('trainerReviewTable');
    if (!trainerReviewTable) {
        console.warn("Trainer review table not found. JS functionality disabled.");
        return;
    }

    // --- Load embedded data ---
    let MEMORY_FACTOR_TP = 0.5;
    try {
        MEMORY_FACTOR_TP = JSON.parse(document.getElementById('memoryFactorDataTrainer')?.textContent || '{"factor":0.5}').factor;
    } catch (e) {
        console.error("Error parsing memory factor data:", e);
    }

    // --- Helper to calculate memory for a single row (which represents a group) ---
    function calculateGroupMemory(groupRowEl) {
        const memoryDisplayEl = groupRowEl.querySelector('.total-course-memory-display');
        const buildCheckbox = groupRowEl.querySelector('.build-group-checkbox');
        if (!memoryDisplayEl || !buildCheckbox) return;

        if (!buildCheckbox.checked) {
            memoryDisplayEl.textContent = 'N/A';
            return;
        }

        const memoryPerPod = parseFloat(groupRowEl.dataset.memoryOnePod);
        if (isNaN(memoryPerPod) || memoryPerPod <= 0) {
            memoryDisplayEl.textContent = '0.00';
            return;
        }

        let totalPodsInGroup = 0;
        let isRowValid = true;

        groupRowEl.querySelectorAll('.assignment-sub-row').forEach(subRow => {
            const startPodInput = subRow.querySelector('.start-pod-input');
            const endPodInput = subRow.querySelector('.end-pod-input');
            const startVal = parseInt(startPodInput.value, 10);
            const endVal = parseInt(endPodInput.value, 10);

            startPodInput.classList.toggle('is-invalid', isNaN(startVal) || startVal < 1);
            if (isNaN(startVal) || startVal < 1) isRowValid = false;

            endPodInput.classList.toggle('is-invalid', isNaN(endVal) || endVal < 1 || endVal < startVal);
            if (isNaN(endVal) || endVal < 1 || endVal < startVal) isRowValid = false;

            if (isRowValid) {
                totalPodsInGroup += (endVal - startVal + 1);
            }
        });

        if (!isRowValid) {
            memoryDisplayEl.textContent = '0.00';
            return;
        }
        
        // Use the total number of pods from all assignment segments for the calculation
        const totalMemory = totalPodsInGroup > 0
            ? (memoryPerPod + (totalPodsInGroup - 1) * memoryPerPod * MEMORY_FACTOR_TP)
            : 0;
            
        memoryDisplayEl.textContent = totalMemory.toFixed(2);
    }
    
    // --- Helper to enable/disable inputs for a row ---
    function toggleGroupInputs(groupRowEl, enable) {
        groupRowEl.querySelectorAll('.sub-host-select, .sub-pod-input').forEach(input => {
            input.disabled = !enable;
        });
        calculateGroupMemory(groupRowEl);
    }

    // --- Main initialization loop for each group row ---
    trainerReviewTable.querySelectorAll('tbody tr.trainer-group-row').forEach(groupRow => {
        const buildCheckbox = groupRow.querySelector('.build-group-checkbox');
        
        if (buildCheckbox) {
            buildCheckbox.addEventListener('change', function() {
                toggleGroupInputs(groupRow, this.checked);
            });
        }
        
        groupRow.querySelectorAll('.sub-pod-input, .sub-host-select').forEach(input => {
            input.addEventListener('input', () => {
                calculateGroupMemory(groupRow);
            });
        });
        
        // Initial state setup
        toggleGroupInputs(groupRow, buildCheckbox.checked);
    });

    // --- Form submission logic ---
    const finalizeAndProceedFormElement = document.getElementById('finalizeAndProceedForm');
    if (finalizeAndProceedFormElement) {
        finalizeAndProceedFormElement.addEventListener('submit', function(event) {
            const finalTrainerAssignments = [];
            let isFormValid = true;

            document.querySelectorAll('#trainerReviewTable tbody tr.trainer-group-row').forEach(groupRow => {
                if (!isFormValid) return;

                const buildCheckbox = groupRow.querySelector('.build-group-checkbox');
                let assignmentPayload = {
                    group_id: groupRow.dataset.groupId,
                    build_trainer: buildCheckbox.checked,
                    tag: groupRow.dataset.tag,
                    labbuild_course: groupRow.dataset.labbuildCourse,
                    vendor: groupRow.dataset.vendor,
                    num_pods: parseInt(groupRow.dataset.numPods, 10),
                    sf_codes_in_group: (groupRow.dataset.sfCodesInGroup || "").split(','),
                    sf_course_types_in_group: (groupRow.dataset.sfCourseTypesInGroup || "").split(','),
                    memory_gb_one_pod: parseFloat(groupRow.dataset.memoryOnePod),
                    interactive_assignments: [],
                    error_message: groupRow.querySelector('.text-warning')?.textContent || null
                };

                if (buildCheckbox.checked) {
                    groupRow.querySelectorAll('.assignment-sub-row').forEach(subRow => {
                        const hostSelect = subRow.querySelector('.sub-host-select');
                        const startPodInput = subRow.querySelector('.start-pod-input');
                        const endPodInput = subRow.querySelector('.end-pod-input');
                        
                        if (!hostSelect.value || startPodInput.classList.contains('is-invalid') || endPodInput.classList.contains('is-invalid')) {
                            alert(`Invalid host or pod range for group: ${assignmentPayload.tag}`);
                            isFormValid = false;
                        } else {
                            assignmentPayload.interactive_assignments.push({
                                host: hostSelect.value,
                                start_pod: parseInt(startPodInput.value, 10),
                                end_pod: parseInt(endPodInput.value, 10)
                            });
                        }
                    });
                }
                
                finalTrainerAssignments.push(assignmentPayload);
            });

            if (!isFormValid) {
                event.preventDefault(); return;
            }
            
            const finalInput = document.getElementById('finalTrainerAssignmentsInput');
            if (finalInput) {
                finalInput.value = JSON.stringify(finalTrainerAssignments);
            } else {
                event.preventDefault();
                console.error("Hidden input #finalTrainerAssignmentsInput not found!");
            }
        });
    }
});