// dashboard/static/js/trainer_pod_review.js

document.addEventListener('DOMContentLoaded', function() {
    const trainerReviewTable = document.getElementById('trainerReviewTable');
    if (!trainerReviewTable) {
        console.warn("Trainer review table not found. JS functionality disabled.");
        return;
    }

    let COURSE_MEMORY_STORE_TP = [];
    let MEMORY_FACTOR_TP = 0.5;

    try {
        COURSE_MEMORY_STORE_TP = JSON.parse(document.getElementById('courseMemoryDataTrainer')?.textContent || '[]');
        MEMORY_FACTOR_TP = JSON.parse(document.getElementById('memoryFactorDataTrainer')?.textContent || '{"factor":0.5}').factor;
    } catch (e) {
        console.error("Error parsing embedded trainer review data:", e);
    }

    function getLabbuildCourseMemoryTP(lbCourseName) {
        if (!lbCourseName || !COURSE_MEMORY_STORE_TP.length) return 0;
        const config = COURSE_MEMORY_STORE_TP.find(c => c.course_name === lbCourseName);
        if (config) {
            const memValStr = config.memory_gb_per_pod !== undefined ? String(config.memory_gb_per_pod) : String(config.memory);
            const parsedMem = parseFloat(memValStr);
            return !isNaN(parsedMem) && parsedMem >= 0 ? parsedMem : 0;
        }
        return 0;
    }

    function calculateAndDisplayTrainerPodMemory(trainerRowEl) {
        const memoryDisplayEl = trainerRowEl.querySelector('.total-course-memory-display');
        const startPodInput = trainerRowEl.querySelector('.start-pod-input');
        const endPodInput = trainerRowEl.querySelector('.end-pod-input');
        const buildCheckbox = trainerRowEl.querySelector('.build-trainer-checkbox');

        if (!memoryDisplayEl || !startPodInput || !endPodInput) return;
        
        // If the build checkbox is unchecked or disabled, show N/A
        if (!buildCheckbox || !buildCheckbox.checked) {
            memoryDisplayEl.textContent = 'N/A';
            return;
        }

        const memoryPerPod = parseFloat(trainerRowEl.dataset.memoryOnePod);
        startPodInput.classList.toggle('is-invalid', isNaN(parseInt(startPodInput.value, 10)) || parseInt(startPodInput.value, 10) < 1);
        
        if (startPodInput.classList.contains('is-invalid') || isNaN(memoryPerPod) || memoryPerPod <= 0) {
            memoryDisplayEl.textContent = '0.00';
            return;
        }
        // For a single trainer pod, memory is just the base memory. No factor applied.
        memoryDisplayEl.textContent = memoryPerPod.toFixed(2);
    }

    function toggleTrainerInputs(trainerRowElement, enable) {
        const subRow = trainerRowElement.querySelector('.assignment-sub-row');
        const hostSelect = trainerRowElement.querySelector('.sub-host-select');
        const startPodInput = trainerRowElement.querySelector('.start-pod-input');
        
        if (subRow) subRow.classList.toggle('disabled-inputs', !enable);
        if (hostSelect) hostSelect.disabled = !enable;
        if (startPodInput) startPodInput.disabled = !enable;

        calculateAndDisplayTrainerPodMemory(trainerRowElement);
    }
    
    trainerReviewTable.querySelectorAll('tbody tr.trainer-data-row').forEach(trainerRow => {
        const buildCheckbox = trainerRow.querySelector('.build-trainer-checkbox');
        const startPodInput = trainerRow.querySelector('.start-pod-input');
        const endPodInput = trainerRow.querySelector('.end-pod-input'); 
        const hostSelect = trainerRow.querySelector('.sub-host-select');
        
        if (buildCheckbox && !buildCheckbox.disabled) {
            buildCheckbox.addEventListener('change', function() {
                toggleTrainerInputs(trainerRow, this.checked);
            });
        }
        
        if (startPodInput && endPodInput) {
            startPodInput.addEventListener('input', function() {
                if (!startPodInput.disabled) {
                    endPodInput.value = this.value; // Keep end pod in sync with start
                    calculateAndDisplayTrainerPodMemory(trainerRow);
                }
            });
        }

        if (hostSelect) {
             hostSelect.addEventListener('change', () => {
                 if (!hostSelect.disabled) calculateAndDisplayTrainerPodMemory(trainerRow);
             });
        }

        // Initial setup on page load
        if(buildCheckbox) {
            toggleTrainerInputs(trainerRow, buildCheckbox.checked);
        }
    });

    const finalizeAndProceedFormElement = document.getElementById('finalizeAndProceedForm');
    if (finalizeAndProceedFormElement) {
        finalizeAndProceedFormElement.addEventListener('submit', function(event) {
            const finalTrainerAssignments = [];
            let isFormValid = true;

            document.querySelectorAll('#trainerReviewTable tbody tr.trainer-data-row').forEach(trainerRow => {
                if (!isFormValid) return;

                const buildCheckbox = trainerRow.querySelector('.build-trainer-checkbox');
                const sfTrainerCode = trainerRow.dataset.sfCourseCodeTrainer;
                
                let assignmentPayload = {
                    interim_doc_id: trainerRow.dataset.interimDocId,
                    build_trainer: buildCheckbox.checked,
                    labbuild_course: trainerRow.dataset.labbuildCourse,
                    memory_gb_one_pod: parseFloat(trainerRow.dataset.memoryOnePod),
                    interactive_assignments: [],
                    error_message: trainerRow.querySelector('.error-icon-container')?.title || null
                };

                if (buildCheckbox.disabled) {
                    assignmentPayload.build_trainer = false;
                    assignmentPayload.error_message = trainerRow.querySelector('.error-icon-container')?.title || "Disabled by rule";
                } else if (buildCheckbox.checked) {
                    const hostSelect = trainerRow.querySelector('.sub-host-select');
                    const startPodInput = trainerRow.querySelector('.start-pod-input');
                    calculateAndDisplayTrainerPodMemory(trainerRow);
                    
                    if (!hostSelect.value) {
                        alert(`Host not selected for Trainer Pod: ${sfTrainerCode}.`);
                        hostSelect.focus(); isFormValid = false; return;
                    }
                    if (startPodInput.classList.contains('is-invalid')) {
                        alert(`Invalid pod number for Trainer Pod: ${sfTrainerCode}.`);
                        startPodInput.focus(); isFormValid = false; return;
                    }
                    const podValue = parseInt(startPodInput.value, 10);
                    assignmentPayload.interactive_assignments = [{ host: hostSelect.value, start_pod: podValue, end_pod: podValue }];
                }
                
                finalTrainerAssignments.push(assignmentPayload);
            });

            if (!isFormValid) {
                event.preventDefault(); return;
            }
            
            const finalTrainerAssignmentsInput = document.getElementById('finalTrainerAssignmentsInput');
            if (finalTrainerAssignmentsInput) {
                finalTrainerAssignmentsInput.value = JSON.stringify(finalTrainerAssignments);
            } else {
                console.error("Hidden input #finalTrainerAssignmentsInput not found."); event.preventDefault(); return;
            }
        });
    }
});