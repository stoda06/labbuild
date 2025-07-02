document.addEventListener('DOMContentLoaded', function () {
    const allocationTableBody = document.getElementById('allocationsTableBody');
    const toastElement = document.getElementById('ajaxToast');
    const bsToast = toastElement ? new bootstrap.Toast(toastElement, { delay: 5000 }) : null;

    // --- NEW: Bulk Action Elements ---
    const selectAllGroupsCheckbox = document.getElementById('selectAllGroupsCheckbox');
    const bulkActionsBar = document.getElementById('bulkActionsBar');
    const selectedItemsCountSpan = document.getElementById('selectedItemsCount');
    const bulkActionSubmitForm = document.getElementById('bulkActionSubmitForm');
    const selectedItemsJsonInput = document.getElementById('selectedItemsJsonInput');
    const bulkActionTriggerButtons = document.querySelectorAll('.bulk-action-trigger');


    function showToast(title, message, isError = false) {
        if (!bsToast) return;
        const toastTitleEl = document.getElementById('toastTitle');
        const toastBodyEl = document.getElementById('toastBody');
        
        toastTitleEl.textContent = title;
        toastBodyEl.textContent = message;
        toastElement.className = 'toast'; // Reset classes
        if (isError) {
            toastElement.classList.add('bg-danger', 'text-white');
        } else {
            toastElement.classList.add('bg-success', 'text-white');
        }
        
        bsToast.show();
    }

    async function handleAjaxFormSubmit(form) {
        const button = form.querySelector('button[type="submit"]');
        if (!button) {
            console.error("Could not find submit button within the form.", form);
            return;
        }

        const originalButtonHTML = button.innerHTML;
        button.disabled = true;
        button.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

        try {
            const formData = new FormData(form);
            const response = await fetch(form.action, {
                method: 'POST',
                headers: { 'X-Requested-With': 'XMLHttpRequest' },
                body: formData
            });

            const result = await response.json();

            if (!response.ok) {
                throw new Error(result.message || `Server responded with status ${response.status}`);
            }

            showToast("Success", result.message);

            if (form.classList.contains('ajax-extend-form')) {
                const statusInput = form.querySelector('input[name="current_extend_status"]');
                const icon = button.querySelector('i'); // This will be null now, we need to re-select from originalHTML
                const newStatusIsExtended = result.new_status === 'true';

                if (statusInput) statusInput.value = result.new_status;
                
                // --- THIS IS THE CORRECTED LOGIC ---
                // 1. Restore the original HTML (which contains the <i> icon) to the button first.
                button.innerHTML = originalButtonHTML;
                
                // 2. Now that the icon exists again, re-select it and update its classes.
                const newIcon = button.querySelector('i');
                if (newIcon) {
                    newIcon.classList.remove('fa-lock', 'fa-lock-open');
                    newIcon.classList.add(newStatusIsExtended ? 'fa-lock' : 'fa-lock-open');
                }
                
                // 3. Update the button's color and title.
                button.classList.remove('text-danger', 'text-success');
                button.classList.add(newStatusIsExtended ? 'text-danger' : 'text-success');
                form.title = result.new_title;
                // --- END CORRECTION ---

            } else if (result.action === 'remove') {
                const rowToRemove = form.closest('tr');
                if (rowToRemove) {
                    rowToRemove.style.transition = 'opacity 0.5s ease-out';
                    rowToRemove.style.opacity = '0';
                    setTimeout(() => rowToRemove.remove(), 500);
                }
            }

        } catch (error) {
            console.error("AJAX Form Error:", error);
            showToast("Error", error.message, true);
            // On error, restore the original button content
            if (document.body.contains(button)) {
                button.innerHTML = originalButtonHTML;
            }
        } finally {
            if (document.body.contains(button)) {
                button.disabled = false;
            }
        }
    }
    
    /**
     * Handles clicks on buttons that trigger AJAX actions (like full tag teardown).
     * @param {HTMLButtonElement} button - The button that was clicked.
     */
    async function handleAjaxButtonClick(button) {
        const confirmMessage = button.dataset.confirmMessage;
        if (confirmMessage && !confirm(confirmMessage)) {
            return;
        }
        
        const originalButtonHTML = button.innerHTML;
        button.disabled = true;
        button.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
        
        const formData = new FormData();
        formData.append('tag', button.dataset.tag);

        try {
            const response = await fetch(button.dataset.teardownUrl, {
                method: 'POST',
                headers: { 'X-Requested-With': 'XMLHttpRequest' },
                body: formData
            });

            const result = await response.json();
            
            if (!response.ok) {
                throw new Error(result.message || `Server responded with status ${response.status}`);
            }

            showToast("Success", result.message);

            if (result.action === 'remove') {
                const rowToRemove = button.closest('tr.summary-row');
                if (rowToRemove) {
                    const detailRow = rowToRemove.nextElementSibling;
                    rowToRemove.style.transition = 'opacity 0.5s ease-out';
                    rowToRemove.style.opacity = '0';
                    if (detailRow && detailRow.classList.contains('detail-row')) {
                        detailRow.style.transition = 'opacity 0.5s ease-out';
                        detailRow.style.opacity = '0';
                    }
                    setTimeout(() => {
                        if (detailRow && detailRow.classList.contains('detail-row')) detailRow.remove();
                        rowToRemove.remove();
                    }, 500);
                }
            }
        } catch (error) {
            console.error("AJAX Button Error:", error);
            showToast("Error", error.message, true);
            // Restore button on error
             if (document.body.contains(button)) {
                button.innerHTML = originalButtonHTML;
            }
        } finally {
            // Re-enable button ONLY if it hasn't been removed from the DOM
            if (document.body.contains(button)) {
                button.disabled = false;
            }
        }
    }

    /**
     * Toggles the edit mode for a summary row.
     * @param {HTMLElement} summaryRow - The <tr> element of the summary row.
     * @param {boolean} isEditing - True to enter edit mode, false to exit.
     */
    function toggleEditMode(summaryRow, isEditing) {
        summaryRow.classList.toggle('is-editing', isEditing);
        
        // --- NEW LOGIC: Also toggle visibility of the AJAX action forms ---
        // This ensures the lock/teardown buttons are hidden when editing.
        const viewModeActions = summaryRow.querySelectorAll('.view-mode');
        const editModeActions = summaryRow.querySelectorAll('.edit-mode');

        viewModeActions.forEach(el => el.style.display = isEditing ? 'none' : '');
        editModeActions.forEach(el => el.style.display = isEditing ? 'block' : '');
    }
    

    if (allocationTableBody) {
        // --- Main Event Listener for the entire table ---
        allocationTableBody.addEventListener('click', async function (event) {
            const target = event.target;
            const button = target.closest('button');
            if (!button) return;

            // --- Handler for AJAX Teardown Button ---
            if (button.matches('.ajax-teardown-tag-btn')) {
                event.preventDefault();
                handleAjaxButtonClick(button);
                return;
            }

            const summaryRow = button.closest('.summary-row');
            if (!summaryRow) return; // Exit if not in a summary row context

            // --- Handler for Edit Button ---
            if (button.matches('.edit-btn')) {
                // Copy current view text to input values before showing them
                summaryRow.querySelectorAll('.editable-cell').forEach(cell => {
                    const viewSpan = cell.querySelector('.view-mode');
                    const inputs = cell.querySelectorAll('input');
                    if (viewSpan && inputs.length > 0) {
                        if (cell.classList.contains('col-dates')) {
                            const dates = viewSpan.textContent.split(' to ');
                            inputs[0].value = dates[0] ? dates[0].trim() : '';
                            inputs[1].value = dates[1] ? dates[1].trim() : '';
                        } else if (cell.classList.contains('col-apm')) {
                            const userText = viewSpan.innerHTML.match(/User: (.*?)<br>/)?.[1] || '';
                            const passText = viewSpan.innerHTML.match(/Pass: (.*)/)?.[1] || '';
                            inputs[0].value = userText.trim() === 'N/A' ? '' : userText.trim();
                            inputs[1].value = passText.trim() === 'N/A' ? '' : passText.trim();
                        } else {
                            inputs[0].value = viewSpan.textContent.trim();
                        }
                    }
                });
                toggleEditMode(summaryRow, true);
            }

            // --- Handler for Cancel Button ---
            if (button.matches('.cancel-btn')) {
                toggleEditMode(summaryRow, false);
            }

            // --- Handler for Save Button ---
            if (button.matches('.save-btn')) {
                const groupData = JSON.parse(summaryRow.dataset.groupKey); 
                const courseIdentifier = (Array.isArray(groupData.course_names) && groupData.course_names.length > 0) ? groupData.course_names[0] : null;

                if (!courseIdentifier) {
                    alert('Error: Could not identify the course for this group.');
                    return;
                }
                
                const payload = {
                    tag: groupData.tag,
                    course_name: courseIdentifier,
                    start_date: summaryRow.querySelector('input[name="start_date"]').value,
                    end_date: summaryRow.querySelector('input[name="end_date"]').value,
                    trainer_name: summaryRow.querySelector('input[name="trainer_name"]').value,
                    apm_username: summaryRow.querySelector('input[name="apm_username"]').value,
                    apm_password: summaryRow.querySelector('input[name="password"]').value,
                };

                button.disabled = true;
                button.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

                try {
                    const response = await fetch("{{ url_for('allocation_actions.update_allocation_summary') }}", {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(payload)
                    });
                    const result = await response.json();

                    if (result.success) {
                        summaryRow.querySelector('.col-dates .view-mode').textContent = `${payload.start_date || 'N/A'} to ${payload.end_date || 'N/A'}`;
                        summaryRow.querySelector('.col-trainer .view-mode').textContent = payload.trainer_name || 'N/A';
                        summaryRow.querySelector('.col-apm .view-mode').innerHTML = `User: ${payload.apm_username || 'N/A'}<br>Pass: ${payload.apm_password || 'N/A'}`;
                        
                        groupData.start_date = payload.start_date;
                        groupData.end_date = payload.end_date;
                        groupData.trainer_name = payload.trainer_name;
                        groupData.apm_username = payload.apm_username;
                        groupData.apm_password = payload.apm_password;
                        summaryRow.dataset.groupKey = JSON.stringify(groupData);
                        
                        toggleEditMode(summaryRow, false); // Exit edit mode
                        showToast("Success", "Allocation details updated.");
                    } else {
                        throw new Error(result.error || "Failed to save changes.");
                    }
                } catch (error) {
                    showToast('Save Error', error.message, true);
                    console.error("Save error:", error);
                } finally {
                    button.disabled = false;
                    button.innerHTML = '<i class="fas fa-save"></i> Save';
                }
            }
        });

        // --- Event Listener for Form Submissions ---
        allocationTableBody.addEventListener('submit', function(event) {
            if (event.target.matches('.ajax-extend-form, .ajax-item-teardown-form')) {
                event.preventDefault();
                handleAjaxFormSubmit(event.target);
            }
        });
    }

    

    // --- Expander Icon Toggling ---
    const allocationTable = document.getElementById('allocationsTable');
    if (allocationTable) {
        allocationTable.addEventListener('show.bs.collapse', function (event) {
            const triggerRow = event.target.previousElementSibling;
            const icon = triggerRow.querySelector('.expander-icon');
            if (icon) {
                icon.classList.remove('fa-plus-square');
                icon.classList.add('fa-minus-square');
            }
        });
        allocationTable.addEventListener('hide.bs.collapse', function (event) {
            const triggerRow = event.target.previousElementSibling;
            const icon = triggerRow.querySelector('.expander-icon');
            if (icon) {
                icon.classList.remove('fa-minus-square');
                icon.classList.add('fa-plus-square');
            }
        });
    }

});