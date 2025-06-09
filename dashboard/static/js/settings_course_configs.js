// dashboard/static/js/settings_course_configs.js

document.addEventListener('DOMContentLoaded', function() {
    // --- Add Config Modal ---
    const addConfigModalElement = document.getElementById('addConfigModal');
    if (addConfigModalElement) {
        const addJsonTextarea = addConfigModalElement.querySelector('#add_config_json');

        addConfigModalElement.addEventListener('show.bs.modal', function() {
            // Set initial empty JSON structure when "Add New" is clicked
            if (addJsonTextarea && (addJsonTextarea.value.trim() === "" || addJsonTextarea.value.trim() === "{}")) {
                addJsonTextarea.value = JSON.stringify({
                    "course_name": "",
                    "vendor_shortcode": "",
                    "description": "",
                    "memory_gb_per_pod": 0,
                    "components": [],
                    "networks": [],
                    "prtg": {},
                    "groups": [] // If your F5 or other configs use this
                }, null, 2); // Pretty print with 2 spaces
            }
        });

        addConfigModalElement.addEventListener('hidden.bs.modal', function () {
            const form = addConfigModalElement.querySelector('form');
            if (form) form.reset();
            // Reset textarea to default placeholder or empty JSON after closing
            if (addJsonTextarea) {
                 addJsonTextarea.value = JSON.stringify({
                    "course_name": "",
                    "vendor_shortcode": ""
                }, null, 2);
            }
        });
    } else {
        console.warn("Add Config Modal element ('addConfigModal') not found.");
    }


    // --- Edit Config Modal ---
    const editConfigModalElement = document.getElementById('editConfigModal');
    if (editConfigModalElement) {
        editConfigModalElement.addEventListener('show.bs.modal', function (event) {
            const button = event.relatedTarget; // Button that triggered the modal
            if (!button) {
                console.error("Edit Config modal: No relatedTarget (triggering button) found.");
                return;
            }

            // Extract data from button's data-* attributes
            const configId = button.getAttribute('data-config-id');
            const configJsonString = button.getAttribute('data-config-json');

            // Log for debugging
            console.log("--- Edit Config Modal Data ---");
            console.log("ID:", configId);
            console.log("Raw Config JSON Attr:", configJsonString);

            // Get modal elements
            const modalTitle = editConfigModalElement.querySelector('.modal-title');
            const configIdInput = editConfigModalElement.querySelector('#edit_config_id');
            const configJsonTextarea = editConfigModalElement.querySelector('#edit_config_json');
            const errorContainer = editConfigModalElement.querySelector('.modal-body');

            // Ensure all modal elements are found
            if (!modalTitle || !configIdInput || !configJsonTextarea || !errorContainer) {
                console.error("Edit Config modal: One or more form elements not found. Check IDs.");
                if (errorContainer) errorContainer.innerHTML = '<p class="text-danger">Modal form error. Cannot load data.</p>';
                return;
            }

            // Clear previous dynamic error messages
            errorContainer.querySelectorAll('.dynamic-error-msg').forEach(el => el.remove());

            // Populate hidden ID field
            configIdInput.value = configId || '';

            // Process and populate JSON textarea
            let configObject = {}; // Default to empty object
            let courseNameForTitle = 'N/A';

            if (configJsonString && configJsonString.trim() !== "") {
                try {
                    // Browsers automatically unescape HTML entities from getAttribute
                    configObject = JSON.parse(configJsonString);
                    courseNameForTitle = configObject.course_name || 'N/A'; // Get course name for title
                    // Remove _id from the object displayed in textarea, as it's not directly editable
                    // but it is submitted via the hidden input.
                    delete configObject._id;
                } catch (e) {
                    console.error("Error parsing config JSON for edit modal:", e, "\nOriginal JSON String:", configJsonString);
                    configJsonTextarea.value = configJsonString; // Show raw string on error
                    const errorMsg = document.createElement('small');
                    errorMsg.className = 'text-danger d-block dynamic-error-msg';
                    errorMsg.textContent = 'Invalid JSON format in current config. Displaying raw data.';
                    // Insert error message after the textarea
                    configJsonTextarea.parentNode.insertBefore(errorMsg, configJsonTextarea.nextSibling);
                }
            }
            // Always set textarea value, pretty-printing the object (or empty if error/no data)
            // This ensures that if an error occurred above and textarea was set to raw string, it remains.
            // Otherwise, if it's empty or was successfully parsed, pretty-print.
            if (configJsonTextarea.value.trim() === "" || configJsonTextarea.value.trim() === "{}" || configObject ) {
                 configJsonTextarea.value = JSON.stringify(configObject, null, 2);
            }


            // Update modal title
            modalTitle.textContent = 'Edit Course Config: ' + courseNameForTitle;
        });

        // Reset form when Edit Modal is hidden
        editConfigModalElement.addEventListener('hidden.bs.modal', function () {
            const form = editConfigModalElement.querySelector('form');
            if (form) form.reset(); // Resets form fields to their initial HTML values
            editConfigModalElement.querySelectorAll('.dynamic-error-msg').forEach(el => el.remove());
            const modalTitle = editConfigModalElement.querySelector('.modal-title');
            if (modalTitle) modalTitle.textContent = 'Edit Course Configuration'; // Reset title
            const configJsonTextarea = editConfigModalElement.querySelector('#edit_config_json');
            if (configJsonTextarea) configJsonTextarea.value = '{}'; // Clear textarea
        });
    } else {
        console.warn("Edit Config Modal element ('editConfigModal') not found.");
    }
});