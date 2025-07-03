// dashboard/static/js/settings_course_configs.js

document.addEventListener('DOMContentLoaded', function() {
    const configModal = document.getElementById('configModal');
    if (!configModal) return;

    const form = configModal.querySelector('#configForm');

    // --- TEMPLATE DEFINITIONS ---
    // A map to associate section names with their corresponding HTML template IDs
    const sectionTemplates = {
        'components': 'components-template',
        'networks': 'networks-template',
        'groups': 'generic-object-template', // Using a generic one for simple objects
        'prtg': 'generic-object-template',
        'servers': 'generic-object-template'
    };
    
    // --- UTILITY FUNCTIONS ---

    /**
     * Re-indexes the name and id attributes of form elements within a container
     * after an item has been added or removed.
     * @param {HTMLElement} container - The container holding the dynamic items.
     * @param {string} sectionName - The top-level key (e.g., 'components').
     */
    function reindexItems(container, sectionName) {
        const items = container.querySelectorAll(':scope > .dynamic-item, :scope > .dynamic-item-nested');
        items.forEach((item, index) => {
            item.querySelectorAll('input, select, textarea').forEach(input => {
                const name = input.getAttribute('name');
                if (name) {
                    // This regex finds the first index placeholder and replaces it
                    const newName = name.replace(/\[\d+\]/, `[${index}]`);
                    input.setAttribute('name', newName);
                }
            });
        });
    }

    /**
     * Adds a new dynamic item to a container, cloning it from a template.
     * @param {HTMLElement} container - The container to add the item to.
     * @param {string} templateId - The ID of the <template> element to use.
     * @param {string} sectionName - The name of the section (e.g., 'components').
     * @param {object} [data={}] - Optional data to populate the new item's fields.
     */
    function addItem(container, templateId, sectionName, data = {}) {
        const template = document.getElementById(templateId);
        if (!template) return;

        const newItem = template.content.cloneNode(true).firstElementChild;
        const newIndex = container.children.length;

        // Populate fields with data before adding to DOM
        Object.keys(data).forEach(key => {
            const input = newItem.querySelector(`[name$="[${key}]"]`);
            if (input) input.value = data[key];
        });
        
        // Handle generic template where keys are also inputs
        if(templateId === 'generic-object-template') {
            const keyInput = newItem.querySelector('.generic-key-input');
            const valueInput = newItem.querySelector('.generic-value-input');
            if (keyInput && valueInput && data) {
                 const key = Object.keys(data)[0];
                 const value = data[key];
                 if (key) keyInput.value = key;
                 if (value) valueInput.value = JSON.stringify(value); // Stringify complex values
            }
        }

        // Recursively handle nested items (for 'networks' -> 'port_groups')
        if (sectionName === 'networks' && data.port_groups && Array.isArray(data.port_groups)) {
            const nestedContainer = newItem.querySelector('.nested-items-container');
            data.port_groups.forEach(pgData => {
                addNestedItem(nestedContainer, 'port_groups-template', 'networks', newIndex, 'port_groups', pgData);
            });
        }

        // Add remove listener
        newItem.querySelector('.remove-item-btn').addEventListener('click', () => {
            newItem.remove();
            reindexItems(container, sectionName);
        });
        
        // Add listener for inline "Add Port Group" button
        const addNestedBtn = newItem.querySelector('.add-nested-item-btn');
        if (addNestedBtn) {
            addNestedBtn.addEventListener('click', (e) => {
                const nestedContainer = e.target.previousElementSibling;
                addNestedItem(nestedContainer, e.target.dataset.templateId, 'networks', newIndex, 'port_groups');
            });
        }
        
        container.appendChild(newItem);
        reindexItems(container, sectionName); // Re-index after adding
    }
    
    /**
     * Adds a nested item (like a port group inside a network).
     */
    function addNestedItem(container, templateId, parentName, parentIndex, sectionName, data = {}) {
        const template = document.getElementById(templateId);
        if (!template) return;

        const newNestedItem = template.content.cloneNode(true).firstElementChild;
        const newIndex = container.children.length;
        
        newNestedItem.querySelectorAll('input, select, textarea').forEach(input => {
            let name = input.getAttribute('name');
            if(name) {
                name = name.replace('__PARENT_NAME__', parentName)
                           .replace('__PARENT_INDEX__', parentIndex)
                           .replace('__INDEX__', newIndex);
                input.setAttribute('name', name);
            }
        });

        Object.keys(data).forEach(key => {
            const input = newNestedItem.querySelector(`[name$="[${key}]"]`);
            if (input) input.value = data[key];
        });

        newNestedItem.querySelector('.remove-item-btn').addEventListener('click', () => {
            newNestedItem.remove();
            // Re-indexing nested items is more complex but can be done if needed
        });

        container.appendChild(newNestedItem);
    }
    
    // --- MODAL EVENT LISTENERS ---

    configModal.addEventListener('show.bs.modal', function(event) {
        const button = event.relatedTarget;
        const mode = button.dataset.mode; // 'add' or 'edit'

        // Reset form and set action URL
        form.reset();
        document.querySelectorAll('.dynamic-items-container').forEach(c => c.innerHTML = '');
        
        if (mode === 'add') {
            form.action = "{{ url_for('settings.add_course_config') }}";
            configModal.querySelector('.modal-title').textContent = 'Add New Course Configuration';
            form.querySelector('#config_id').value = '';
        } else { // 'edit' mode
            form.action = "{{ url_for('settings.update_course_config') }}";
            const configData = JSON.parse(button.dataset.configJson);
            
            configModal.querySelector('.modal-title').textContent = `Edit Course Config: ${configData.course_name}`;
            form.querySelector('#config_id').value = configData._id;

            // Populate simple fields
            form.querySelector('#course_name').value = configData.course_name || '';
            form.querySelector('#vendor_shortcode').value = configData.vendor_shortcode || '';
            form.querySelector('#alias').value = configData.alias || '';
            form.querySelector('#apm_version').value = configData.apm_version || '';
            form.querySelector('#memory').value = configData.memory !== undefined ? configData.memory : '';
            
            // Populate dynamic sections
            Object.keys(sectionTemplates).forEach(sectionName => {
                const container = document.getElementById(`${sectionName}-container`);
                const templateId = sectionTemplates[sectionName];
                if (container && configData[sectionName] && Array.isArray(configData[sectionName])) {
                    configData[sectionName].forEach(itemData => {
                        addItem(container, templateId, sectionName, itemData);
                    });
                }
            });
        }
    });

    // Add event listeners to all "Add Item" buttons
    document.querySelectorAll('.add-item-btn').forEach(button => {
        button.addEventListener('click', () => {
            const sectionName = button.dataset.sectionName;
            const container = document.getElementById(`${sectionName}-container`);
            const templateId = sectionTemplates[sectionName];
            if(container && templateId) {
                addItem(container, templateId, sectionName);
            }
        });
    });

});