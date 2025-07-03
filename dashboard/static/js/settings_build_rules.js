// dashboard/static/js/settings_build_rules.js

document.addEventListener('DOMContentLoaded', function() {
    const ruleModal = document.getElementById('ruleModal');
    if (!ruleModal) return;

    const form = ruleModal.querySelector('#ruleForm');

    ruleModal.addEventListener('show.bs.modal', function(event) {
        const button = event.relatedTarget;
        const mode = button.dataset.mode;

        // Reset the form for both 'add' and 'edit' modes
        form.reset();
        
        if (mode === 'add') {
            ruleModal.querySelector('.modal-title').textContent = 'Add New Build Rule';
            form.action = '/settings/build-rules/add'; // Set action for adding
            form.querySelector('#rule_id').value = '';
        } else { // 'edit' mode
            ruleModal.querySelector('.modal-title').textContent = 'Edit Build Rule';
            const ruleData = JSON.parse(button.dataset.ruleJson);
            
            form.action = `/settings/build-rules/update`; // Update action
            form.querySelector('#rule_id').value = ruleData._id;

            // --- Populate Top-Level Fields ---
            form.querySelector('#rule_name').value = ruleData.rule_name || '';
            form.querySelector('#priority').value = ruleData.priority || 50;
            
            // --- Populate Conditions ---
            const conditions = ruleData.conditions || {};
            form.querySelector('[name="conditions.vendor"]').value = conditions.vendor || '';
            // Join array fields back into CSV strings for the inputs
            form.querySelector('[name="conditions.course_code_contains"]').value = (conditions.course_code_contains || []).join(', ');
            form.querySelector('[name="conditions.course_code_not_contains"]').value = (conditions.course_code_not_contains || []).join(', ');
            form.querySelector('[name="conditions.course_type_contains"]').value = (conditions.course_type_contains || []).join(', ');

            // --- Populate Actions ---
            const actions = ruleData.actions || {};
            form.querySelector('[name="actions.set_labbuild_course"]').value = actions.set_labbuild_course || '';
            form.querySelector('[name="actions.allow_spillover"]').checked = actions.allow_spillover === true;
            form.querySelector('[name="actions.start_pod_number"]').value = actions.start_pod_number || '';
            form.querySelector('[name="actions.set_max_pods"]').value = actions.set_max_pods || '';
            form.querySelector('[name="actions.host_priority"]').value = (actions.host_priority || []).join(', ');

            // --- Populate Nested Action Objects ---
            const calcPods = actions.calculate_pods_from_pax || {};
            const calcPodsEnabled = form.querySelector('#calc_pods_enabled');
            if (Object.keys(calcPods).length > 0) {
                calcPodsEnabled.checked = true;
                form.querySelector('[name="actions.calculate_pods_from_pax.divisor"]').value = calcPods.divisor || '';
                form.querySelector('[name="actions.calculate_pods_from_pax.min_pods"]').value = calcPods.min_pods || '';
                form.querySelector('[name="actions.calculate_pods_from_pax.use_field"]').value = calcPods.use_field || 'pax';
            }

            const maestro = actions.maestro_split_build || {};
            const maestroEnabled = form.querySelector('#maestro_enabled');
            if (Object.keys(maestro).length > 0) {
                maestroEnabled.checked = true;
                form.querySelector('[name="actions.maestro_split_build.main_course"]').value = maestro.main_course || '';
                form.querySelector('[name="actions.maestro_split_build.rack1_course"]').value = maestro.rack1_course || '';
                form.querySelector('[name="actions.maestro_split_build.rack2_course"]').value = maestro.rack2_course || '';
                form.querySelector('[name="actions.maestro_split_build.rack_host"]').value = (maestro.rack_host || []).join(', ');
            }
        }
    });
});