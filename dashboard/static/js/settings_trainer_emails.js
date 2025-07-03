// dashboard/static/js/settings_trainer_emails.js
document.addEventListener('DOMContentLoaded', function() {
    const editTrainerModalElement = document.getElementById('editTrainerModal');
    if (editTrainerModalElement) {
        editTrainerModalElement.addEventListener('show.bs.modal', function(event) {
            const button = event.relatedTarget; // The button that triggered the modal

            // Extract data from the button's data-* attributes
            const trainerId = button.dataset.trainerId;
            const trainerName = button.dataset.name;
            const trainerEmail = button.dataset.email;
            const isActive = button.dataset.active === 'true'; // Convert string to boolean

            // Get the form and its elements inside the modal
            const form = editTrainerModalElement.querySelector('#editTrainerForm');
            const nameInput = editTrainerModalElement.querySelector('#edit_trainer_name');
            const emailInput = editTrainerModalElement.querySelector('#edit_email_address');
            const activeCheckbox = editTrainerModalElement.querySelector('#edit_active');

            // Set the form's action URL dynamically
            form.action = `/settings/trainer-emails/update/${trainerId}`;

            // Populate the form fields with the data
            nameInput.value = trainerName;
            emailInput.value = trainerEmail;
            activeCheckbox.checked = isActive;
        });
    }
});