// In dashboard/static/js/saved_plans.js

document.addEventListener('DOMContentLoaded', function () {
    const scheduleModal = document.getElementById('scheduleExecutionModal');
    if (!scheduleModal) return;

    let commandsToSchedule = [];

    // --- Modal Setup ---
    scheduleModal.addEventListener('show.bs.modal', function (event) {
        const button = event.relatedTarget;
        const batchId = button.dataset.batchId;
        commandsToSchedule = JSON.parse(button.dataset.commandsJson);

        // Populate the modal's hidden input
        scheduleModal.querySelector('#batchIdInput').value = batchId;

        // Set a sensible default start time (2 minutes from now)
        const now = new Date();
        now.setMinutes(now.getMinutes() + 2);
        now.setSeconds(0);
        const localDateTimeString = now.getFullYear() + '-' +
            ('0' + (now.getMonth() + 1)).slice(-2) + '-' +
            ('0' + now.getDate()).slice(-2) + 'T' +
            ('0' + now.getHours()).slice(-2) + ':' +
            ('0' + now.getMinutes()).slice(-2);
        scheduleModal.querySelector('#scheduleStartTime').value = localDateTimeString;
        
        // Initial schedule calculation and preview
        calculateAndPreviewSchedule();
    });

    // --- Recalculate Preview on Change ---
    const scheduleInputs = scheduleModal.querySelectorAll('#scheduleStartTime, #concurrencyLevel, #staggerMinutes');
    scheduleInputs.forEach(input => {
        input.addEventListener('change', calculateAndPreviewSchedule);
    });

    /**
     * Calculates the execution schedule based on user inputs and displays a preview.
     * This is a client-side replica of the backend logic for immediate feedback.
     */
    function calculateAndPreviewSchedule() {
        const previewTbody = scheduleModal.querySelector('#schedulePreviewTbody');
        if (!commandsToSchedule || commandsToSchedule.length === 0) {
            previewTbody.innerHTML = `<tr><td colspan="3" class="text-muted text-center p-3">No commands to schedule.</td></tr>`;
            return;
        }

        const startTimeStr = scheduleModal.querySelector('#scheduleStartTime').value;
        const concurrency = parseInt(scheduleModal.querySelector('#concurrencyLevel').value, 10) || 1;
        const staggerMins = parseInt(scheduleModal.querySelector('#staggerMinutes').value, 10) || 0;

        if (!startTimeStr) {
            previewTbody.innerHTML = `<tr><td colspan="3" class="text-warning text-center p-3">Please select a valid start time.</td></tr>`;
            return;
        }

        const startTime = new Date(startTimeStr);
        const staggerMillis = staggerMins * 60 * 1000;

        // --- The "Smart Stagger" Algorithm ---
        const hostNextAvailableTime = {}; // Tracks when each host is free
        const workerTracks = Array(concurrency).fill(startTime.getTime()); // Tracks when each worker slot is free

        const scheduledCommands = commandsToSchedule.map(command => {
            const host = command.host;
            const hostAvailableAt = hostNextAvailableTime[host] || 0;

            // Find the earliest available worker slot
            workerTracks.sort((a, b) => a - b);
            const earliestWorkerAvailableAt = workerTracks[0];

            // Command can only run when BOTH its host AND a worker are free
            const scheduledRunTimeMillis = Math.max(hostAvailableAt, earliestWorkerAvailableAt);
            const scheduledRunTime = new Date(scheduledRunTimeMillis);

            // The next time this host/worker will be free
            const nextAvailableTime = scheduledRunTimeMillis + staggerMillis;

            // Update the state for the next command
            hostNextAvailableTime[host] = nextAvailableTime;
            workerTracks[0] = nextAvailableTime; // Update the earliest worker track we just used

            return { ...command, scheduledRunTime };
        });

        // --- Display the Preview ---
        const userLocale = navigator.language || 'en-US';
        const timeFormatOptions = { dateStyle: 'short', timeStyle: 'medium', hour12: false };
        let previewHtml = '';

        scheduledCommands.forEach(cmd => {
            const timeStr = new Intl.DateTimeFormat(userLocale, timeFormatOptions).format(cmd.scheduledRunTime);
            previewHtml += `
                <tr>
                    <td class="col-time">${timeStr}</td>
                    <td class="col-host"><code>${cmd.host}</code></td>
                    <td class="col-command">${cmd.cli_command}</td>
                </tr>
            `;
        });
        
        previewTbody.innerHTML = previewHtml;
    }
});