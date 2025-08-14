/**
 * Manages the "Copy to Clipboard" functionality for a button and a text source.
 */
class ClipboardManager {
    /**
     * @param {string} buttonId The ID of the trigger button.
     * @param {string} sourceElementId The ID of the element containing the text to copy.
     */
    constructor(buttonId, sourceElementId) {
        this.copyButton = document.getElementById(buttonId);
        this.sourceElement = document.getElementById(sourceElementId);

        if (!this.copyButton || !this.sourceElement) {
            console.warn(`ClipboardManager setup failed: Button or source element not found for buttonId '${buttonId}'.`);
            return;
        }
        this._attachListener();
    }

    /**
     * Attaches the click event listener to the copy button.
     * @private
     */
    _attachListener() {
        this.copyButton.addEventListener('click', () => this._copyText());
    }

    /**
     * Handles the logic of reading text and writing it to the clipboard,
     * including providing visual feedback to the user.
     * @private
     */
    _copyText() {
        const textToCopy = this.sourceElement.textContent || '';
        if (textToCopy.trim() === '') return;

        navigator.clipboard.writeText(textToCopy).then(() => {
            this._provideFeedback(true);
        }).catch(err => {
            console.error('Failed to copy text: ', err);
            this._provideFeedback(false);
        });
    }

    /**
     * Provides visual feedback on the button after a copy attempt.
     * @param {boolean} success - Whether the copy operation was successful.
     * @private
     */
    _provideFeedback(success) {
        const originalHtml = this.copyButton.innerHTML;
        const feedbackClass = success ? 'btn-success' : 'btn-danger';
        const feedbackIcon = success ? 'fa-check' : 'fa-times';
        const feedbackText = success ? 'Copied!' : 'Failed';

        this.copyButton.innerHTML = `<i class="fas ${feedbackIcon}"></i> ${feedbackText}`;
        this.copyButton.classList.add(feedbackClass);
        this.copyButton.classList.remove('btn-outline-secondary');

        setTimeout(() => {
            this.copyButton.innerHTML = originalHtml;
            this.copyButton.classList.remove(feedbackClass);
            this.copyButton.classList.add('btn-outline-secondary');
        }, 2000);
    }
}

/**
 * Manages all interactions within the "Email Previews" tab.
 */
class EmailPreviewManager {
    /**
     * @param {string} containerId The ID of the main container for the email tab.
     */
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        if (!this.container) {
            console.warn("EmailPreviewManager: Container element not found. Manager will not be active.");
            return;
        }
        this.sendRealUrl = this.container.dataset.sendRealUrl;
        this.sendTestUrl = this.container.dataset.sendTestUrl;
        this._initialize();
    }

    /**
     * Sets up initial state and event listeners for the email tab.
     * @private
     */
    _initialize() {
        this.container.querySelectorAll('.email-body-iframe').forEach(iframe => {
            iframe.onload = () => {
                try {
                    iframe.contentDocument.body.contentEditable = true;
                } catch (e) {
                    console.warn("Could not make iframe editable:", e);
                }
            };
        });
        this.container.addEventListener('click', (event) => this._handleEmailActions(event));
    }

    /**
     * Central event handler for all clicks within the email tab.
     * @param {Event} event The click event.
     * @private
     */
    async _handleEmailActions(event) {
        const testBtn = event.target.closest('.send-test-email-btn');
        const sendBtn = event.target.closest('.send-one-email-btn');
        const sendAllTestsBtn = event.target.closest('#sendAllTestsBtn');
        const sendAllRealBtn = event.target.closest('#sendAllEmailsBtn');
        
        if (testBtn) {
            await this._sendEmailRequest(this.sendTestUrl, testBtn);
        } else if (sendBtn) {
            await this._sendEmailRequest(this.sendRealUrl, sendBtn);
        } else if (sendAllTestsBtn) {
            await this._handleSendAll(true);
        } else if (sendAllRealBtn) {
            await this._handleSendAll(false);
        }
    }

    async _handleSendAll(isTest) {
        const buttonSelector = isTest ? '.send-test-email-btn:not(:disabled)' : '.send-one-email-btn:not(:disabled)';
        const endpointUrl = isTest ? this.sendTestUrl : this.sendRealUrl;
        const actionName = isTest ? "test emails" : "REAL emails";
        const allUnsentButtons = this.container.querySelectorAll(buttonSelector);
        
        if (allUnsentButtons.length === 0) {
            return alert(`All ${actionName} have already been sent or are in progress.`);
        }
        if (!confirm(`Are you sure you want to send all ${allUnsentButtons.length} unsent ${actionName}?`)) {
            return;
        }
        
        for (const button of allUnsentButtons) {
            await this._sendEmailRequest(endpointUrl, button);
        }
        alert(`Finished sending all ${actionName}. Check individual button statuses.`);
    }

    /**
     * Performs the AJAX request to send a single email and provides UI feedback.
     * @param {string} endpointUrl - The API endpoint to call.
     * @param {HTMLButtonElement} button - The button that was clicked.
     * @private
     */
    async _sendEmailRequest(endpointUrl, button) {
        const btnGroup = button.closest('.btn-group');
        const section = button.closest('.email-preview-section');
        if (!btnGroup || !section || !endpointUrl) {
            console.error("Could not find required elements or URL for email action.");
            return;
        }
        
        const trainerName = btnGroup.dataset.trainerName;
        const coursePayload = JSON.parse(btnGroup.dataset.coursePayload);
        const subject = section.querySelector('.email-subject-input').value;
        const iframe = section.querySelector('.email-body-iframe');
        const htmlBody = iframe?.contentWindow?.document?.documentElement?.outerHTML || '';

        if (!trainerName || !subject || !htmlBody || !coursePayload) {
            alert("Client-side validation failed: Missing data for email.");
            return;
        }

        button.disabled = true;
        const originalHtml = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
        
        // highlight-start
        // --- MODIFICATION: Exhaustive Logging and Error Handling ---
        console.log(`[EMAIL ACTION] Preparing to send email. Endpoint: ${endpointUrl}`);
        const payload = {
            trainer_name: trainerName,
            edited_subject: subject,
            edited_html_body: htmlBody,
            course_item_to_email: coursePayload
        };
        console.log('[EMAIL ACTION] Payload to be sent:', payload);

        try {
            const response = await fetch(endpointUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            console.log('[EMAIL ACTION] Received response from server.');
            console.log(`[EMAIL ACTION] Status: ${response.status} (${response.statusText})`);
            console.log('[EMAIL ACTION] Response Headers:', Object.fromEntries(response.headers.entries()));

            const responseText = await response.text();
            console.log(`[EMAIL ACTION] Raw Response Body (as text):\n---\n${responseText}\n---`);

            if (!response.ok) {
                // This handles HTTP errors like 500, 502 (Gateway Timeout), etc.
                const errorMessage = `Server responded with an error: ${response.status} ${response.statusText}. ` +
                                     `Check the Flask/Gunicorn terminal logs for a Python stack trace. ` +
                                     `The response body was: "${responseText}"`;
                console.error(errorMessage);
                throw new Error(errorMessage);
            }

            if (responseText.trim() === '') {
                // This specifically handles the 200 OK with empty body scenario
                const errorMessage = "The server returned a successful (200 OK) response, but the body was empty. " +
                                     "This often means a critical, unhandled error (like a timeout) occurred on the server " +
                                     "before it could generate a JSON response. Check the Gunicorn/Uvicorn logs for worker timeouts.";
                console.error(errorMessage);
                throw new Error(errorMessage);
            }

            // Only attempt to parse if we have a non-empty body and a successful status
            const result = JSON.parse(responseText);
            console.log('[EMAIL ACTION] Parsed JSON result:', result);

            if (result.status !== 'success') {
                throw new Error(result.message || 'The server indicated a failure but provided no message.');
            }

            const successClass = endpointUrl.includes('test') ? 'btn-outline-success' : 'btn-success';
            button.className = `btn btn-sm ${successClass}`;
            button.innerHTML = '<i class="fas fa-check"></i> Sent!';

        } catch (error) {
            // This will catch everything: network failures, parsing failures, and our thrown errors.
            console.error('[EMAIL ACTION] A critical error occurred during the fetch operation:', error);
            alert(`Failed to send email. See the browser's developer console (F12) for detailed error information.`);
            button.disabled = false;
            button.innerHTML = originalHtml;
        }
        // --- END MODIFICATION ---
    }
}



/**
 * Main orchestrator class for the Build Plan Review page.
 */
class BuildPlanReviewPage {
    constructor() {
        this.initialize();
    }

    /**
     * Initializes all interactive components on the page.
     */
    initialize() {
        new ClipboardManager('copyApmCommandsBtn', 'apmCommandsOutput');
        new ClipboardManager('copyLabbuildCommandsBtn', 'labbuildCommandsOutput');
        new EmailPreviewManager('emailPreviewTab');
        this._initializeScheduleOptions();
    }
    
    /**
     * Sets up the event listener for the scheduling options dropdown.
     * @private
     */
    _initializeScheduleOptions() {
        const scheduleOptionSelect = document.getElementById('schedule_option');
        const startTimeWrapper = document.getElementById('start_time_wrapper');
        const startTimeInput = document.getElementById('schedule_start_time');
    
        if (scheduleOptionSelect && startTimeWrapper && startTimeInput) {
            scheduleOptionSelect.addEventListener('change', function() {
                if (this.value === 'specific_time') {
                    startTimeWrapper.style.display = 'block';
                    startTimeInput.required = true;
                } else {
                    startTimeWrapper.style.display = 'none';
                    startTimeInput.required = false;
                    startTimeInput.value = ''; // Clear the value if hidden
                }
            });
        }
    }
}

// --- Entry Point ---
document.addEventListener('DOMContentLoaded', () => {
    new BuildPlanReviewPage();
});