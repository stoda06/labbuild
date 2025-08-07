document.addEventListener('DOMContentLoaded', function () {
    /**
     * A helper function to attach a "copy to clipboard" functionality to a button.
     * @param {string} buttonId - The ID of the button element.
     * @param {string} sourceElementId - The ID of the element whose text content should be copied.
     */
    function setupCopyButton(buttonId, sourceElementId) {
        const copyButton = document.getElementById(buttonId);
        const sourceElement = document.getElementById(sourceElementId);

        if (!copyButton || !sourceElement) {
            console.warn(`Copy functionality setup failed: Button or source element not found for buttonId '${buttonId}'.`);
            return;
        }

        copyButton.addEventListener('click', function () {
            // Get the text content from the <pre> or <code> block
            const textToCopy = sourceElement.textContent || '';
            
            if (textToCopy.trim() === '') {
                return; // Do nothing if there's no text
            }

            navigator.clipboard.writeText(textToCopy).then(() => {
                // Success feedback
                const originalHtml = this.innerHTML;
                this.innerHTML = '<i class="fas fa-check"></i> Copied!';
                // Use Bootstrap classes for feedback
                this.classList.add('btn-success');
                this.classList.remove('btn-outline-secondary');
                
                setTimeout(() => {
                    this.innerHTML = originalHtml;
                    this.classList.remove('btn-success');
                    this.classList.add('btn-outline-secondary');
                }, 2000);
            }).catch(err => {
                // Error feedback
                console.error('Failed to copy text: ', err);
                const originalHtml = this.innerHTML;
                this.innerHTML = '<i class="fas fa-times"></i> Failed';
                this.classList.add('btn-danger');
                this.classList.remove('btn-outline-secondary');

                setTimeout(() => {
                    this.innerHTML = originalHtml;
                    this.classList.remove('btn-danger');
                    this.classList.add('btn-outline-secondary');
                }, 2000);
            });
        });
    }

    /**
     * Handles the AJAX request for sending a single email (test or real).
     */
    async function sendEmailRequest(endpointUrl, button) {
        const btnGroup = button.closest('.btn-group');
        const section = button.closest('.email-preview-section');
        if (!btnGroup || !section) {
            console.error("Could not find parent elements for email action.");
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

        try {
            const response = await fetch(endpointUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    trainer_name: trainerName,
                    edited_subject: subject,
                    edited_html_body: htmlBody,
                    course_item_to_email: coursePayload
                })
            });
            const result = await response.json();
            if (!response.ok || result.status !== 'success') {
                throw new Error(result.message || 'Unknown server error');
            }

            // Success feedback
            const successClass = endpointUrl.includes('test') ? 'btn-outline-success' : 'btn-success';
            button.className = `btn btn-sm ${successClass}`;
            button.innerHTML = '<i class="fas fa-check"></i> Sent!';
            // Note: We leave the button disabled to prevent re-sending.

        } catch (error) {
            alert(`Failed to send email: ${error.message}`);
            button.disabled = false; // Re-enable on failure
            button.innerHTML = originalHtml;
        }
    }

    // --- Initialize the copy buttons for the APM tab and the LabBuild modal ---
    setupCopyButton('copyApmCommandsBtn', 'apmCommandsOutput');
    setupCopyButton('copyLabbuildCommandsBtn', 'labbuildCommandsOutput');
    // --- Initialize Email Action Buttons ---
    const emailTab = document.getElementById('emailPreviewTab');
    if (emailTab) {
        // Read the correct URLs from the data attributes ---
        const sendRealUrl = emailTab.dataset.sendRealUrl;
        const sendTestUrl = emailTab.dataset.sendTestUrl;

        emailTab.addEventListener('click', async function(event) {
            const testBtn = event.target.closest('.send-test-email-btn');
            const sendBtn = event.target.closest('.send-one-email-btn');
            const sendAllRealBtn = event.target.closest('#sendAllEmailsBtn');
            const sendAllTestsBtn = event.target.closest('#sendAllTestsBtn');

            // Use the URL variables instead of hardcoded strings
            if (testBtn) {
                if (sendTestUrl) await sendEmailRequest(sendTestUrl, testBtn);
            } else if (sendBtn) {
                if (sendRealUrl) await sendEmailRequest(sendRealUrl, sendBtn);
            } else if (sendAllTestsBtn) {
                const allTestButtons = emailTab.querySelectorAll('.send-test-email-btn:not(:disabled)');
                if (allTestButtons.length === 0) return alert("All test emails have already been sent.");
                if (!confirm(`Send all ${allTestButtons.length} test emails?`)) return;
                
                for (const button of allTestButtons) {
                    if (sendTestUrl) await sendEmailRequest(sendTestUrl, button);
                }
                alert("Finished sending all test emails.");
            } else if (sendAllRealBtn) {
                const allRealButtons = emailTab.querySelectorAll('.send-one-email-btn:not(:disabled)');
                if (allRealButtons.length === 0) return alert("All real emails have already been sent.");
                if (!confirm(`ARE YOU SURE you want to send all ${allRealButtons.length} REAL emails?`)) return;
                
                for (const button of allRealButtons) {
                    if (sendRealUrl) await sendEmailRequest(sendRealUrl, button);
                }
                alert("Finished sending all real emails.");
            }
        });
    }
});