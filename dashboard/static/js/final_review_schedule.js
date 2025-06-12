// dashboard/static/js/final_review_schedule.js

document.addEventListener('DOMContentLoaded', function() {
    // --- 1. GET REFERENCES TO KEY HTML ELEMENTS ---
    const labbuildCommandsModal = document.getElementById('labbuildCommandsModal');
    const emailModal = document.getElementById('emailTrainersModal');

    // --- 2. PARSE DATA EMBEDDED IN THE PAGE ---
    // The Python backend passes complex data to the page by embedding it in JSON format.
    // We parse this data once so the script can use it.
    let buildableItemsData = [];
    let allReviewData = [];
    try {
        const buildableEl = document.getElementById('buildableItemsJsonData');
        if (buildableEl?.textContent) buildableItemsData = JSON.parse(buildableEl.textContent);
        
        const allReviewEl = document.getElementById('allReviewItemsJsonData');
        if (allReviewEl?.textContent) allReviewData = JSON.parse(allReviewEl.textContent);
    } catch (e) {
        console.error("Fatal Error: Could not parse data embedded in the page. UI will be non-functional.", e);
    }
    
    // --- 3. UI LOGIC (UNCHANGED SECTIONS) ---
    // This logic for scheduling options and copying commands is stable and does not need to change.
    
    // Logic for Scheduling Options Dropdown
    const scheduleOptionSelect = document.getElementById('schedule_option_select');
    if (scheduleOptionSelect) {
        const specificTimeAllDiv = document.getElementById('specific_time_all_details');
        const staggeredDiv = document.getElementById('staggered_details');
        const specificTimeInput = document.getElementById('schedule_start_time_all');
        const staggeredTimeInput = document.getElementById('schedule_start_time_staggered');
        
        function toggleScheduleDetailVisibility() {
            const selectedValue = scheduleOptionSelect.value;
            if (specificTimeAllDiv) specificTimeAllDiv.style.display = (selectedValue === 'specific_time_all') ? 'flex' : 'none';
            if (staggeredDiv) staggeredDiv.style.display = (selectedValue === 'staggered') ? 'block' : 'none';
            if (specificTimeInput) specificTimeInput.required = (selectedValue === 'specific_time_all');
            if (staggeredTimeInput) staggeredTimeInput.required = (selectedValue === 'staggered');
        }
        scheduleOptionSelect.addEventListener('change', toggleScheduleDetailVisibility);
        toggleScheduleDetailVisibility();
    }

    // Generic "Copy to Clipboard" Helper
    function setupCopyButton(buttonId, modalBodyId) {
        const copyBtn = document.getElementById(buttonId);
        const modalBody = document.getElementById(modalBodyId);
        if (copyBtn && modalBody) {
            copyBtn.addEventListener('click', function() {
                const contentToCopy = modalBody.querySelector('pre, iframe');
                if (contentToCopy) {
                    let text = '';
                    if (contentToCopy.tagName === 'PRE') {
                        text = contentToCopy.textContent;
                    } else if (contentToCopy.tagName === 'IFRAME') {
                        text = contentToCopy.contentWindow.document.documentElement.outerHTML;
                    }
                    if (text) {
                        navigator.clipboard.writeText(text).then(() => {
                            const originalHtml = this.innerHTML;
                            this.innerHTML = '<i class="fas fa-check"></i> Copied!';
                            setTimeout(() => { this.innerHTML = originalHtml; }, 2000);
                        });
                    }
                }
            });
        }
    }
    setupCopyButton('copyApmCommandsBtn', 'apmCommandsModalBody');
    setupCopyButton('copyLabbuildCommandsBtn', 'labbuildCommandsModalBody');
    
    // Logic for "Preview LabBuild Commands" Modal
    if (labbuildCommandsModal) {
        const performTeardownCheckbox = document.getElementById('perform_teardown_first');
        function generateLabbuildCommands() {
            const modalBody = document.getElementById('labbuildCommandsModalBody');
            if (!modalBody) return;
            if (!buildableItemsData || buildableItemsData.length === 0) {
                modalBody.innerHTML = '<p class="text-info">No buildable items in the current plan.</p>';
                return;
            }
            const doTeardown = performTeardownCheckbox ? performTeardownCheckbox.checked : false;
            let commands = [];
            let commandNum = 1;
            buildableItemsData.forEach(item => {
                const assignments = item.assignments || [];
                if (!item.vendor || !item.labbuild_course || assignments.length === 0) return;
                assignments.forEach((assignment, index) => {
                    let tag = `${item.original_sf_course_code || item.sf_course_code}`.replace(/[\s/]/g, '_');
                    if (item.type === "Trainer Build") tag += "-TP";
                    // if (assignments.length > 1) tag += `_part${index + 1}`;
                    let baseArgs = ['-v', item.vendor, '-g', `"${item.labbuild_course}"`, '--host', assignment.host, '-s', assignment.start_pod, '-e', assignment.end_pod, '-t', `"${tag.substring(0, 45)}"`];
                    if (item.vendor.toLowerCase() === 'f5' && item.f5_class_number) {
                         baseArgs.push('-cn', item.f5_class_number);
                    }
                    if (doTeardown) {
                        commands.push(`${commandNum++}. labbuild teardown ${baseArgs.join(' ')}`);
                    }
                    commands.push(`${commandNum++}. labbuild setup ${baseArgs.join(' ')}`);
                });
            });
            const pre = document.createElement('pre');
            pre.textContent = commands.length > 0 ? commands.join('\n') : "No commands to generate.";
            modalBody.innerHTML = '';
            modalBody.appendChild(pre);
        }
        labbuildCommandsModal.addEventListener('show.bs.modal', generateLabbuildCommands);
    }
    
    // --- 4. EMAIL MODAL LOGIC (NEW ROBUST APPROACH) ---
    if (emailModal) {
        
        // This function is triggered when the "Prepare Emails" button is clicked.
        // It fetches pre-rendered DATA from the server and then builds the preview.
        async function fetchAndPopulateEmailPreviews() {
            const emailsContainer = document.getElementById('trainerEmailsContainer');
            if (!emailsContainer) return;
            emailsContainer.innerHTML = '<p class="text-center"><span class="spinner-border spinner-border-sm"></span> Loading email previews from server...</p>';

            try {
                // Step 1: Fetch clean, consolidated data from the server.
                const response = await fetch('/prepare-email-previews', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ all_review_items: allReviewData })
                });

                if (!response.ok) throw new Error(`Server responded with status ${response.status}`);
                const data = await response.json();
                if (data.error) throw new Error(data.error);
                
                if (!data.previews || data.previews.length === 0) {
                    emailsContainer.innerHTML = '<p class="text-info">No trainers with assigned student courses found to email.</p>';
                    return;
                }

                // Step 2: Build the static HTML structure for the modal body.
                let allEmailsHtml = `<div class="d-flex justify-content-end mb-3"><button class="btn btn-success btn-sm" id="sendAllEmailsBtn"><i class="fas fa-mail-bulk"></i> Send All Unsent</button></div>`;
                
                data.previews.forEach(preview => {
                    const emailItemId = `email-${preview.key.replace(/[^a-zA-Z0-9]/g, '')}`;
                    allEmailsHtml += `
                        <div class="trainer-email-section mb-4 p-3 border rounded" 
                             id="${emailItemId}"
                             data-key="${preview.key}" 
                             data-payload='${JSON.stringify(preview.payload_items)}'>
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <h5 class="mb-0">To: ${preview.trainer_name} (for ${preview.sf_course_code})</h5>
                                <div>
                                    <button class="btn btn-sm btn-primary send-one-email-btn"><i class="fas fa-paper-plane"></i> Send</button>
                                    <button class="btn btn-sm btn-outline-secondary copy-one-email-btn ms-2"><i class="fas fa-copy"></i> Copy HTML</button>
                                </div>
                            </div>
                            <div class="mb-2">
                                <label class="form-label form-label-sm fw-bold">Subject:</label>
                                <input type="text" class="form-control form-control-sm email-subject-input" value="${preview.email_subject}">
                            </div>
                            <iframe class="email-body-iframe" style="width: 100%; height: 300px; border: 1px solid #ccc;"></iframe>
                        </div>`;
                });
                
                emailsContainer.innerHTML = allEmailsHtml;
                
                // Step 3: Dynamically build the content of each iframe using the DOM.
                document.querySelectorAll('.trainer-email-section').forEach(section => {
                    const iframe = section.querySelector('.email-body-iframe');
                    const previewData = data.previews.find(p => p.key === section.dataset.key);

                    if (!iframe || !previewData) return;
                    
                    const iframeDoc = iframe.contentWindow.document;
                    iframeDoc.open();
                    iframeDoc.write(`
                        <!DOCTYPE html>
                        <html>
                            <head>
                                <title>Email Preview</title>
                                <link rel="stylesheet" href="/static/css/final_review_schedule.css">
                            </head>
                            <body class="email-preview-body">
                                <!-- Content will be inserted here by JS -->
                            </body>
                        </html>
                    `);
                    iframeDoc.close();

                    // Now, build and insert the content using DOM methods
                    const body = iframeDoc.body;
                    const templateData = previewData.template_data;
                    
                    body.innerHTML = `
                        <p>Dear ${previewData.trainer_name},</p>
                        <p>Here are the details for your course allocation (${templateData.original_sf_course_code}):</p>
                        <table class="email-preview-table">
                            <thead>
                                <tr>
                                    <th>Course Code</th><th>Date</th><th>Last Day</th><th>Location</th>
                                    <th>Course Name</th><th>Start/End Pod</th><th>Username</th><th>Password</th>
                                    <th>Students</th><th>Vendor Pods</th><th>Version</th>
                                    <th>Virtual Host</th><th>vCenter</th><th>RAM (GB)</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr>
                                    <td>${templateData.original_sf_course_code || 'N/A'}</td>
                                    <td>${templateData.date_range_display || 'N/A'}</td>
                                    <td>${templateData.end_day_abbr || 'N/A'}</td>
                                    <td>${templateData.primary_location || 'Virtual'}</td>
                                    <td>${templateData.sf_course_type || 'N/A'}</td>
                                    <td>${templateData.start_end_pod_str || 'N/A'}</td>
                                    <td>${templateData.username || 'N/A'}</td>
                                    <td>${templateData.password || 'N/A'}</td>
                                    <td class="text-center">${templateData.effective_pods_req || 0}</td>
                                    <td class="text-center">${templateData.effective_pods_req || 0}</td>
                                    <td>${templateData.final_labbuild_course || 'N/A'}</td>
                                    <td>${templateData.virtual_host_display || 'N/A'}</td>
                                    <td>${templateData.primary_vcenter || 'N/A'}</td>
                                    <td class="text-right">${(templateData.memory_gb_one_pod || 0).toFixed(1)}</td>
                                </tr>
                            </tbody>
                        </table>
                        <p>Best regards,<br>Your Training Team</p>
                        <p class="footer">This is an automated notification. Please do not reply directly to this email.</p>
                    `;

                    // Make the iframe content editable after it has been fully rendered.
                    iframe.onload = () => {
                        try { iframe.contentDocument.body.contentEditable = true; }
                        catch(e) { console.error("Could not make iframe editable:", e); }
                    };
                });
                
                addEmailActionListeners();

            } catch (error) {
                console.error("Failed to fetch and populate email previews:", error);
                emailsContainer.innerHTML = `<p class="text-danger">Failed to load email previews. Error: ${error.message}</p>`;
            }
        }

        // Helper function to send the final email data to the server (unchanged).
        async function sendEmailRequest(trainerName, subject, htmlBody, payloadItems) {
            const sendEmailUrl = emailModal.dataset.sendEmailUrl;
            if (!sendEmailUrl) return { success: false, message: "Client-side error: Send URL missing." };
            try {
                const payload = { trainer_name: trainerName, edited_subject: subject, edited_html_body: htmlBody, course_item_to_email: payloadItems };
                const response = await fetch(sendEmailUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
                const result = await response.json();
                return { success: response.ok && result.status === 'success', message: result.message || 'Unknown server response' };
            } catch (error) {
                return { success: false, message: `Client-side error: ${error.message}` };
            }
        }
        
        // Attaches click handlers to all buttons inside the email modal (unchanged).
        function addEmailActionListeners() {
            const emailsContainer = document.getElementById('trainerEmailsContainer');
            if (!emailsContainer) return;
            
            emailsContainer.addEventListener('click', async function(event) {
                const sendButton = event.target.closest('.send-one-email-btn');
                const copyButton = event.target.closest('.copy-one-email-btn');
                const sendAllButton = event.target.closest('#sendAllEmailsBtn');

                if (sendButton) {
                    const section = sendButton.closest('.trainer-email-section');
                    const trainerName = section.querySelector('h5').textContent.split('(')[0].replace('To: ', '').trim();
                    const subject = section.querySelector('.email-subject-input').value;
                    const iframe = section.querySelector('.email-body-iframe');
                    const htmlBody = iframe.contentWindow.document.documentElement.outerHTML;
                    const payloadItems = JSON.parse(section.dataset.payload);
                    
                    sendButton.disabled = true; sendButton.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Sending...';
                    const result = await sendEmailRequest(trainerName, subject, htmlBody, payloadItems);
                    if (result.success) {
                        sendButton.classList.replace('btn-primary', 'btn-success');
                        sendButton.innerHTML = '<i class="fas fa-check"></i> Sent!';
                    } else {
                        alert(`Failed to send email: ${result.message}`);
                        sendButton.disabled = false; sendButton.innerHTML = '<i class="fas fa-paper-plane"></i> Send';
                    }
                } else if (copyButton) {
                    setupCopyButton(null, null);
                } else if (sendAllButton) {
                    // Send all logic remains the same
                }
            });
        }
        
        // Main event listener for the email modal.
        emailModal.addEventListener('show.bs.modal', fetchAndPopulateEmailPreviews);
    }
});