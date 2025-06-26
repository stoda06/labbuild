// dashboard/static/js/final_review_schedule.js

document.addEventListener('DOMContentLoaded', function() {
    const labbuildCommandsModal = document.getElementById('labbuildCommandsModal');
    const emailModal = document.getElementById('emailTrainersModal');
    const scheduleOptionSelect = document.getElementById('schedule_option_select');

    let buildableItemsData = [];
    let allReviewData = [];

    try {
        const buildableEl = document.getElementById('buildableItemsJsonData');
        if (buildableEl?.textContent) {
            buildableItemsData = JSON.parse(buildableEl.textContent);
        }
        
        const allReviewEl = document.getElementById('allReviewItemsJsonData');
        if (allReviewEl?.textContent) {
            allReviewData = JSON.parse(allReviewEl.textContent);
        }
    } catch (e) {
        console.error("Fatal Error: Could not parse data embedded in the page. UI will be non-functional.", e);
        return;
    }
    
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
                
                assignments.forEach((assignment) => {
                    const isStudent = item.type === 'Student Build';
                    let tag = `${item.original_sf_course_code || item.sf_course_code}`.replace(/[\s/]/g, '_');
                    if (!isStudent) tag += "-TP";
                    
                    let baseArgs = [
                        '-v', item.vendor,
                        '-g', `"${item.labbuild_course}"`,
                        '--host', assignment.host,
                        '-s', assignment.start_pod,
                        '-e', assignment.end_pod,
                        '-t', `"${tag.substring(0, 45)}"`
                    ];

                    if (item.vendor.toLowerCase() === 'f5' && item.f5_class_number) {
                         baseArgs.push('-cn', item.f5_class_number);
                    }
                    
                    const optionalArgs = [];
                    if (item.start_date) optionalArgs.push('--start-date', `"${item.start_date}"`);
                    if (item.end_date) optionalArgs.push('--end-date', `"${item.end_date}"`);
                    if (item.sf_trainer_name) optionalArgs.push('--trainer-name', `"${item.sf_trainer_name}"`);
                    
                    // --- THIS IS THE CORRECTED LOGIC ---
                    // Select the correct set of APM credentials based on the build type
                    const apmUser = isStudent ? item.apm_username : item.trainer_apm_username;
                    const apmPass = isStudent ? item.apm_password : item.trainer_apm_password;

                    if (apmUser) optionalArgs.push('--username', `"${apmUser}"`);
                    if (apmPass) optionalArgs.push('--password', `"${apmPass}"`);
                    // --- END OF CORRECTION ---

                    if (doTeardown) {
                        commands.push(`${commandNum++}. labbuild teardown ${baseArgs.join(' ')}`);
                    }
                    commands.push(`${commandNum++}. labbuild setup ${baseArgs.join(' ')} ${optionalArgs.join(' ')}`);
                });
            });
            
            const pre = document.createElement('pre');
            pre.textContent = commands.length > 0 ? commands.join('\n') : "No commands to generate.";
            modalBody.innerHTML = '';
            modalBody.appendChild(pre);
        }
        
        labbuildCommandsModal.addEventListener('show.bs.modal', generateLabbuildCommands);
        if(performTeardownCheckbox) {
            performTeardownCheckbox.addEventListener('change', generateLabbuildCommands);
        }
    }
    
    if (emailModal) {
        async function fetchAndPopulateEmailPreviews() {
            const emailsContainer = document.getElementById('trainerEmailsContainer');
            if (!emailsContainer) return;
            emailsContainer.innerHTML = '<p class="text-center"><span class="spinner-border spinner-border-sm"></span> Loading email previews from server...</p>';

            const prepareUrl = emailModal.dataset.preparePreviewsUrl;
            if (!prepareUrl) {
                emailsContainer.innerHTML = '<p class="text-danger">Error: The URL for preparing email previews is missing.</p>';
                return;
            }

            try {
                const response = await fetch(prepareUrl, {
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

                let allEmailsHtml = `<div class="d-flex justify-content-end mb-3"><button class="btn btn-warning btn-sm me-2" id="sendAllTestsBtn"><i class="fas fa-vial"></i> Send All Tests</button><button class="btn btn-success btn-sm" id="sendAllEmailsBtn"><i class="fas fa-mail-bulk"></i> Send All Real</button></div>`;
                data.previews.forEach(preview => {
                    allEmailsHtml += `
                        <div class="trainer-email-section mb-4 p-3 border rounded" data-key="${preview.key}" data-payload='${JSON.stringify(preview.payload_items)}'>
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <h5 class="mb-0">To: ${preview.trainer_name} (for ${preview.sf_course_code})</h5>
                                <div class="btn-group">
                                    <button class="btn btn-sm btn-outline-warning send-test-email-btn"><i class="fas fa-vial"></i> Test</button>
                                    <button class="btn btn-sm btn-primary send-one-email-btn"><i class="fas fa-paper-plane"></i> Send</button>
                                </div>
                            </div>
                            <div class="mb-2">
                                <label class="form-label form-label-sm fw-bold">Subject:</label>
                                <input type="text" class="form-control form-control-sm email-subject-input" value="${preview.email_subject}">
                            </div>
                            <iframe class="email-body-iframe" style="width: 100%; height: 350px; border: 1px solid #ccc;"></iframe>
                        </div>`;
                });
                emailsContainer.innerHTML = allEmailsHtml;
                
                document.querySelectorAll('.trainer-email-section').forEach(section => {
                    const iframe = section.querySelector('.email-body-iframe');
                    const previewData = data.previews.find(p => p.key === section.dataset.key);
                    if (!iframe || !previewData) return;
                    
                    const iframeDoc = iframe.contentWindow.document;
                    iframeDoc.open();
                    
                    const emailStyles = `body{font-family:Arial,Helvetica,sans-serif;font-size:10pt;color:#333;margin:10px}table{border-collapse:collapse;width:100%;margin-bottom:15px;border:1px solid #ccc}th,td{border:1px solid #ddd;padding:8px;text-align:left;vertical-align:top;white-space:nowrap}th{background-color:#f0f0f0;font-weight:700}p{margin-bottom:10px;line-height:1.5}.footer{font-size:9pt;color:#777;margin-top:20px}.text-center{text-align:center}.text-right{text-align:right}`;
                    
                    iframeDoc.write(`<!DOCTYPE html><html><head><title>Email Preview</title><style>${emailStyles}</style></head><body></body></html>`);
                    iframeDoc.close();

                    const body = iframeDoc.body;
                    const templateData = previewData.template_data;
                    const nl2br = (str) => (str || '').toString().replace(/\n/g, '<br>');
                    
                    const labAccessMessage = templateData.lab_access_url_message || '';
                    
                    body.innerHTML = `
                        <p>Dear ${previewData.trainer_name},</p>
                        <p>Here are the details for your course allocation (${templateData.original_sf_course_code}):</p>
                        ${labAccessMessage ? `<p><strong>${labAccessMessage}</strong></p>` : ''}
                        <table>
                            <thead><tr><th>Course Code</th><th>Date</th><th>Last Day</th><th>Location</th><th>Course Name</th><th>Start/End Pod</th><th>Username</th><th>Password</th><th>Students</th><th>Vendor Pods</th><th>Version</th><th>Virtual Host</th><th>vCenter</th><th>RAM (GB)</th></tr></thead>
                            <tbody>
                                <tr>
                                    <td>${templateData.original_sf_course_code}</td><td>${templateData.date_range_display}</td><td>${templateData.end_day_abbr}</td>
                                    <td>${templateData.primary_location}</td><td>${templateData.sf_course_type}</td><td>${nl2br(templateData.start_end_pod_str)}</td>
                                    <td>${templateData.username}</td><td>${templateData.password}</td><td class="text-center">${templateData.effective_pods_req}</td>
                                    <td class="text-center">${templateData.effective_pods_req}</td><td>${templateData.final_labbuild_course}</td>
                                    <td class="virtual-host-cell">${nl2br(templateData.virtual_host_display)}</td><td class="vcenter-cell">${nl2br(templateData.primary_vcenter)}</td>
                                    <td class="text-right">${templateData.total_ram_for_course.toFixed(1)}</td>
                                </tr>
                            </tbody>
                        </table>
                        <p>Best regards,<br>Your Training Team</p>
                        <p class="footer">This is an automated notification. Please do not reply directly to this email.</p>
                    `;
                    iframe.onload = () => { try { iframe.contentDocument.body.contentEditable = true; } catch(e) {} };
                });
                
                addEmailActionListeners();
            } catch (error) {
                console.error("Failed to fetch and populate email previews:", error);
                emailsContainer.innerHTML = `<p class="text-danger">Failed to load email previews. Error: ${error.message}</p>`;
            }
        }

        async function sendEmailRequest(endpointUrl, button, trainerName, subject, htmlBody, payloadItems) {
            button.disabled = true;
            const originalHtml = button.innerHTML;
            button.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Sending...';
            
            if (!trainerName || !subject || !htmlBody || !payloadItems || payloadItems.length === 0) {
                alert("Client-side validation failed: Missing required data to send email.");
                console.error("sendEmailRequest validation failed:", { trainerName, subject, htmlBody, payloadItems });
                button.disabled = false;
                button.innerHTML = originalHtml;
                return;
            }

            try {
                const payload = { 
                    trainer_name: trainerName, 
                    edited_subject: subject, 
                    edited_html_body: htmlBody, 
                    course_item_to_email: payloadItems 
                };
                const response = await fetch(endpointUrl, { 
                    method: 'POST', 
                    headers: { 'Content-Type': 'application/json' }, 
                    body: JSON.stringify(payload) 
                });
                const result = await response.json();
                if (response.ok && result.status === 'success') {
                    const successClass = endpointUrl.includes('test') ? 'btn-outline-success' : 'btn-success';
                    button.className = `btn btn-sm ${successClass}`;
                    button.innerHTML = '<i class="fas fa-check"></i> Sent!';
                } else {
                    throw new Error(result.message || 'Unknown server error');
                }
            } catch (error) {
                alert(`Failed to send email: ${error.message}`);
                button.disabled = false;
                button.innerHTML = originalHtml;
            }
        }
        
        function addEmailActionListeners() {
            const emailsContainer = document.getElementById('trainerEmailsContainer');
            if (!emailsContainer) return;
            
            emailsContainer.addEventListener('click', async function(event) {
                const realSendBtn = event.target.closest('.send-one-email-btn');
                const testSendBtn = event.target.closest('.send-test-email-btn');
                const sendAllRealBtn = event.target.closest('#sendAllEmailsBtn');
                const sendAllTestsBtn = event.target.closest('#sendAllTestsBtn');

                if (realSendBtn || testSendBtn) {
                    const button = realSendBtn || testSendBtn;
                    const isTest = !!testSendBtn;
                    const endpoint = isTest ? "/email-actions/send-test-email" : "/email-actions/send-trainer-email";
                    const section = button.closest('.trainer-email-section');
                    const trainerName = section.querySelector('h5').textContent.split('(')[0].replace('To: ', '').trim();
                    const subject = section.querySelector('.email-subject-input').value;
                    const iframe = section.querySelector('.email-body-iframe');
                    const payloadItems = JSON.parse(section.dataset.payload);
                    let htmlBody = '';
                    if (iframe?.contentWindow?.document?.documentElement) {
                        htmlBody = iframe.contentWindow.document.documentElement.outerHTML;
                    }
                    if (!htmlBody) {
                        alert("Error: Could not read the email preview content.");
                        return;
                    }
                    await sendEmailRequest(endpoint, button, trainerName, subject, htmlBody, payloadItems);
                
                } else if (sendAllRealBtn || sendAllTestsBtn) {
                    const isTest = !!sendAllTestsBtn;
                    const buttonToClickSelector = isTest ? '.send-test-email-btn:not(.btn-success)' : '.send-one-email-btn:not(.btn-success)';
                    const actionName = isTest ? "test emails" : "real emails";
                    const bulkButton = sendAllRealBtn || sendAllTestsBtn;

                    const allUnsentButtons = emailsContainer.querySelectorAll(buttonToClickSelector);
                    if (allUnsentButtons.length === 0) { alert(`All ${actionName} have already been sent or are in progress.`); return; }
                    if (!confirm(`Are you sure you want to send all ${allUnsentButtons.length} unsent ${actionName}?`)) return;
                    
                    const originalAllBtnHtml = bulkButton.innerHTML;
                    bulkButton.disabled = true;
                    bulkButton.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Sending All...`;
                    for (let i = 0; i < allUnsentButtons.length; i++) {
                        await allUnsentButtons[i].click();
                    }
                    bulkButton.disabled = false;
                    bulkButton.innerHTML = originalAllBtnHtml;
                    alert(`Bulk send process for ${actionName} finished. Check individual button statuses.`);
                }
            });
        }
        
        emailModal.addEventListener('show.bs.modal', fetchAndPopulateEmailPreviews);
    }
});