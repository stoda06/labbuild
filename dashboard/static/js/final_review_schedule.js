// dashboard/static/js/final_review_schedule.js

document.addEventListener('DOMContentLoaded', function() {
    // --- Scheduling Options UI ---
    const scheduleOptionSelect = document.getElementById('schedule_option_select');
    const specificTimeAllDiv = document.getElementById('specific_time_all_details');
    const specificTimeAllInput = document.getElementById('schedule_start_time_all');
    const staggeredDiv = document.getElementById('staggered_details');
    const staggeredTimeInput = document.getElementById('schedule_start_time_staggered');
    const staggeredMinutesInput = document.getElementById('schedule_stagger_minutes');

    function toggleScheduleDetailVisibility() {
        if (!scheduleOptionSelect || !specificTimeAllDiv || !staggeredDiv) return;
        const selectedValue = scheduleOptionSelect.value;
        specificTimeAllDiv.style.display = 'none';
        staggeredDiv.style.display = 'none';
        if (specificTimeAllInput) {
            specificTimeAllInput.required = (selectedValue === 'specific_time_all');
            if (selectedValue !== 'specific_time_all') specificTimeAllInput.value = '';
        }
        if (staggeredTimeInput) {
            staggeredTimeInput.required = (selectedValue === 'staggered');
            if (selectedValue !== 'staggered') staggeredTimeInput.value = '';
        }
        if (staggeredMinutesInput) {
             staggeredMinutesInput.required = (selectedValue === 'staggered');
             if (selectedValue !== 'staggered') staggeredMinutesInput.value = '30';
        }
        if (selectedValue === 'specific_time_all') specificTimeAllDiv.style.display = 'block';
        else if (selectedValue === 'staggered') staggeredDiv.style.display = 'block';
    }
    if(scheduleOptionSelect) {
        scheduleOptionSelect.addEventListener('change', toggleScheduleDetailVisibility);
        toggleScheduleDetailVisibility(); // Initial call
    }

    // --- Format Dates ---
    if (typeof updateLocalTimes === "function") {
        updateLocalTimes();
    } else {
        console.warn("final_review_schedule.js: updateLocalTimes function not found (expected from base.html). Dates may not display in local time.");
    }

    // --- Form Submission Validation (for scheduling builds) ---
    const executeForm = document.getElementById('executeBuildsForm');
    if (executeForm) {
        executeForm.addEventListener('submit', function(event) {
            const buildPlanInput = document.getElementById('confirmedBuildPlanInput');
            let buildableItemsCount = 0;
            try {
                if (buildPlanInput && buildPlanInput.value) {
                    buildableItemsCount = JSON.parse(buildPlanInput.value).length;
                }
            } catch (e) { console.error("Error parsing build plan data on submit:", e); }

            if (buildableItemsCount === 0) {
                const tableBody = document.getElementById('finalReviewTableBody');
                const hasAnyRenderedItems = tableBody && tableBody.rows.length > 0 &&
                                          (tableBody.rows[0].cells.length > 1 ||
                                           !/No items to display|No items found/i.test(tableBody.rows[0].cells[0].textContent));
                if(!hasAnyRenderedItems) {
                    alert("No items in the plan to schedule.");
                    event.preventDefault();
                }
            }
        });
    }

    // --- Trainer Email Modal JavaScript ---
    const emailTrainersModalEl = document.getElementById('emailTrainersModal');
    const trainerEmailsContainer = document.getElementById('trainerEmailsContainer');
    const allReviewItemsJsonDataEl = document.getElementById('allReviewItemsJsonData');
    let allReviewItemsDataForEmail = [];
    let sendEmailUrl = '';

    if (emailTrainersModalEl) {
        sendEmailUrl = emailTrainersModalEl.dataset.sendEmailUrl;
        if (!sendEmailUrl) {
            console.error("Email Modal JS: Send email URL not found in modal data attribute. Emails cannot be sent.");
        }
    } else {
        console.warn("Email Modal JS: Modal element #emailTrainersModal not found.");
    }

    if (allReviewItemsJsonDataEl && allReviewItemsJsonDataEl.textContent) {
        try {
            allReviewItemsDataForEmail = JSON.parse(allReviewItemsJsonDataEl.textContent);
        }
        catch (e) { console.error("Email Modal JS: Error parsing all_review_items_json:", e); }
    } else {
        console.warn("Email Modal JS: Script tag #allReviewItemsJsonData not found or empty.");
    }

    const prepareEmailsButton = document.querySelector('button[data-bs-target="#emailTrainersModal"]');
    if (prepareEmailsButton) {
        prepareEmailsButton.addEventListener('click', function() {
            populateTrainerEmailsForModal();
        });
    } else {
        console.warn("Email Modal JS: 'Prepare Emails' button not found.");
    }

    function getDayAbbreviation(dateString) {
        if (!dateString || dateString === "N/A" || dateString === "Ongoing (Extended)") return dateString;
        try {
            let dateObj = new Date(dateString + 'T00:00:00Z');
            if (isNaN(dateObj.getTime())) { dateObj = new Date(dateString); }
            if (isNaN(dateObj.getTime())) return dateString;
            return dateObj.toLocaleDateString('en-US', { weekday: 'short', timeZone: 'UTC' });
        } catch (e) {
            console.warn("Email Modal JS: Could not parse date for day abbreviation:", dateString, e);
            return dateString;
        }
    }

    function escapeHtml(unsafe) {
        if (typeof unsafe !== 'string') {
            if (unsafe === null || unsafe === undefined) return '';
            unsafe = String(unsafe);
        }
        return unsafe.replace(/&/g, "&").replace(/</g, "<").replace(/>/g, ">").replace(/"/g, '"').replace(/'/g, "'");
    }

    function populateTrainerEmailsForModal() {
        if (!trainerEmailsContainer) { console.error("Email Modal JS: trainerEmailsContainer not found!"); return; }
        trainerEmailsContainer.innerHTML = '<p class="text-center"><span class="spinner-border spinner-border-sm"></span> Loading email previews...</p>';

        if (!allReviewItemsDataForEmail || allReviewItemsDataForEmail.length === 0) {
            trainerEmailsContainer.innerHTML = '<p class="text-warning">No build plan data available to generate emails.</p>';
            return;
        }

        const studentCoursesRelevantForEmail = allReviewItemsDataForEmail.filter(item =>
            item.type === "Student Build" && item.sf_trainer_name && item.sf_trainer_name !== 'N/A'
        );

        if (studentCoursesRelevantForEmail.length === 0) {
            trainerEmailsContainer.innerHTML = '<p class="text-info">No student courses with assigned trainers found in this plan to email.</p>';
            return;
        }

        const emailsDataGrouped = new Map();
        studentCoursesRelevantForEmail.forEach(courseItem => {
            const trainer = courseItem.sf_trainer_name;
            const originalSfCode = courseItem.original_sf_course_code || courseItem.sf_course_code; // Group by original SF code
            if (!emailsDataGrouped.has(trainer)) { emailsDataGrouped.set(trainer, new Map()); }
            if (!emailsDataGrouped.get(trainer).has(originalSfCode)) { emailsDataGrouped.get(trainer).set(originalSfCode, []); }
            emailsDataGrouped.get(trainer).get(originalSfCode).push(courseItem); // Add the whole item
        });

        if (emailsDataGrouped.size === 0) {
            trainerEmailsContainer.innerHTML = '<p class="text-info">No trainers found to email.</p>'; return;
        }

        let allEmailsHtml = `<div class="d-flex justify-content-end mb-3">
                                <button class="btn btn-success btn-sm" id="sendAllPreviewedEmailsBtn" title="Send all currently displayed and editable emails">
                                    <i class="fas fa-mail-bulk"></i> Send All Previewed Emails
                                </button>
                             </div>`;

        emailsDataGrouped.forEach((coursesBySfCodeMap, trainerName) => {
            coursesBySfCodeMap.forEach((courseItemsForThisSfCode, sfCodeForEmail) => {
                // courseItemsForThisSfCode is now a list of student build items (including Maestro components)
                // for this specific trainer and original SF course code.
                
                const emailItemId = `email-${trainerName.replace(/\s+/g, '_')}-${sfCodeForEmail.replace(/\W/g, '')}`;
                let emailSectionTitle = `To: ${trainerName} (for SF Course: ${sfCodeForEmail})`;

                let emailHtml = `<div class="trainer-email-section mb-4 p-3 border rounded" id="${emailItemId}-section" data-email-item-id="${emailItemId}" data-sf-course-code="${sfCodeForEmail}">`;
                emailHtml += `<div class="d-flex justify-content-between align-items-center mb-2">
                                <h5 class="mb-0">${emailSectionTitle}</h5>
                                <div>
                                    <button class="btn btn-sm btn-primary send-one-email-btn"
                                            data-email-item-id="${emailItemId}"
                                            data-trainer-name="${trainerName}"
                                            data-sf-course-code-for-send="${sfCodeForEmail}"
                                            title="Send this specific email to ${trainerName}">
                                        <i class="fas fa-paper-plane"></i> Send
                                    </button>
                                    <button class="btn btn-sm btn-outline-secondary copy-one-email-btn ms-2"
                                            data-email-item-id="${emailItemId}" title="Copy this email content as HTML">
                                        <i class="fas fa-copy"></i> Copy HTML
                                    </button>
                                </div>
                              </div>`;
                
                const emailSubject = `Lab Allocation for ${sfCodeForEmail}`;
                emailHtml += `<div class="mb-2">
                                <label for="${emailItemId}-subject" class="form-label form-label-sm fw-bold">Subject:</label>
                                <input type="text" class="form-control form-control-sm email-subject-input" id="${emailItemId}-subject" value="${escapeHtml(emailSubject)}">
                              </div><hr>`;
                emailHtml += `<label for="${emailItemId}-body-iframe" class="form-label form-label-sm fw-bold">Body (HTML - Live Editable Preview):</label>
                              <iframe class="email-body-iframe" id="${emailItemId}-body-iframe" style="width: 100%; height: 350px; border: 1px solid #ccc;" data-initial-html=""></iframe>
                              </div>`;

                let singleSfCourseEmailBodyHTML = `<html><head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"><title>${escapeHtml(emailSubject)}</title><style>body {font-family: Arial, Helvetica, sans-serif; font-size: 10pt; color: #333333;} table {border-collapse: collapse; width: 100%; margin-bottom: 15px; border: 1px solid #cccccc;} th, td {border: 1px solid #dddddd; text-align: left; padding: 8px; vertical-align: top;} th {background-color: #f0f0f0; font-weight: bold; color: #333333;} p {margin-bottom: 10px; line-height: 1.5;} .footer {font-size: 9pt; color: #777777; margin-top: 20px;}</style></head><body>`;
                singleSfCourseEmailBodyHTML += `<p>Dear ${trainerName},</p><p>Here are the details for your course allocation (${sfCodeForEmail}):</p>`;
                singleSfCourseEmailBodyHTML += `<table><thead><tr><th>Course Code</th><th>Date</th><th>Last Day</th><th>Location</th><th>Course Name</th><th>Start/End Pod</th><th>Username</th><th>Password</th><th>Students</th><th>Vendor Pods</th><th>Version</th><th>Virtual Host</th><th>vCenter</th><th>RAM (GB)</th></tr></thead><tbody>`;

                courseItemsForThisSfCode.forEach(item => { // Iterate through each component/course for this email
                    const studentPax = item.sf_pax_count || 'N/A';
                    const assignments = item.assignments || [];
                    let startEndPodStr = "N/A", vendorPodsCount = 0, firstPodNum = null;
                    let primaryHostName = "N/A", primaryLocation = "Virtual", primaryVCenter = "N/A";
                    if (assignments.length > 0) {
                        const podRanges = assignments.map(a => `${a.start_pod}-${a.end_pod}`); startEndPodStr = podRanges.join(', ');
                        vendorPodsCount = assignments.reduce((sum, a) => { const s=parseInt(a.start_pod,10),e=parseInt(a.end_pod,10); return sum + (isNaN(s)||isNaN(e)||e<s?0:(e-s+1));},0);
                        firstPodNum = assignments[0].start_pod; primaryHostName = assignments[0].host||"N/A";
                        const hostInfo = (item.host_details_for_assignments||[]).find(hd=>hd.name===primaryHostName)||{};
                        primaryLocation=hostInfo.location||"Virtual"; primaryVCenter=hostInfo.vcenter||"N/A";
                    }
                    
                    // ** Use APM username and password from the item **
                    const apmUsername = item.apm_username || `lab${(item.vendor || "xx").toLowerCase()}-X`;
                    const apmPassword = item.apm_password || "UseProvidedPassword";

                    const startDateFormatted = getDayAbbreviation(item.start_date);
                    const endDateFormatted = getDayAbbreviation(item.end_date);
                    const dateRange = (startDateFormatted===endDateFormatted||!item.end_date||item.end_date==="N/A")?startDateFormatted:`${startDateFormatted}-${endDateFormatted}`;
                    
                    // ** Use sf_course_type for "Course Name" column **
                    // For Maestro components, item.labbuild_course is the component name (e.g., maestro-r81)
                    // For standard courses, item.labbuild_course is the main LabBuild course.
                    // The email should show sf_course_type for the "Course Name" column.
                    // If it's a Maestro component, we might want to show the component's LabBuild name instead of repeating SF Course Type.
                    let courseNameForEmailColumn = item.sf_course_type || item.labbuild_course || "N/A";
                    if (item.is_maestro_course && item.is_maestro_component && item.maestro_component_name) { // item.is_maestro_component was set in Python
                        courseNameForEmailColumn = item.maestro_component_name; // Display component name for Maestro parts
                    } else if (item.is_maestro_course) { // Top-level Maestro item (if not split for email rows)
                        courseNameForEmailColumn = item.sf_course_type || "Maestro Course";
                    }


                    // Version column still uses the specific LabBuild course name for this row
                    const versionForEmail = item.labbuild_course || "N/A";


                    singleSfCourseEmailBodyHTML += `<tr>
                                        <td>${escapeHtml(sfCodeForEmail)}</td><td>${escapeHtml(dateRange)}</td><td>${escapeHtml(endDateFormatted)}</td>
                                        <td>${escapeHtml(primaryLocation)}</td>
                                        <td>${escapeHtml(courseNameForEmailColumn)}</td>
                                        <td>${escapeHtml(startEndPodStr)}</td>
                                        <td>${escapeHtml(apmUsername)}</td>
                                        <td>${escapeHtml(apmPassword)}</td>
                                        <td style="text-align:center;">${escapeHtml(studentPax)}</td>
                                        <td style="text-align:center;">${escapeHtml(vendorPodsCount)}</td>
                                        <td>${escapeHtml(versionForEmail)}</td>
                                        <td>${escapeHtml(primaryHostName)}${vendorPodsCount>0?` (${escapeHtml(startEndPodStr)})`:''}</td>
                                        <td>${escapeHtml(primaryVCenter)}</td>
                                        <td style="text-align:right;">${escapeHtml((item.memory_gb_one_pod || 0).toFixed(1))}</td>
                                      </tr>`;
                });
                singleSfCourseEmailBodyHTML += `</tbody></table><p>Best regards,<br>Your Training Team</p><p class="footer">This is an automated notification. Please do not reply directly to this email.</p></body></html>`;
                
                const tempDiv = document.createElement('div'); tempDiv.innerHTML = emailHtml;
                const iframeElement = tempDiv.querySelector(`#${emailItemId}-body-iframe`);
                if (iframeElement) { iframeElement.setAttribute('data-initial-html', singleSfCourseEmailBodyHTML); }
                allEmailsHtml += tempDiv.innerHTML;
            });
        });

        trainerEmailsContainer.innerHTML = allEmailsHtml || '<p class="text-info">No student courses with trainers assigned.</p>';
        
        document.querySelectorAll('.email-body-iframe').forEach(iframe => {
            const initialHtml = iframe.dataset.initialHtml;
            if (initialHtml) {
                iframe.srcdoc = initialHtml;
                iframe.onload = function() {
                    try {
                        if (iframe.contentWindow && iframe.contentWindow.document) {
                            iframe.contentWindow.document.body.contentEditable = "true";
                            const styleTag = iframe.contentWindow.document.createElement('style');
                            styleTag.textContent = `body {font-family: Arial,sans-serif;font-size:10pt;margin:8px} table{border-collapse:collapse;width:100%;margin-bottom:15px;border:1px solid #ccc} th,td{border:1px solid #ddd;padding:8px;text-align:left;vertical-align:top} th{background-color:#f0f0f0;font-weight:bold}`;
                            iframe.contentWindow.document.head.appendChild(styleTag);
                        } else { console.warn("Could not access iframe contentWindow for editing:", iframe.id); }
                    } catch (e) { console.error("Error making iframe editable:", iframe.id, e); }
                };
            }
        });
        addEmailActionListenersForEach();
    }

    async function sendSingleEmailLogic(trainerName, emailItemId, subject, htmlBody, courseItemsForPayloadList) {
        if (!sendEmailUrl) {
            console.error("Email Modal JS: Cannot send email, backend URL is missing.");
            return { success: false, message: "Client-side configuration error: Send URL missing." };
        }
        try {
            const payload = {
                trainer_name: trainerName,
                edited_subject: subject,
                edited_html_body: htmlBody,
                course_item_to_email: courseItemsForPayloadList // This is the LIST of items for THIS email
            };
            const response = await fetch(sendEmailUrl, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const contentType = response.headers.get("content-type");
            if (contentType && contentType.indexOf("application/json") !== -1) {
                const result = await response.json();
                return { success: (response.ok && result.status === 'success'), message: result.message || 'Unknown server response', result: result };
            } else {
                const textResponse = await response.text();
                console.error("Email Modal JS: Received non-JSON response from server:", textResponse);
                return { success: false, message: `Server error: Expected JSON but received ${contentType}. Status: ${response.status}` };
            }
        } catch (error) {
            console.error('Email Modal JS: Client-side error sending email request:', error);
            return { success: false, message: `Client-side error: ${error.message}` };
        }
    }

    function addEmailActionListenersForEach() {
        document.querySelectorAll('.copy-one-email-btn').forEach(button => {
            button.addEventListener('click', function() {
                const emailItemId = this.dataset.emailItemId;
                const iframe = document.getElementById(`${emailItemId}-body-iframe`);
                if (iframe && iframe.contentWindow) {
                    const htmlContent = iframe.contentWindow.document.documentElement.outerHTML;
                    navigator.clipboard.writeText(htmlContent).then(() => {
                        button.innerHTML = '<i class="fas fa-check"></i> Copied HTML!';
                        setTimeout(() => { button.innerHTML = '<i class="fas fa-copy"></i> Copy HTML'; }, 2000);
                    }).catch(err => {
                        console.error('Failed to copy HTML from iframe: ', err);
                        button.textContent = 'Copy Fail';
                    });
                }
            });
        });

        document.querySelectorAll('.send-one-email-btn').forEach(button => {
            button.addEventListener('click', async function() {
                if (!sendEmailUrl) { alert("Cannot send email: Client Configuration error (URL missing)."); return; }

                const trainerName = this.dataset.trainerName;
                const emailItemId = this.dataset.emailItemId;
                const sfCourseCodeForThisEmail = this.dataset.sfCourseCodeForSend;

                const subjectInput = document.getElementById(`${emailItemId}-subject`);
                const iframe = document.getElementById(`${emailItemId}-body-iframe`);
                if (!subjectInput || !iframe || !iframe.contentWindow) { alert("Error: Email components not found."); return; }
                const subject = subjectInput.value;
                const htmlBody = iframe.contentWindow.document.documentElement.outerHTML;

                // This list contains all "Student Build" items (including Maestro components)
                // for this specific trainer and this specific original SF course code.
                const courseItemsPayloadForThisEmail = allReviewItemsDataForEmail.filter(item =>
                    item.type === "Student Build" &&
                    item.sf_trainer_name === trainerName &&
                    (item.original_sf_course_code === sfCourseCodeForThisEmail || item.sf_course_code === sfCourseCodeForThisEmail)
                );

                if (courseItemsPayloadForThisEmail.length === 0) {
                    alert(`Could not find original course data items for trainer ${trainerName} and SF Course ${sfCourseCodeForThisEmail}.`);
                    return;
                }

                const originalButtonHtml = this.innerHTML;
                this.disabled = true; this.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Sending...';
                const sendResult = await sendSingleEmailLogic(trainerName, emailItemId, subject, htmlBody, courseItemsPayloadForThisEmail);
                
                if (sendResult.success) {
                    this.classList.remove('btn-primary'); this.classList.add('btn-success');
                    this.innerHTML = '<i class="fas fa-check"></i> Sent!';
                } else {
                    alert(`Failed for ${subject}: ${sendResult.message || 'Err'}`);
                    this.disabled = false; this.innerHTML = originalButtonHtml;
                }
            });
        });

        const sendAllBtn = document.getElementById('sendAllPreviewedEmailsBtn');
        if (sendAllBtn) {
            sendAllBtn.addEventListener('click', async function() {
                if (!sendEmailUrl) { alert("Cannot send emails: Client Configuration error (URL missing)."); return; }

                const emailSections = document.querySelectorAll('.trainer-email-section');
                if (emailSections.length === 0) { alert("No emails to send."); return; }
                if (!confirm(`Send all ${emailSections.length} previewed emails?`)) return;
                this.disabled = true; this.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Sending All (${emailSections.length})...`;
                let successCount = 0; let failCount = 0;

                for (const section of emailSections) {
                    const sendButton = section.querySelector('.send-one-email-btn');
                    if (!sendButton || sendButton.classList.contains('btn-success')) { // Already successfully sent or no button
                        if(sendButton && sendButton.classList.contains('btn-success')) successCount++;
                        continue;
                    }
                    const trainerName = sendButton.dataset.trainerName;
                    const emailItemId = sendButton.dataset.emailItemId;
                    const sfCourseCodeForThisEmailAll = sendButton.dataset.sfCourseCodeForSend;
                    const subjectInput = document.getElementById(`${emailItemId}-subject`);
                    const iframe = document.getElementById(`${emailItemId}-body-iframe`);
                    if (!subjectInput || !iframe || !iframe.contentWindow) { failCount++; console.warn(`Skipping ${emailItemId}: missing elements.`); continue; }
                    
                    const courseItemsPayloadForThisEmailAll = allReviewItemsDataForEmail.filter(item =>
                        item.type === "Student Build" &&
                        item.sf_trainer_name === trainerName &&
                        (item.original_sf_course_code === sfCourseCodeForThisEmailAll || item.sf_course_code === sfCourseCodeForThisEmailAll)
                    );
                    if (courseItemsPayloadForThisEmailAll.length === 0) { failCount++; console.warn(`Skipping ${emailItemId}: no corresponding items for bulk send.`); continue;}

                    sendButton.disabled = true; sendButton.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
                    const sendResult = await sendSingleEmailLogic(trainerName, emailItemId, subjectInput.value, iframe.contentWindow.document.documentElement.outerHTML, courseItemsPayloadForThisEmailAll);
                    if (sendResult.success) { sendButton.classList.remove('btn-primary','btn-danger'); sendButton.classList.add('btn-success'); sendButton.innerHTML='<i class="fas fa-check"></i> Sent'; successCount++; }
                    else { sendButton.classList.remove('btn-primary','btn-success'); sendButton.classList.add('btn-danger'); sendButton.innerHTML='<i class="fas fa-times"></i> Fail'; failCount++; }
                }
                this.disabled = false; this.innerHTML = '<i class="fas fa-mail-bulk"></i> Send All Previewed Emails';
                alert(`Bulk send attempt complete. Success: ${successCount}, Failed: ${failCount}.`);
            });
        }
    }
    // --- APM Commands Modal JavaScript ---
    const generateApmCommandsBtn = document.getElementById('generateApmCommandsBtn');
    const apmLoadingIndicator = document.getElementById('apmLoadingIndicator');
    const apmCommandsModalEl = document.getElementById('apmCommandsModal');
    const apmCommandsModalBody = document.getElementById('apmCommandsModalBody');
    const copyApmCommandsBtn = document.getElementById('copyApmCommandsBtn');
    let apmModalInstance = null;
    let apmCommandsUrl = '';

    if (generateApmCommandsBtn) {
        apmCommandsUrl = generateApmCommandsBtn.dataset.apmCommandsUrl;
        if (!apmCommandsUrl) {
            console.error("APM Commands JS: URL not found in button data attribute.");
            generateApmCommandsBtn.disabled = true;
        }
    }

    if (apmCommandsModalEl) {
        apmModalInstance = new bootstrap.Modal(apmCommandsModalEl);
    }

    if (generateApmCommandsBtn) {
        generateApmCommandsBtn.addEventListener('click', async function() {
            if (!apmCommandsUrl) {
                alert("Cannot generate APM commands: Client configuration error (URL missing).");
                if (apmCommandsModalBody) apmCommandsModalBody.innerHTML = '<p class="text-danger">Error: Client configuration error (URL missing).</p>';
                if (apmModalInstance) apmModalInstance.show();
                return;
            }

            if (apmLoadingIndicator) apmLoadingIndicator.style.display = 'inline-block';
            this.disabled = true;
            if (apmCommandsModalBody) apmCommandsModalBody.innerHTML = '<p class="text-center"><span class="spinner-border spinner-border-sm"></span> Generating APM commands...</p>';
            if (apmModalInstance) apmModalInstance.show();

            const batchReviewIdInput = document.querySelector('#executeBuildsForm input[name="batch_review_id"]');
            if (!batchReviewIdInput || !batchReviewIdInput.value) {
                if (apmCommandsModalBody) apmCommandsModalBody.innerHTML = '<p class="text-danger">Error: Batch Review ID not found.</p>';
                if (apmLoadingIndicator) apmLoadingIndicator.style.display = 'none';
                this.disabled = false;
                return;
            }
            const batchId = batchReviewIdInput.value;

            try {
                const response = await fetch(apmCommandsUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ batch_review_id: batchId })
                });

                if (!response.ok) {
                    let errorMsg = `Server error: ${response.status} ${response.statusText}`;
                    try { const errData = await response.json(); errorMsg = errData.error || errData.message || errorMsg; }
                    catch (e) { const textError = await response.text(); errorMsg = textError || errorMsg; }
                    throw new Error(errorMsg);
                }
                const data = await response.json();
                if (apmCommandsModalBody) {
                    if (data.commands && data.commands.length > 0) {
                        const pre = document.createElement('pre'); pre.textContent = data.commands.join('\n');
                        apmCommandsModalBody.innerHTML = ''; apmCommandsModalBody.appendChild(pre);
                    } else { apmCommandsModalBody.innerHTML = `<p class="text-info">${data.message || 'No APM commands generated.'}</p>`; }
                    if (data.error && data.message && data.commands && data.commands.length > 0 && data.commands[0].startsWith("# Errors occurred")) {}
                    else if (data.error && data.message) { const eDiv = document.createElement('div'); eDiv.className='alert alert-warning mt-2'; eDiv.textContent=data.message; apmCommandsModalBody.appendChild(eDiv); }
                }
            } catch (error) {
                console.error("Error generating APM commands:", error);
                if (apmCommandsModalBody) apmCommandsModalBody.innerHTML = `<p class="text-danger">Failed to generate APM commands: ${error.message}</p>`;
            } finally {
                if (apmLoadingIndicator) apmLoadingIndicator.style.display = 'none';
                this.disabled = false;
            }
        });
    }

    if (copyApmCommandsBtn && apmCommandsModalBody) {
        copyApmCommandsBtn.addEventListener('click', function() {
            const preElement = apmCommandsModalBody.querySelector('pre');
            if (preElement && preElement.textContent) {
                navigator.clipboard.writeText(preElement.textContent).then(() => {
                    this.innerHTML = '<i class="fas fa-check"></i> Copied!';
                    setTimeout(() => { this.innerHTML = '<i class="fas fa-copy"></i> Copy All Commands'; }, 2000);
                }).catch(err => { console.error('Failed to copy APM commands: ', err); alert('Failed to copy commands.'); });
            } else { alert('No commands to copy.'); }
        });
    }
    // --- LabBuild Commands Preview Modal JavaScript ---
    const previewLabbuildCommandsBtn = document.getElementById('previewLabbuildCommandsBtn');
    const labbuildCommandsModalEl = document.getElementById('labbuildCommandsModal');
    const labbuildCommandsModalBody = document.getElementById('labbuildCommandsModalBody');
    const copyLabbuildCommandsBtn = document.getElementById('copyLabbuildCommandsBtn');
    const buildableItemsJsonDataEl = document.getElementById('buildableItemsJsonData'); // Get the buildable items
    const performTeardownCheckbox = document.getElementById('perform_teardown_first');

    let labbuildModalInstance = null;
    let buildableItemsData = [];

    if (labbuildCommandsModalEl) {
        labbuildModalInstance = new bootstrap.Modal(labbuildCommandsModalEl);
    }

    if (buildableItemsJsonDataEl && buildableItemsJsonDataEl.textContent) {
        try {
            buildableItemsData = JSON.parse(buildableItemsJsonDataEl.textContent);
        } catch (e) {
            console.error("LabBuild Commands JS: Error parsing buildableItemsJsonData:", e);
        }
    } else {
        console.warn("LabBuild Commands JS: Script tag #buildableItemsJsonData not found or empty.");
    }


    function generateLabbuildCommands() {
        if (!labbuildCommandsModalBody) return;
        labbuildCommandsModalBody.innerHTML = '<p class="text-center"><span class="spinner-border spinner-border-sm"></span> Generating commands...</p>';
        
        if (!buildableItemsData || buildableItemsData.length === 0) {
            labbuildCommandsModalBody.innerHTML = '<p class="text-info">No buildable items in the current plan to generate commands for.</p>';
            return;
        }

        const doTeardownFirst = performTeardownCheckbox ? performTeardownCheckbox.checked : false;
        let commandsArray = [];
        let commandNumber = 1;

        buildableItemsData.forEach(item => {
            // Each 'item' here is expected to be a student or trainer build item
            // with 'assignments', 'vendor', 'labbuild_course', 'original_sf_course_code' etc.
            const assignments = item.assignments || [];
            const vendor = item.vendor;
            const lbCourse = item.labbuild_course;
            const originalSfCode = item.original_sf_course_code || item.sf_course_code; // Fallback
            const itemTypeSuffix = (item.type === "Trainer Build") ? "-TP" : "";
            
            // Construct a unique tag for this build operation
            // Ensure tag is filesystem-friendly and not too long
            let tag = `${originalSfCode}${itemTypeSuffix}`.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 45);


            if (!vendor || !lbCourse || assignments.length === 0) {
                console.warn("Skipping command generation for item due to missing data:", item);
                return; // Skip this item if essential data is missing
            }

            // Process each assignment block (e.g., host + pod range) for this item
            assignments.forEach((assignment, index) => {
                const host = assignment.host;
                const startPod = assignment.start_pod;
                const endPod = assignment.end_pod;
                let partTag = tag;
                if (assignments.length > 1) { // Add part number if multiple assignments for one item
                    partTag = `${tag}_part${index + 1}`.substring(0,45);
                }


                if (!host || startPod === undefined || endPod === undefined) {
                    console.warn("Skipping assignment segment due to missing host/pods:", assignment);
                    return;
                }

                let baseArgs = [
                    '-v', vendor,
                    '-g', `"${lbCourse}"`, // Quote LabBuild course name if it might have spaces
                    '--host', host,
                    '-s', String(startPod),
                    '-e', String(endPod),
                    '-t', `"${partTag}"` // Quote tag
                ];

                // Add F5 class number if applicable (example: from item.f5_class_number)
                if (vendor && vendor.toLowerCase() === 'f5' && item.f5_class_number) {
                    baseArgs.push('-cn', String(item.f5_class_number));
                }
                // Add other specific flags based on item properties if needed (e.g. memory, clonefrom)
                // Example: if (item.memory_for_setup) baseArgs.push('-mem', String(item.memory_for_setup));


                if (doTeardownFirst) {
                    commandsArray.push(`${commandNumber++}. labbuild teardown ${baseArgs.join(' ')}`);
                }
                commandsArray.push(`${commandNumber++}. labbuild setup ${baseArgs.join(' ')}`);
            });
        });

        if (commandsArray.length > 0) {
            const pre = document.createElement('pre');
            pre.textContent = commandsArray.join('\n');
            labbuildCommandsModalBody.innerHTML = ''; // Clear loading/previous
            labbuildCommandsModalBody.appendChild(pre);
        } else {
            labbuildCommandsModalBody.innerHTML = '<p class="text-info">No LabBuild commands to generate based on the current plan and selections.</p>';
        }
    }


    if (previewLabbuildCommandsBtn) {
        previewLabbuildCommandsBtn.addEventListener('click', function() {
            // Re-parse buildableItemsData in case it was dynamically updated (though unlikely for this page)
            // Or ensure it's fresh if the main plan can change without page reload.
            if (buildableItemsJsonDataEl && buildableItemsJsonDataEl.textContent) {
                try { buildableItemsData = JSON.parse(buildableItemsJsonDataEl.textContent); }
                catch (e) { console.error("Error re-parsing buildableItems for LabBuild preview:", e); }
            }
            generateLabbuildCommands();
            // Bootstrap modal is shown via data-bs-toggle and data-bs-target
        });
    }

    if (copyLabbuildCommandsBtn && labbuildCommandsModalBody) {
        copyLabbuildCommandsBtn.addEventListener('click', function() {
            const preElement = labbuildCommandsModalBody.querySelector('pre');
            if (preElement && preElement.textContent) {
                navigator.clipboard.writeText(preElement.textContent).then(() => {
                    this.innerHTML = '<i class="fas fa-check"></i> Copied!';
                    setTimeout(() => { this.innerHTML = '<i class="fas fa-copy"></i> Copy All Commands'; }, 2000);
                }).catch(err => {
                    console.error('Failed to copy LabBuild commands: ', err);
                    alert('Failed to copy commands. Please try manual selection.');
                });
            } else {
                alert('No commands to copy.');
            }
        });
    }
});