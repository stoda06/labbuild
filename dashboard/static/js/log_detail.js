// --- START OF dashboard/static/js/log_detail.js ---

document.addEventListener('DOMContentLoaded', () => {
    // --- Get Elements and Initial Data (Unchanged) ---
    const summaryCard = document.getElementById('runSummaryCard');
    const logOutputElement = document.getElementById('detailed-log-output');
    const logCountBadge = document.getElementById('logCountBadge');
    const sseStatusElement = document.getElementById('sse-status');
    let noLogsPlaceholder = document.getElementById('no-logs-placeholder');
    const runId = summaryCard ? summaryCard.dataset.runId : null;
    const sseEnabled = summaryCard ? summaryCard.dataset.sseEnabled === 'true' : false;
    const initialLogCount = logOutputElement ? parseInt(logOutputElement.dataset.initialLogCount || '0', 10) : 0;
  
    // --- State Variables (Unchanged) ---
    let eventSource = null;
    let currentLogCount = initialLogCount;
    let statusCheckInterval = null;
    const statusCheckFrequency = 10000; // ms
    const finalStates = ['completed', 'completed_with_errors', 'failed', 'failed_exception', 'terminated_by_user', 'unknown'];
  
    if (!runId || !logOutputElement) {
      console.error("Essential data/elements missing. Log streaming disabled.");
      if (sseStatusElement) { sseStatusElement.textContent = "Initialization error."; }
      return;
    }
  
    // --- Utility Functions (Unchanged - Keep formatLocalDateTime, updateRenderedTimes, updateLogCount, escapeHtml) ---
    function formatLocalDateTime(isoString) {
      if (!isoString) { return 'N/A'; }
      try {
        const date = new Date(isoString);
        if (isNaN(date.getTime())) { console.warn(`Invalid date: ${isoString}`); return 'Invalid Date';}
        return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
      } catch (e) { console.error("formatLocalDateTime error:", isoString, e); return 'Invalid Date'; }
    }
  
    function updateRenderedTimes() {
      logOutputElement.querySelectorAll('.local-datetime:not(.js-formatted-time)').forEach(span => {
        const isoTimestamp = span.dataset.timestamp;
        if (isoTimestamp && typeof isoTimestamp === 'string') { span.textContent = formatLocalDateTime(isoTimestamp); }
        else if (!span.textContent) { span.textContent = 'No Timestamp'; }
        span.classList.add('js-formatted-time');
      });
    }
  
    function updateLogCount() { if (logCountBadge) { logCountBadge.textContent = `Total: ${currentLogCount}`; } }
  
    function escapeHtml(unsafe) {
      if (typeof unsafe !== 'string') { unsafe = String(unsafe); }
      return unsafe.replace(/&/g, "&").replace(/</g, "<").replace(/>/g, ">").replace(/"/g, '"').replace(/'/g, "'");
    }
  
    // --- UPDATED appendLogMessage Function ---
    function appendLogMessage(logData) {
      if (!logOutputElement) { return; }
      if (noLogsPlaceholder) { noLogsPlaceholder.remove(); noLogsPlaceholder = null; }
  
      if (typeof logData !== 'object' || logData === null) {
        console.error("Invalid logData received:", logData);
        logData = { level: 'ERROR', message: '[Received invalid log data format]' };
      }
  
      const level = logData.level || 'UNKNOWN';
      const isError = ['ERROR', 'CRITICAL'].includes(level);
      const isWarning = level === 'WARNING';
      const timestamp = logData.timestamp ? formatLocalDateTime(logData.timestamp) : 'No Timestamp';
      const loggerName = escapeHtml(logData.logger_name || 'unknown');
      const message = escapeHtml(logData.message || '');
  
      // Create the main log line container
      const lineDiv = document.createElement('div');
      lineDiv.className = `log-line log-level-${level.toLowerCase()}`;
      if (isError) { lineDiv.classList.add('log-error'); }
      if (isWarning) { lineDiv.classList.add('log-warning'); }
  
      // Create and append spans for each part
      const timeSpan = document.createElement('span');
      timeSpan.className = 'log-timestamp local-datetime js-formatted-time'; // Mark as formatted
      timeSpan.dataset.timestamp = logData.timestamp || ""; // Store original ISO if available
      timeSpan.textContent = timestamp;
      lineDiv.appendChild(timeSpan);
  
      const levelSpan = document.createElement('span');
      levelSpan.className = 'log-level';
      levelSpan.textContent = `[${level}]`;
      lineDiv.appendChild(levelSpan);
  
      const loggerSpan = document.createElement('span');
      loggerSpan.className = 'log-logger';
      loggerSpan.textContent = `${loggerName}:`;
      lineDiv.appendChild(loggerSpan);
  
      const messageSpan = document.createElement('span');
      messageSpan.className = 'log-message';
      messageSpan.textContent = message; // Already escaped
      lineDiv.appendChild(messageSpan);
  
      // Append the structured line to the output
      logOutputElement.appendChild(lineDiv);
      currentLogCount++;
      updateLogCount();
  
      // Auto-scroll if near the bottom
      const scrollThreshold = 30;
      const isScrolledBottom = logOutputElement.scrollHeight - logOutputElement.clientHeight <= logOutputElement.scrollTop + scrollThreshold;
      if (isScrolledBottom) {
        logOutputElement.scrollTop = logOutputElement.scrollHeight;
      }
    } // End of appendLogMessage
  
    // --- SSE Connection Logic (Unchanged - Keep connectSSE, closeSSE) ---
    function connectSSE() {
      if (!sseEnabled) { console.warn("SSE disabled."); if (sseStatusElement) sseStatusElement.textContent = "Real-time streaming disabled."; return; }
      if (eventSource) { console.warn("SSE connection active."); return; }
      const sseUrl = `/log-stream/${runId}`; eventSource = new EventSource(sseUrl); console.log(`Connecting SSE: ${sseUrl}`); if (sseStatusElement) sseStatusElement.textContent = "Connecting...";
      eventSource.onopen = function() { console.log("SSE opened."); if (sseStatusElement) sseStatusElement.textContent = "Connected."; };
      eventSource.onmessage = function(event) { try { const logData = JSON.parse(event.data); appendLogMessage(logData); } catch (e) { console.error("SSE parse error:", event.data, e); appendLogMessage({ level: 'ERROR', message: `[Invalid Stream]: ${escapeHtml(event.data)}` }); } };
      eventSource.onerror = function(event) { console.error("SSE error:", event); if (sseStatusElement) sseStatusElement.textContent = "Stream error."; if (eventSource.readyState === EventSource.CLOSED) { console.log("SSE closed."); if (sseStatusElement) sseStatusElement.textContent = "Stream failed."; stopStatusChecks(); eventSource = null; } };
      eventSource.addEventListener('connected', function(event) { console.log("SSE Server connected:", event.data); });
      eventSource.addEventListener('error', function(event) { if (event.data) { console.error("SSE server error:", event.data); appendLogMessage({ level: 'ERROR', message: `STREAM ERROR: ${escapeHtml(event.data)}` }); if (sseStatusElement) sseStatusElement.textContent = `Stream error: ${event.data}`; } else { console.error("SSE general/network error."); } });
    }
    function closeSSE() { if (eventSource) { eventSource.close(); eventSource = null; console.log("SSE closed by client."); if (sseStatusElement) sseStatusElement.textContent = "Stream closed."; } }
  
  
    // --- Status Checking Logic (Unchanged - Keep checkRunStatus, startStatusChecks, stopStatusChecks) ---
     async function checkRunStatus() {
      if (!runId) { stopStatusChecks(); return; }
      try {
        const response = await fetch(`/status/${runId}`);
        if (!response.ok) { console.error(`Status check failed: ${response.status}`); if (response.status === 404) stopStatusChecks(); return; }
        const data = await response.json();
        const statusSpan=document.getElementById('summary-status'); const endSpan=document.getElementById('summary-end-time'); const durationSpan=document.getElementById('summary-duration'); const successSpan=document.getElementById('summary-success'); const failureSpan=document.getElementById('summary-failure');
        if(statusSpan){statusSpan.textContent=data.overall_status; statusSpan.className=`status-${data.overall_status.toLowerCase().replace(/ /g, '_')}`;}
        if(endSpan){const fmtEnd=formatLocalDateTime(data.end_time_iso); if(endSpan.textContent!==fmtEnd){endSpan.dataset.timestamp=data.end_time_iso||""; endSpan.textContent=fmtEnd; endSpan.classList.add('js-formatted-time');}}
        if(durationSpan){durationSpan.textContent=data.duration_seconds?data.duration_seconds.toFixed(2)+'s':'N/A';}
        if(successSpan){successSpan.textContent=data.summary?data.summary.success_count:'?';}
        if(failureSpan){failureSpan.textContent=data.summary?data.summary.failure_count:'?';}
        if (finalStates.includes(data.overall_status)) { console.log(`Run finished (${data.overall_status}). Closing stream.`); closeSSE(); stopStatusChecks(); if (sseStatusElement) sseStatusElement.textContent = `Stream finished (${data.overall_status}).`; }
      } catch (error) { console.error("Status check error:", error); }
    }
    function startStatusChecks() { if (statusCheckInterval) return; checkRunStatus(); statusCheckInterval = setInterval(checkRunStatus, statusCheckFrequency); console.log("Started status checks."); }
    function stopStatusChecks() { if (statusCheckInterval) { clearInterval(statusCheckInterval); statusCheckInterval = null; console.log("Stopped status checks."); } }
  
  
    // --- Initialization (Unchanged) ---
    updateRenderedTimes();
    updateLogCount();
    const initialStatusElem = summaryCard ? document.getElementById('summary-status') : null;
    const initialStatus = initialStatusElem ? initialStatusElem.textContent.trim() : null;
    if (initialStatus && !finalStates.includes(initialStatus)) { console.log(`Initial status '${initialStatus}'. Connect SSE & check status.`); connectSSE(); startStatusChecks(); }
    else if (initialStatus) { const msg = `Run already finished (${initialStatus}). No live logs.`; if (sseStatusElement) sseStatusElement.textContent = msg; console.log(msg); }
    else { const msg = "Run status unknown. Check status periodically."; if (sseStatusElement) sseStatusElement.textContent = msg; console.warn(msg); startStatusChecks(); }
    window.addEventListener('beforeunload', () => { closeSSE(); stopStatusChecks(); });
  
  }); // End DOMContentLoaded
  
  // --- END OF FILE dashboard/static/js/log_detail.js ---