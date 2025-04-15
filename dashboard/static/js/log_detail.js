// --- START OF FILE dashboard/static/js/log_detail.js ---

document.addEventListener('DOMContentLoaded', () => {
  // --- Get Elements and Initial Data ---
  const summaryCard = document.getElementById('runSummaryCard');
  const logOutputElement = document.getElementById('detailed-log-output');
  const logCountBadge = document.getElementById('logCountBadge');
  const sseStatusElement = document.getElementById('sse-status');
  let noLogsPlaceholder = document.getElementById('no-logs-placeholder');

  // Read data passed from HTML via data-* attributes
  const runId = summaryCard ? summaryCard.dataset.runId : null;
  const sseEnabled = summaryCard ? summaryCard.dataset.sseEnabled === 'true' : false;
  const initialLogCount = logOutputElement ? parseInt(logOutputElement.dataset.initialLogCount || '0', 10) : 0;

  // --- State Variables ---
  let eventSource = null;
  let currentLogCount = initialLogCount; // Initialize with historical count
  let statusCheckInterval = null;
  const statusCheckFrequency = 10000; // ms
  const finalStates = [
    'completed', 'completed_with_errors', 'failed',
    'failed_exception', 'terminated_by_user', 'unknown'
  ];

  // Exit early if essential elements/data are missing
  if (!runId || !logOutputElement) {
    console.error("Essential data (runId) or elements (log output) missing. Log streaming disabled.");
    if (sseStatusElement) { sseStatusElement.textContent = "Initialization error."; }
    return; // Stop execution if runId or output area isn't found
  }

  // --- Utility Functions ---
  function formatLocalDateTime(isoString) {
    if (!isoString) { return 'N/A'; }
    try {
      const date = new Date(isoString);
      if (isNaN(date.getTime())) {
        console.warn(`Invalid date string encountered: ${isoString}`);
        return 'Invalid Date';
      }
      // Use default browser locale and options for simplicity
      return date.toLocaleString(undefined, {
        dateStyle: 'medium', timeStyle: 'medium'
      });
    } catch (e) {
      console.error("formatLocalDateTime error:", isoString, e);
      return 'Invalid Date';
    }
  }

  function updateRenderedTimes() {
    // Update any timestamps that haven't been formatted yet
    // Check within the specific container to avoid reformatting unnecessarily
    logOutputElement.querySelectorAll('.local-datetime:not(.js-formatted-time)').forEach(span => {
      const isoTimestamp = span.dataset.timestamp;
      if (isoTimestamp && typeof isoTimestamp === 'string') {
        span.textContent = formatLocalDateTime(isoTimestamp);
      } else if (!span.textContent) {
        span.textContent = 'No Timestamp';
      }
      span.classList.add('js-formatted-time'); // Mark as formatted
    });
  }

  function updateLogCount() {
    if (logCountBadge) {
      logCountBadge.textContent = `Total: ${currentLogCount}`;
    }
  }

  function escapeHtml(unsafe) {
    if (typeof unsafe !== 'string') {
        unsafe = String(unsafe); // Convert numbers, etc., to string
    }
    // Basic escaping for common HTML characters
    return unsafe
      .replace(/&/g, "&")
      .replace(/</g, "<")
      .replace(/>/g, ">")
      // --- FIX: Replace double quote with " ---
      .replace(/"/g, '"')
      .replace(/'/g, "'");
  } // End of escapeHtml

  function appendLogMessage(logData) {
    if (!logOutputElement) { return; } // Guard clause
    if (noLogsPlaceholder) { noLogsPlaceholder.remove(); noLogsPlaceholder = null; } // Remove placeholder

    if (typeof logData !== 'object' || logData === null) {
         console.error("Invalid logData received in appendLogMessage:", logData);
         // Create a fallback object to display an error message
         logData = { level: 'ERROR', message: '[Received invalid log data format]' };
    }

    const level = logData.level || 'UNKNOWN';
    const isError = ['ERROR', 'CRITICAL'].includes(level); // Check membership
    const isWarning = level === 'WARNING';
    const timestamp = logData.timestamp ? formatLocalDateTime(logData.timestamp) : 'No Timestamp';
    const loggerName = escapeHtml(logData.logger_name || 'unknown');
    const message = escapeHtml(logData.message || ''); // Escape potentially unsafe message

    const lineDiv = document.createElement('div');
    lineDiv.className = `log-line log-level-${level.toLowerCase()}`; // Use lowercase level for class
    if (isError) { lineDiv.classList.add('log-error'); }
    if (isWarning) { lineDiv.classList.add('log-warning'); }

    // Construct HTML safely using template literal and textContent
    lineDiv.innerHTML = `
      <span class="local-datetime js-formatted-time">${timestamp}</span>
      [${level}] ${loggerName}: `; // Static part
    lineDiv.appendChild(document.createTextNode(message)); // Append escaped message as text

    logOutputElement.appendChild(lineDiv);
    currentLogCount++;
    updateLogCount();

    // Auto-scroll if near the bottom
    const scrollThreshold = 30; // px tolerance
    const isScrolledBottom = logOutputElement.scrollHeight - logOutputElement.clientHeight <= logOutputElement.scrollTop + scrollThreshold;
    if (isScrolledBottom) {
      logOutputElement.scrollTop = logOutputElement.scrollHeight;
    }
  } // End of appendLogMessage

  // --- SSE Connection Logic ---
  function connectSSE() {
    if (!sseEnabled) {
      console.warn("SSE disabled (Redis not configured).");
      if (sseStatusElement) { sseStatusElement.textContent = "Real-time streaming disabled."; }
      return;
    }
    if (eventSource) {
      console.warn("SSE connection already active.");
      return;
    }

    const sseUrl = `/log-stream/${runId}`;
    eventSource = new EventSource(sseUrl); // Initialize
    console.log(`Connecting SSE: ${sseUrl}`);
    if (sseStatusElement) { sseStatusElement.textContent = "Connecting to log stream..."; }

    eventSource.onopen = function() {
      console.log("SSE connection opened.");
      if (sseStatusElement) { sseStatusElement.textContent = "Connected to log stream."; }
    };

    eventSource.onmessage = function(event) {
      try {
        const logData = JSON.parse(event.data); // Parse incoming JSON
        appendLogMessage(logData);
      } catch (e) {
        console.error("Failed to parse SSE log data:", event.data, e);
        // Append raw data as an error if parse fails
        appendLogMessage({ level: 'ERROR', message: `[Invalid Stream Data]: ${escapeHtml(event.data)}` });
      }
    };

    eventSource.onerror = function(event) {
      console.error("SSE connection error:", event);
      const errorMsg = "Log stream connection error. May retry automatically.";
      if (sseStatusElement) { sseStatusElement.textContent = errorMsg; }
      // Check if the connection is definitely closed
      if (eventSource.readyState === EventSource.CLOSED) {
        console.log("SSE connection permanently closed.");
        if (sseStatusElement) { sseStatusElement.textContent = "Log stream connection failed."; }
        stopStatusChecks(); // Stop checking status if stream fails definitively
        eventSource = null; // Clear reference
      }
    };

    // Listener for custom 'connected' event from server
    eventSource.addEventListener('connected', function(event) {
      console.log("SSE Server signaled connection:", event.data);
    });

    // Listener for custom 'error' events from server
    eventSource.addEventListener('error', function(event) {
      // Check if it's a server-sent error event with data vs. a network error
      if (event.data) {
        console.error("SSE server error event received:", event.data);
        appendLogMessage({ level: 'ERROR', message: `STREAM ERROR: ${escapeHtml(event.data)}` });
        if (sseStatusElement) { sseStatusElement.textContent = `Log stream error: ${event.data}`; }
      } else {
          // This might be a general network error caught by this listener too
          console.error("SSE general error or network issue detected.");
          // Optionally update status, but avoid appending a message without data
      }
    });

  } // End of connectSSE

  function closeSSE() {
    if (eventSource) {
      eventSource.close(); // Close the connection
      eventSource = null; // Clear the reference
      console.log("SSE connection closed by client.");
      if (sseStatusElement) { sseStatusElement.textContent = "Log stream closed."; }
    }
  }

  // --- Status Checking Logic ---
  async function checkRunStatus() {
    // Use runId directly from the outer scope
    if (!runId) {
      console.warn("Cannot check status, run ID missing.");
      stopStatusChecks(); // Stop if no run ID
      return;
    }

    try {
      const response = await fetch(`/status/${runId}`); // Use runId here
      if (!response.ok) {
        console.error(`Status check failed: ${response.status}`);
        if (response.status === 404) { stopStatusChecks(); } // Stop polling if 404
        return;
      }
      const data = await response.json();

      // Update summary card elements
      const statusSpan = document.getElementById('summary-status');
      const endSpan = document.getElementById('summary-end-time');
      const durationSpan = document.getElementById('summary-duration');
      const successSpan = document.getElementById('summary-success');
      const failureSpan = document.getElementById('summary-failure');

      if (statusSpan) {
        statusSpan.textContent = data.overall_status;
        statusSpan.className = `status-${data.overall_status.toLowerCase().replace(/ /g, '_')}`;
      }
      if (endSpan) {
          const formattedEnd = formatLocalDateTime(data.end_time_iso);
          if (endSpan.textContent !== formattedEnd) { // Avoid unnecessary updates
               endSpan.dataset.timestamp = data.end_time_iso || "";
               endSpan.textContent = formattedEnd;
               endSpan.classList.add('js-formatted-time'); // Ensure class if updated
          }
      }
      if (durationSpan) {
          durationSpan.textContent = data.duration_seconds ? data.duration_seconds.toFixed(2) + 's' : 'N/A';
      }
      if (successSpan) { successSpan.textContent = data.summary ? data.summary.success_count : '?'; }
      if (failureSpan) { failureSpan.textContent = data.summary ? data.summary.failure_count : '?'; }

      // --- Stop Polling/SSE if Run Finished ---
      if (finalStates.includes(data.overall_status)) {
        console.log(`Run finished (${data.overall_status}). Closing stream.`);
        closeSSE();
        stopStatusChecks();
        if (sseStatusElement) { sseStatusElement.textContent = `Log stream finished (${data.overall_status}).`; }
      }
    } catch (error) {
      console.error("Error during status check:", error);
      // Optional: Implement logic to stop polling after several consecutive errors?
    }
  } // End of checkRunStatus

  function startStatusChecks() {
    if (statusCheckInterval) { return; } // Already running
    checkRunStatus(); // Check immediately
    statusCheckInterval = setInterval(checkRunStatus, statusCheckFrequency);
    console.log("Started periodic run status checks.");
  }

  function stopStatusChecks() {
    if (statusCheckInterval) {
      clearInterval(statusCheckInterval); // Clear the interval
      statusCheckInterval = null; // Reset the handle
      console.log("Stopped periodic run status checks.");
    }
  }

  // --- Initialization ---
  updateRenderedTimes(); // Format initial timestamps from historical logs
  updateLogCount();      // Set initial count from historical logs

  const initialStatusElem = summaryCard ? document.getElementById('summary-status') : null;
  const initialStatus = initialStatusElem ? initialStatusElem.textContent.trim() : null;

  // Connect SSE only if run is potentially active based on initial status
  if (initialStatus && !finalStates.includes(initialStatus)) {
    console.log(`Initial status '${initialStatus}'. Connect SSE & check status.`);
    connectSSE();
    startStatusChecks();
  } else if (initialStatus) {
    // Run already finished
    const msg = `Run already finished (${initialStatus}). No live logs.`;
    if (sseStatusElement) { sseStatusElement.textContent = msg; }
    console.log(msg);
  } else {
    // Status couldn't be determined initially
    const msg = "Run status unknown. Will check status periodically.";
    if (sseStatusElement) { sseStatusElement.textContent = msg; }
    console.warn(msg);
    startStatusChecks(); // Start checks anyway, maybe status updates later
  }

  // Cleanup on page leave
  window.addEventListener('beforeunload', () => {
    closeSSE();
    stopStatusChecks();
  });

}); // End DOMContentLoaded

// --- END OF FILE dashboard/static/js/log_detail.js ---