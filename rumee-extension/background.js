// ─── Rumee Extension — Background Service Worker ─────────────────────────────
// MV3 service worker: sleeps between alarms. ALL state lives in
// chrome.storage.local so we survive sleep/wake cycles mid-job.

importScripts('config.js', 'drive/upload.js');

const ALARM_NAME = 'rumee-daily-sync';

// ─── Alarm setup ─────────────────────────────────────────────────────────────

chrome.runtime.onInstalled.addListener(async () => {
  await scheduleAlarm();
  console.log('[Rumee] Extension installed. Alarm scheduled.');
});

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (alarm.name === ALARM_NAME) {
    console.log('[Rumee] Alarm fired — starting sync');
    await startSync();
  }
});

/**
 * Create (or recreate) the daily alarm based on the stored schedule time.
 * Default: 09:00 local time.
 */
async function scheduleAlarm() {
  const { scheduleHour = 9, scheduleMinute = 0 } =
    await chrome.storage.local.get(['scheduleHour', 'scheduleMinute']);

  await chrome.alarms.clear(ALARM_NAME);

  const now   = new Date();
  const next  = new Date();
  next.setHours(scheduleHour, scheduleMinute, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1); // already passed today → tomorrow

  chrome.alarms.create(ALARM_NAME, {
    when:         next.getTime(),
    periodInMinutes: 24 * 60,   // repeat daily
  });
}

// ─── Sync orchestration ───────────────────────────────────────────────────────

/**
 * Entry point — called by alarm or popup "Run now".
 * Builds a job queue, stores it, then starts processing.
 */
async function startSync(manualJobIds = null) {
  const running = await isRunning();
  if (running) {
    console.log('[Rumee] Sync already in progress — skipping');
    return;
  }

  const today = todayStr();
  const { lastRun = {} } = await chrome.storage.local.get('lastRun');

  // Build queue: manual override OR all jobs that are due today
  const queue = (manualJobIds || JOBS.map(j => j.id)).filter(id => {
    const job = JOBS.find(j => j.id === id);
    if (!job) return false;
    if (job.frequency === 'manual' && !manualJobIds) return false;
    if (job.frequency === 'daily') return lastRun[id] !== today;
    if (job.frequency === '3day') {
      const last = lastRun[id];
      if (!last) return true;
      const daysSince = (Date.now() - new Date(last).getTime()) / 86400000;
      return daysSince >= 3;
    }
    return true; // manual job explicitly requested
  });

  if (queue.length === 0) {
    console.log('[Rumee] All jobs up to date — nothing to do');
    notify('Rumee Sync', 'All files are already up to date.');
    return;
  }

  await chrome.storage.local.set({
    syncRunning:  true,
    syncQueue:    queue,
    syncDone:     [],
    syncFailed:   [],
    syncStarted:  Date.now(),
  });

  console.log(`[Rumee] Starting sync — ${queue.length} jobs:`, queue);
  await processNextJob();
}

/**
 * Pull the first job off the queue and process it.
 * Stores currentJobId so we can resume if the worker sleeps mid-job.
 */
async function processNextJob() {
  const { syncQueue = [], syncDone = [], syncFailed = [] } =
    await chrome.storage.local.get(['syncQueue', 'syncDone', 'syncFailed']);

  if (syncQueue.length === 0) {
    await finishSync(syncDone, syncFailed);
    return;
  }

  const [jobId, ...remaining] = syncQueue;
  await chrome.storage.local.set({
    syncQueue:    remaining,
    currentJobId: jobId,
  });

  const job = JOBS.find(j => j.id === jobId);
  if (!job) {
    console.warn(`[Rumee] Unknown job "${jobId}" — skipping`);
    await markJobResult(jobId, false, 'Unknown job id');
    await processNextJob();
    return;
  }

  console.log(`[Rumee] Starting job: ${job.label}`);

  try {
    await openTabForJob(job);
    // Tab message handler (onMessage) will call continueAfterDownload() when done
  } catch (err) {
    console.error(`[Rumee] Failed to open tab for ${job.label}:`, err);
    await markJobResult(jobId, false, err.message);
    // Wait then process next job
    setTimeout(processNextJob, JOB_GAP_MS);
  }
}

/**
 * Open a background tab for a job. Stores the tabId so we can
 * clean it up even if the service worker restarts.
 */
async function openTabForJob(job) {
  // Close any stale tab from a previous run
  const { currentTabId } = await chrome.storage.local.get('currentTabId');
  if (currentTabId) {
    try { await chrome.tabs.remove(currentTabId); } catch (_) {}
  }

  const tab = await chrome.tabs.create({
    url:    job.startUrl,
    active: false,   // run in background — don't hijack the user's focus
  });

  await chrome.storage.local.set({ currentTabId: tab.id });
  console.log(`[Rumee] Opened tab ${tab.id} for ${job.label}`);
}

// ─── Message handler (content script → background) ───────────────────────────

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  // Content script announces it's ready and asks for the current job
  if (msg.type === 'CONTENT_READY') {
    handleContentReady(sender.tab?.id).then(job => sendResponse({ job }));
    return true; // async response
  }

  // Content script has captured a download URL — fetch it and upload
  if (msg.type === 'DOWNLOAD_URL_CAPTURED') {
    handleDownloadUrlCaptured(msg).then(ok => sendResponse({ ok }));
    return true;
  }

  // Content script hit an error it can't recover from
  if (msg.type === 'JOB_ERROR') {
    handleJobError(msg.jobId, msg.error);
    sendResponse({ ok: true });
    return true;
  }
});

/**
 * Content script loaded on the right page — tell it which job to run.
 */
async function handleContentReady(tabId) {
  const { currentJobId, currentTabId } = await chrome.storage.local.get([
    'currentJobId',
    'currentTabId',
  ]);
  // Only respond if this is our tab
  if (tabId !== currentTabId) return null;

  const job = JOBS.find(j => j.id === currentJobId);
  return job || null;
}

/**
 * Content script captured the download URL — re-fetch from background
 * (avoids 64MB sendMessage limit; cookies sent automatically via credentials:include
 *  because extension has host_permission for the domain).
 */
async function handleDownloadUrlCaptured(msg) {
  const { jobId, url, headers = {}, referer, filename, folderKey, mimeType } = msg;

  try {
    console.log(`[Rumee] Fetching file for ${jobId}: ${url}`);

    const res = await fetch(url, {
      credentials: 'include',
      headers: {
        'Referer': referer,
        ...headers,
      },
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }

    const buffer = await res.arrayBuffer();
    console.log(`[Rumee] Downloaded ${buffer.byteLength} bytes — uploading to Drive`);

    const folderId = DRIVE_FOLDERS[folderKey];
    if (!folderId) throw new Error(`No Drive folder mapped for key "${folderKey}"`);

    const driveFile = await uploadToDrive(buffer, filename, folderId, mimeType);
    console.log(`[Rumee] Uploaded to Drive: ${driveFile.name} (${driveFile.id})`);

    await markJobResult(jobId, true);

    // Close the tab and move on
    const { currentTabId } = await chrome.storage.local.get('currentTabId');
    if (currentTabId) {
      try { await chrome.tabs.remove(currentTabId); } catch (_) {}
      await chrome.storage.local.remove('currentTabId');
    }

    setTimeout(processNextJob, JOB_GAP_MS);
    return true;

  } catch (err) {
    console.error(`[Rumee] Upload failed for ${jobId}:`, err);
    await markJobResult(jobId, false, err.message);
    const { currentTabId } = await chrome.storage.local.get('currentTabId');
    if (currentTabId) {
      try { await chrome.tabs.remove(currentTabId); } catch (_) {}
      await chrome.storage.local.remove('currentTabId');
    }
    setTimeout(processNextJob, JOB_GAP_MS);
    return false;
  }
}

async function handleJobError(jobId, error) {
  console.error(`[Rumee] Content script error for ${jobId}:`, error);
  await markJobResult(jobId, false, error);
  const { currentTabId } = await chrome.storage.local.get('currentTabId');
  if (currentTabId) {
    try { await chrome.tabs.remove(currentTabId); } catch (_) {}
    await chrome.storage.local.remove('currentTabId');
  }
  setTimeout(processNextJob, JOB_GAP_MS);
}

// ─── Job result helpers ───────────────────────────────────────────────────────

async function markJobResult(jobId, success, errMsg = null) {
  const { syncDone = [], syncFailed = [], lastRun = {} } =
    await chrome.storage.local.get(['syncDone', 'syncFailed', 'lastRun']);

  if (success) {
    lastRun[jobId] = todayStr();
    await chrome.storage.local.set({
      syncDone: [...syncDone, jobId],
      lastRun,
    });
  } else {
    await chrome.storage.local.set({
      syncFailed: [...syncFailed, { id: jobId, error: errMsg }],
    });
  }
  await chrome.storage.local.remove('currentJobId');
}

async function finishSync(done, failed) {
  await chrome.storage.local.set({ syncRunning: false });
  await chrome.storage.local.remove(['currentJobId', 'currentTabId']);

  const msg = failed.length === 0
    ? `✅ All ${done.length} files synced to Drive.`
    : `✅ ${done.length} synced  ❌ ${failed.length} failed: ${failed.map(f => f.id).join(', ')}`;

  notify('Rumee Sync Complete', msg);
  console.log('[Rumee] Sync complete:', { done, failed });
}

async function isRunning() {
  const { syncRunning } = await chrome.storage.local.get('syncRunning');
  // Safety valve: if a sync has been "running" for over 90 minutes, reset it
  const { syncStarted } = await chrome.storage.local.get('syncStarted');
  if (syncRunning && syncStarted && Date.now() - syncStarted > 90 * 60_000) {
    console.warn('[Rumee] Stale sync detected — resetting');
    await chrome.storage.local.set({ syncRunning: false });
    return false;
  }
  return !!syncRunning;
}

// ─── Utility ─────────────────────────────────────────────────────────────────

function todayStr() {
  return new Date().toISOString().slice(0, 10); // 'YYYY-MM-DD'
}

function notify(title, message) {
  chrome.notifications.create({
    type:    'basic',
    iconUrl: 'icons/icon48.png',
    title,
    message,
  });
}

// ─── Popup message handler ────────────────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === 'RUN_NOW') {
    startSync(msg.jobIds || null).then(() => sendResponse({ ok: true }));
    return true;
  }
  if (msg.type === 'GET_STATUS') {
    chrome.storage.local.get(
      ['syncRunning', 'syncQueue', 'syncDone', 'syncFailed', 'lastRun', 'currentJobId'],
      data => sendResponse(data)
    );
    return true;
  }
  if (msg.type === 'UPDATE_SCHEDULE') {
    chrome.storage.local.set(
      { scheduleHour: msg.hour, scheduleMinute: msg.minute },
      () => { scheduleAlarm().then(() => sendResponse({ ok: true })); }
    );
    return true;
  }
});

// ─── Resume on wake ───────────────────────────────────────────────────────────
// If the service worker wakes up and a sync was in progress, resume it.
(async () => {
  const { syncRunning, syncQueue = [], currentJobId } =
    await chrome.storage.local.get(['syncRunning', 'syncQueue', 'currentJobId']);

  if (!syncRunning) return;

  // If a job was mid-flight when the worker died, re-queue it at the front
  if (currentJobId && !syncQueue.includes(currentJobId)) {
    console.log(`[Rumee] Resuming after sleep — re-queuing ${currentJobId}`);
    await chrome.storage.local.set({ syncQueue: [currentJobId, ...syncQueue] });
  }

  console.log('[Rumee] Service worker woke — resuming sync');
  await processNextJob();
})();
