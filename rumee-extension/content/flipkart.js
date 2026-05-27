// ─── Rumee Extension — Flipkart Content Script ───────────────────────────────
// Runs on https://seller.flipkart.com/* (document_idle).
// Flipkart Seller Hub is an AngularJS SPA; pages render after the hash changes.

// ── Selector constants (update after live testing) ────────────────────────────
const SEL_FK = {
  // Generic download/export button
  DOWNLOAD_BTN:   'button[class*="download" i], button[class*="export" i], [data-testid*="download"], [data-testid*="export"], .download-btn, .export-btn',

  // Date range picker
  DATE_RANGE_BTN: '[class*="date-range"], [class*="DateRange"], [class*="dateFilter"]',

  // "Last 30 days" / "Custom range" options
  LAST_30:        '[data-value="LAST_30_DAYS"], li:has-text("Last 30 days")',
  CUSTOM_RANGE:   '[data-value="CUSTOM"], li:has-text("Custom")',

  // Start/end date inputs for custom range
  DATE_FROM:      'input[name="startDate"], input[placeholder*="From"], [data-testid="from-date"]',
  DATE_TO:        'input[name="endDate"],   input[placeholder*="To"],   [data-testid="to-date"]',

  DATE_APPLY:     'button:has-text("Apply"), [data-testid="apply-btn"]',
};

// Flipkart download endpoint patterns
const FK_DOWNLOAD_PATTERNS = [
  /\/download/i,
  /\/export/i,
  /\/report/i,
  /downloadReport/i,
  /getReportDownload/i,
  /\.xlsx/i,
  /\.csv/i,
  /orders\/download/i,
];

function looksLikeDownload(url) {
  return FK_DOWNLOAD_PATTERNS.some(p => p.test(url));
}

// ── Job handlers ──────────────────────────────────────────────────────────────
const HANDLERS_FK = {
  fk_orders:   handleFkOrders,
  fk_payments: handleFkPayments,
  fk_ads:      handleFkAds,
  fk_views:    handleFkViews,
  fk_keywords: handleFkKeywords,
  fk_claims:   handleFkClaims,
  fk_listings: handleFkListings,
};

// ── Entry point ───────────────────────────────────────────────────────────────
(async () => {
  const job = await askBackground();
  if (!job) return;

  console.log(`[Rumee/Flipkart] Running job: ${job.id}`);

  const handler = HANDLERS_FK[job.id];
  if (!handler) {
    reportError(job.id, `No Flipkart handler for "${job.id}"`);
    return;
  }

  try {
    await handler(job);
  } catch (err) {
    reportError(job.id, err.message || String(err));
  }
})();

// ── Helpers (same pattern as meesho.js) ──────────────────────────────────────

function askBackground() {
  return new Promise(resolve => {
    chrome.runtime.sendMessage(
      { type: 'CONTENT_READY', url: window.location.href },
      response => resolve(response?.job || null)
    );
  });
}

function reportError(jobId, error) {
  console.error(`[Rumee/Flipkart] ${jobId} error:`, error);
  chrome.runtime.sendMessage({ type: 'JOB_ERROR', jobId, error });
}

function waitForElement(selector, timeout = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const selectors = selector.split(',').map(s => s.trim());
    const check = () => {
      for (const sel of selectors) {
        try { const el = document.querySelector(sel); if (el) return el; } catch (_) {}
      }
      return null;
    };
    const el = check();
    if (el) return resolve(el);
    const observer = new MutationObserver(() => {
      const found = check();
      if (found) { observer.disconnect(); clearTimeout(timer); resolve(found); }
    });
    observer.observe(document.body, { childList: true, subtree: true });
    const timer = setTimeout(() => { observer.disconnect(); reject(new Error(`Timeout: ${selector}`)); }, timeout);
  });
}

function findButtonByText(text) {
  const els = Array.from(document.querySelectorAll('button, [role="button"], a'));
  return els.find(el => el.textContent.trim().toLowerCase().includes(text.toLowerCase()));
}

async function clickAndWait(el, ms = 800) { el.click(); await sleep(ms); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function todayISO()     { return new Date().toISOString().slice(0, 10); }
function daysAgoISO(n)  { const d = new Date(); d.setDate(d.getDate() - n); return d.toISOString().slice(0, 10); }

function interceptNextDownload(timeout = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const origFetch   = window.fetch;
    const origXHROpen = XMLHttpRequest.prototype.open;
    let resolved = false;

    const timer = setTimeout(() => { restore(); reject(new Error('No download URL intercepted')); }, timeout);

    function restore() {
      window.fetch = origFetch;
      XMLHttpRequest.prototype.open = origXHROpen;
    }
    function capture(url, headers = {}) {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      restore();
      resolve({ url, headers, referer: window.location.href });
    }

    window.fetch = async function(input, init = {}) {
      const url = (typeof input === 'string') ? input : (input instanceof Request ? input.url : String(input));
      if (looksLikeDownload(url)) {
        const headers = {};
        if (init?.headers) { const h = new Headers(init.headers); h.forEach((v, k) => { headers[k] = v; }); }
        capture(url, headers);
        return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      return origFetch.apply(this, arguments);
    };

    XMLHttpRequest.prototype.open = function(method, url, ...rest) {
      if (looksLikeDownload(url)) capture(url);
      return origXHROpen.apply(this, [method, url, ...rest]);
    };
  });
}

function dispatchDownload(job, url, headers, referer) {
  chrome.runtime.sendMessage({
    type: 'DOWNLOAD_URL_CAPTURED',
    jobId:     job.id,
    url,
    headers,
    referer,
    filename:  job.filename,
    folderKey: job.folderKey,
    mimeType:  job.mimeType,
  });
}

/**
 * Generic Flipkart download: wait for a download button, intercept click, dispatch.
 */
async function genericFkDownload(job, extraSetup = null) {
  // Flipkart SPA needs extra time to hydrate after hash navigation
  await sleep(3000);

  if (extraSetup) await extraSetup();

  let btn = await waitForElement(SEL_FK.DOWNLOAD_BTN).catch(() => null);
  if (!btn) btn = findButtonByText('download') || findButtonByText('export');
  if (!btn) throw new Error(`Download button not found for ${job.id}`);

  const interceptPromise = interceptNextDownload();
  await clickAndWait(btn, 500);

  const { url, headers, referer } = await interceptPromise;
  console.log(`[Rumee/Flipkart] Captured URL for ${job.id}: ${url}`);
  dispatchDownload(job, url, headers, referer);
}

// ── Per-job handlers ──────────────────────────────────────────────────────────

async function handleFkOrders(job) {
  // Seller Hub → Orders → Download All Orders
  await genericFkDownload(job, async () => {
    // Try to set "Last 30 days" or a wider date range
    const dateBtn = document.querySelector(SEL_FK.DATE_RANGE_BTN);
    if (dateBtn) {
      await clickAndWait(dateBtn);
      const last30 = document.querySelector(SEL_FK.LAST_30) || findButtonByText('Last 30 days');
      if (last30) await clickAndWait(last30, 1000);
    }
  });
}

async function handleFkPayments(job) {
  // Seller Hub → Payments → Settlement Report
  await genericFkDownload(job);
}

async function handleFkAds(job) {
  // Seller Hub → Advertising → Reports
  // Flipkart Ads reports require selecting a campaign type + date range.
  // The download button typically appears after filters are applied.
  await sleep(3000);

  // Try to find and click a "Download Report" button directly
  const btn = findButtonByText('Download Report')
    || findButtonByText('Download')
    || await waitForElement(SEL_FK.DOWNLOAD_BTN).catch(() => null);

  if (!btn) throw new Error('Ads download button not found');

  const interceptPromise = interceptNextDownload();
  await clickAndWait(btn, 500);
  const { url, headers, referer } = await interceptPromise;
  dispatchDownload(job, url, headers, referer);
}

async function handleFkViews(job) {
  // Seller Hub → Listing Performance → Download Views Report
  await genericFkDownload(job);
}

async function handleFkKeywords(job) {
  // Seller Hub → Keyword Performance → Download
  await genericFkDownload(job);
}

async function handleFkClaims(job) {
  // Seller Hub → Claims → Download Claims Report
  // The page may have two download buttons (Seller Claims + Auto-Approved).
  // Click the first visible one for now; refine after testing.
  await genericFkDownload(job);
}

async function handleFkListings(job) {
  // Seller Hub → My Listings → Download All
  await genericFkDownload(job, async () => {
    // Wait for the full listing table to load before looking for download
    await sleep(4000);
  });
}
