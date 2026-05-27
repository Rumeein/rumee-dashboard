// ─── Rumee Extension — Meesho Content Script ─────────────────────────────────
// Runs on https://supplier.meesho.com/* (document_idle).
//
// IMPORTANT — TEST THIS FIRST (see README §Testing):
//   Meesho uses Akamai Bot Manager. This script runs inside your real Chrome
//   session with real cookies so it should pass, but the download endpoints
//   validate Referer + session tightly. If a download fetch returns HTML instead
//   of CSV/XLSX the session check failed — log out, log back in, retry.
//
// Selectors and API paths below are best-effort. Update them after testing.
// All selector constants are at the top for easy adjustment.

// ── Selector constants (update these after live testing) ─────────────────────
const SEL = {
  // Date range picker trigger (the button/input that opens the calendar)
  DATE_RANGE_BTN:   '[data-testid="date-range-picker"], .date-range-picker, [class*="DateRange"], [class*="dateRange"]',

  // "Custom" option inside the date picker dropdown
  DATE_CUSTOM_OPT:  '[data-value="CUSTOM"], [data-option="custom"], li:has-text("Custom")',

  // Start / end date inputs once custom is selected
  DATE_FROM_INPUT:  'input[placeholder*="From"], input[placeholder*="Start"], [data-testid="from-date"]',
  DATE_TO_INPUT:    'input[placeholder*="To"], input[placeholder*="End"], [data-testid="to-date"]',

  // Apply / confirm button on the date picker
  DATE_APPLY_BTN:   'button:has-text("Apply"), button:has-text("Confirm"), [data-testid="apply-date"]',

  // The download / export button on each report page
  DOWNLOAD_BTN:     'button[class*="download" i], button[class*="export" i], [data-testid*="download"], [data-testid*="export"]',
};

// ── Download endpoint patterns (used to detect the download XHR/fetch) ───────
// Add more patterns here if Meesho adds new download URLs.
const MEESHO_DOWNLOAD_PATTERNS = [
  /\/download/i,
  /\/export/i,
  /\/csv/i,
  /\/report/i,
  /downloadOrders/i,
  /downloadReturns/i,
  /downloadPayments/i,
];

function looksLikeDownload(url) {
  return MEESHO_DOWNLOAD_PATTERNS.some(p => p.test(url));
}

// ── Job handlers (one per job id) ─────────────────────────────────────────────
const HANDLERS = {
  me_orders:   handleOrders,
  me_returns:  handleReturns,
  me_payments: handlePayments,
  me_ads:      handleAds,
  me_claims:   handleClaims,
  me_catalog:  handleCatalog,
};

// ── Entry point ───────────────────────────────────────────────────────────────
(async () => {
  // Ask background which job (if any) we should run on this page.
  // If background returns null it means this tab wasn't opened by Rumee.
  const job = await askBackground();
  if (!job) return;

  console.log(`[Rumee/Meesho] Running job: ${job.id}`);

  const handler = HANDLERS[job.id];
  if (!handler) {
    reportError(job.id, `No handler for job "${job.id}"`);
    return;
  }

  try {
    await handler(job);
  } catch (err) {
    reportError(job.id, err.message || String(err));
  }
})();

// ── Generic helpers ───────────────────────────────────────────────────────────

function askBackground() {
  return new Promise(resolve => {
    chrome.runtime.sendMessage(
      { type: 'CONTENT_READY', url: window.location.href },
      response => resolve(response?.job || null)
    );
  });
}

function reportError(jobId, error) {
  console.error(`[Rumee/Meesho] ${jobId} error:`, error);
  chrome.runtime.sendMessage({ type: 'JOB_ERROR', jobId, error });
}

/**
 * Wait for an element matching selector to appear in the DOM.
 * Tries multiple selectors (comma-separated) and returns the first match.
 */
function waitForElement(selector, timeout = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const selectors = selector.split(',').map(s => s.trim());

    const check = () => {
      for (const sel of selectors) {
        try {
          const el = document.querySelector(sel);
          if (el) return el;
        } catch (_) {}
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

    const timer = setTimeout(() => {
      observer.disconnect();
      reject(new Error(`Timeout waiting for: ${selector}`));
    }, timeout);
  });
}

/** Find a button by its visible text (case-insensitive, partial match). */
function findButtonByText(text) {
  const buttons = Array.from(document.querySelectorAll('button, [role="button"]'));
  return buttons.find(b => b.textContent.trim().toLowerCase().includes(text.toLowerCase()));
}

/** Click an element and wait a short moment for the UI to react. */
async function clickAndWait(el, waitMs = 800) {
  el.click();
  await sleep(waitMs);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/** ISO date string for today: 'YYYY-MM-DD' */
function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

/** ISO date string for N days ago */
function daysAgoISO(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

/**
 * Patch window.fetch to intercept the next request matching looksLikeDownload().
 * Returns a promise that resolves with { url, headers, referer }.
 * The intercepted fetch is CANCELLED (empty 200 response returned to the page)
 * so the browser never starts a file download.
 */
function interceptNextDownload(timeout = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const origFetch = window.fetch;
    const origXHROpen = XMLHttpRequest.prototype.open;
    let resolved = false;

    const timer = setTimeout(() => {
      restore();
      reject(new Error('No download URL intercepted within timeout'));
    }, timeout);

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

    // ── Intercept fetch ───────────────────────────────────────────────────
    window.fetch = async function(input, init = {}) {
      const url = (typeof input === 'string') ? input
                : (input instanceof Request) ? input.url
                : String(input);

      if (looksLikeDownload(url)) {
        // Flatten any Headers object into a plain object
        const headers = {};
        if (init?.headers) {
          const h = new Headers(init.headers);
          h.forEach((v, k) => { headers[k] = v; });
        }
        capture(url, headers);
        // Return empty success so the page doesn't throw
        return new Response('{}', {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      return origFetch.apply(this, arguments);
    };

    // ── Intercept XHR ────────────────────────────────────────────────────
    XMLHttpRequest.prototype.open = function(method, url, ...rest) {
      if (looksLikeDownload(url)) {
        capture(url);
        // Don't abort — just capture. XHR abort is complex; let it proceed.
      }
      return origXHROpen.apply(this, [method, url, ...rest]);
    };
  });
}

/**
 * Tell background to fetch and upload the file.
 */
function dispatchDownload(job, url, headers, referer) {
  chrome.runtime.sendMessage({
    type:      'DOWNLOAD_URL_CAPTURED',
    jobId:     job.id,
    url,
    headers,
    referer,
    filename:  job.filename,
    folderKey: job.folderKey,
    mimeType:  job.mimeType,
  });
}

// ── Job handlers ──────────────────────────────────────────────────────────────

/**
 * Common pattern for most Meesho report pages:
 * 1. Wait for the Download/Export button to appear
 * 2. Set up fetch intercept
 * 3. Click the button
 * 4. Capture URL and dispatch to background
 */
async function genericDownload(job, extraSetup = null) {
  // Allow the SPA to finish rendering
  await sleep(2000);

  if (extraSetup) await extraSetup();

  // Find download button
  let downloadBtn = await waitForElement(SEL.DOWNLOAD_BTN).catch(() => null);
  if (!downloadBtn) {
    // Fallback: search by text
    downloadBtn = findButtonByText('download') || findButtonByText('export');
  }
  if (!downloadBtn) throw new Error('Download button not found on page');

  const interceptPromise = interceptNextDownload();
  await clickAndWait(downloadBtn, 500);

  const { url, headers, referer } = await interceptPromise;
  console.log(`[Rumee/Meesho] Captured download URL for ${job.id}: ${url}`);
  dispatchDownload(job, url, headers, referer);
}

async function handleOrders(job) {
  // Meesho orders page — set date range to "Custom" → full history or last 6 months
  await genericDownload(job, async () => {
    // Try to set custom date range: 180 days back to today
    const dateBtn = document.querySelector(SEL.DATE_RANGE_BTN);
    if (!dateBtn) {
      console.warn('[Rumee/Meesho] Date range button not found — using page default');
      return;
    }

    await clickAndWait(dateBtn);

    // Look for "Custom" option in the dropdown
    const customOpt = document.querySelector(SEL.DATE_CUSTOM_OPT)
      || findButtonByText('Custom')
      || Array.from(document.querySelectorAll('li, option')).find(
          el => el.textContent.trim().toLowerCase() === 'custom'
        );

    if (customOpt) {
      await clickAndWait(customOpt);
      await sleep(500);

      const fromInput = document.querySelector(SEL.DATE_FROM_INPUT);
      const toInput   = document.querySelector(SEL.DATE_TO_INPUT);

      if (fromInput && toInput) {
        // Set from = 180 days ago, to = today
        const fromDate = daysAgoISO(180);
        const toDate   = todayISO();

        fromInput.value = fromDate;
        fromInput.dispatchEvent(new Event('input', { bubbles: true }));
        fromInput.dispatchEvent(new Event('change', { bubbles: true }));

        await sleep(300);

        toInput.value = toDate;
        toInput.dispatchEvent(new Event('input', { bubbles: true }));
        toInput.dispatchEvent(new Event('change', { bubbles: true }));

        await sleep(300);

        const applyBtn = document.querySelector(SEL.DATE_APPLY_BTN)
          || findButtonByText('Apply')
          || findButtonByText('Confirm');
        if (applyBtn) await clickAndWait(applyBtn, 1000);
      }
    }
  });
}

async function handleReturns(job) {
  await genericDownload(job);
}

async function handlePayments(job) {
  await genericDownload(job);
}

async function handleAds(job) {
  await genericDownload(job);
}

async function handleClaims(job) {
  // Support tickets export
  await sleep(2000);

  let exportBtn = findButtonByText('Export')
    || findButtonByText('Download')
    || await waitForElement('[data-testid*="export"], [data-testid*="download"]').catch(() => null);

  if (!exportBtn) throw new Error('Export button not found on support tickets page');

  const interceptPromise = interceptNextDownload();
  await clickAndWait(exportBtn, 500);

  const { url, headers, referer } = await interceptPromise;
  dispatchDownload(job, url, headers, referer);
}

async function handleCatalog(job) {
  // Catalog download — similar pattern but on My Products page
  await genericDownload(job, async () => {
    // Wait for catalog table to load before looking for download
    await sleep(3000);
  });
}
