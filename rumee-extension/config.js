// ─── Rumee Extension — Shared Config ─────────────────────────────────────────
// Loaded in both background (via importScripts) and content scripts (via manifest).
// Edit ONLY this file to change folder IDs or add/remove jobs.

// ── Google Drive folder IDs ───────────────────────────────────────────────────
// ⚠️  FK_CLAIMS, ME_ADS, ME_CLAIMS have canAddChildren:false — fix Drive
//     permissions (share folder with your Google account as Editor) before
//     running integration tests, or uploads to those three will fail silently.
const DRIVE_FOLDERS = {
  FK_ORDERS:   '1-LzJJo3Wi3x6YrUjYCm7SYm3x2tWQqko',
  FK_ADS:      '1ZhNhUH0Yl4ingB830PEgt6pHfHoc1T2S',
  FK_VIEWS:    '1W05Pdgc_Fk7CbRIRUdtA6ZcTFM6SSrxz',
  FK_KEYWORDS: '1VlwkUbx6bzLi1fw1F3qbO_klfDM3vNth',
  FK_LISTINGS: '1sBCegMtxLxr02RkvmlJ5OGYHfD_raBnU',
  FK_PAYMENTS: '1KY-M0_7_FDm_GlqMht4HO2w2wzPRkSgp',
  FK_CLAIMS:   '1Ov-iVVqrl9KpCoZXUlqeV0tNTjGFQeD3',  // ⚠️ fix permissions
  ME_ORDERS:   '1V0ZnC6r577zYJIYeyDhl8rItBrAXgnwQ',
  ME_RETURNS:  '1MEW8yK9lsercJ5k1gQIRh_xiOHpneSV8',
  ME_CATALOG:  '1e7qdkFu6trp3BQDQdAY22i_INGvzKNeu',
  ME_PAYMENTS: '1DoZoUTmNf6hMqC0-WlS2IWPzTDwyAwQr',
  ME_ADS:      '1HMThJGvTIVygdjKh1pTyzbEblro4_0sk',   // ⚠️ fix permissions
  ME_CLAIMS:   '1LX79E16fhxEF5kZGWmdXl4oNsVvqwspf',  // ⚠️ fix permissions
};

// ── Job definitions ───────────────────────────────────────────────────────────
// frequency:
//   'daily'   — run every day
//   '3day'    — run every 3 days (views, keywords — data refreshes slowly)
//   'manual'  — never auto-run; only via popup "Run now" button
//              (catalog & listings — only meaningful when SKUs change)
//
// startUrl: the page the extension navigates to before the content script acts.
// The content script reads window.location to decide what to do.
const JOBS = [
  // ── MEESHO ── (test these first — Akamai CDN validates session cookies tightly)
  {
    id:        'me_orders',
    platform:  'meesho',
    label:     'Meesho Orders CSV',
    startUrl:  'https://supplier.meesho.com/orders',
    folderKey: 'ME_ORDERS',
    filename:  'meesho_orders.csv',
    mimeType:  'text/csv',
    frequency: 'daily',
  },
  {
    id:        'me_returns',
    platform:  'meesho',
    label:     'Meesho Returns CSV',
    startUrl:  'https://supplier.meesho.com/returns',
    folderKey: 'ME_RETURNS',
    filename:  'meesho_returns.csv',
    mimeType:  'text/csv',
    frequency: 'daily',
  },
  {
    id:        'me_payments',
    platform:  'meesho',
    label:     'Meesho Payments XLSX',
    startUrl:  'https://supplier.meesho.com/payments',
    folderKey: 'ME_PAYMENTS',
    filename:  'meesho_payments.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'daily',
  },
  {
    id:        'me_ads',
    platform:  'meesho',
    label:     'Meesho Ads Cost XLSX',
    startUrl:  'https://supplier.meesho.com/advertisements',
    folderKey: 'ME_ADS',
    filename:  'meesho_ads.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'daily',
  },
  {
    id:        'me_claims',
    platform:  'meesho',
    label:     'Meesho Support Tickets CSV',
    startUrl:  'https://supplier.meesho.com/support/tickets',
    folderKey: 'ME_CLAIMS',
    filename:  'meesho_tickets.csv',
    mimeType:  'text/csv',
    frequency: 'daily',
  },
  {
    id:        'me_catalog',
    platform:  'meesho',
    label:     'Meesho Catalog XLSX',
    startUrl:  'https://supplier.meesho.com/products',
    folderKey: 'ME_CATALOG',
    filename:  'meesho_catalog.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'manual',   // only run manually — update when SKUs change
  },

  // ── FLIPKART ──
  {
    id:        'fk_orders',
    platform:  'flipkart',
    label:     'Flipkart Orders XLSX',
    startUrl:  'https://seller.flipkart.com/index.html#orders-manager',
    folderKey: 'FK_ORDERS',
    filename:  'flipkart_orders.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'daily',
  },
  {
    id:        'fk_payments',
    platform:  'flipkart',
    label:     'Flipkart Payments XLSX',
    startUrl:  'https://seller.flipkart.com/index.html#payments',
    folderKey: 'FK_PAYMENTS',
    filename:  'flipkart_payments.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'daily',
  },
  {
    id:        'fk_ads',
    platform:  'flipkart',
    label:     'Flipkart Ads Report XLSX',
    startUrl:  'https://seller.flipkart.com/index.html#advertising',
    folderKey: 'FK_ADS',
    filename:  'flipkart_ads.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'daily',
  },
  {
    id:        'fk_views',
    platform:  'flipkart',
    label:     'Flipkart Views CSV',
    startUrl:  'https://seller.flipkart.com/index.html#listing-performance',
    folderKey: 'FK_VIEWS',
    filename:  'flipkart_views.csv',
    mimeType:  'text/csv',
    frequency: '3day',
  },
  {
    id:        'fk_keywords',
    platform:  'flipkart',
    label:     'Flipkart Keywords CSV',
    startUrl:  'https://seller.flipkart.com/index.html#keyword-performance',
    folderKey: 'FK_KEYWORDS',
    filename:  'flipkart_keywords.csv',
    mimeType:  'text/csv',
    frequency: '3day',
  },
  {
    id:        'fk_claims',
    platform:  'flipkart',
    label:     'Flipkart Claims XLSX',
    startUrl:  'https://seller.flipkart.com/index.html#claims',
    folderKey: 'FK_CLAIMS',
    filename:  'flipkart_claims.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'daily',
  },
  {
    id:        'fk_listings',
    platform:  'flipkart',
    label:     'Flipkart Master Listing XLSX',
    startUrl:  'https://seller.flipkart.com/index.html#my-listings',
    folderKey: 'FK_LISTINGS',
    filename:  'flipkart_listings.xlsx',
    mimeType:  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    frequency: 'manual',   // only run manually — update when SKUs change
  },
];

// How long to wait (ms) for a page element or download URL before declaring failure
const TIMEOUT_MS = 30000;

// How long to wait (ms) between jobs (gives tabs time to settle)
const JOB_GAP_MS = 4000;
