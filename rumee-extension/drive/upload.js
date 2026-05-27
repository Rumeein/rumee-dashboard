// ─── Rumee Extension — Google Drive Upload ───────────────────────────────────
// Uses resumable upload protocol for all files (handles any size safely).
// Called only from background.js.

/**
 * Get (or refresh) the OAuth2 token via chrome.identity.
 * Token is cached in chrome.storage.local with an expiry timestamp.
 * Always call this before any Drive API request.
 */
async function getDriveToken(interactive = false) {
  const { driveToken, driveTokenExpiry } = await chrome.storage.local.get([
    'driveToken',
    'driveTokenExpiry',
  ]);

  // Use cached token if still valid (with 5-minute buffer)
  if (driveToken && driveTokenExpiry && Date.now() < driveTokenExpiry - 300_000) {
    return driveToken;
  }

  // Request a fresh token
  return new Promise((resolve, reject) => {
    chrome.identity.getAuthToken({ interactive }, async (token) => {
      if (chrome.runtime.lastError || !token) {
        reject(new Error(
          chrome.runtime.lastError?.message || 'Failed to get Drive auth token'
        ));
        return;
      }
      // Cache token — Drive tokens typically expire in 1 hour
      await chrome.storage.local.set({
        driveToken: token,
        driveTokenExpiry: Date.now() + 3600_000,
      });
      resolve(token);
    });
  });
}

/**
 * Clear the cached token (call when Drive API returns 401).
 */
async function invalidateDriveToken() {
  const { driveToken } = await chrome.storage.local.get('driveToken');
  if (driveToken) {
    // Tell Chrome to forget this token so next getAuthToken fetches a new one
    await new Promise(resolve => chrome.identity.removeCachedAuthToken({ token: driveToken }, resolve));
  }
  await chrome.storage.local.remove(['driveToken', 'driveTokenExpiry']);
}

/**
 * Upload an ArrayBuffer to a Drive folder using the resumable upload protocol.
 * Safe for any file size.
 *
 * @param {ArrayBuffer} buffer     - File contents
 * @param {string}      filename   - Name to give the file in Drive
 * @param {string}      folderId   - Drive folder ID (parent)
 * @param {string}      mimeType   - MIME type of the file
 * @returns {Promise<{id: string, name: string}>} - Drive file metadata
 */
async function uploadToDrive(buffer, filename, folderId, mimeType) {
  const token = await getDriveToken(true);

  // ── Step 1: Initiate resumable upload session ─────────────────────────────
  const initRes = await fetch(
    'https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable',
    {
      method: 'POST',
      headers: {
        'Authorization':           `Bearer ${token}`,
        'Content-Type':            'application/json',
        'X-Upload-Content-Type':   mimeType,
        'X-Upload-Content-Length': String(buffer.byteLength),
      },
      body: JSON.stringify({
        name:    filename,
        parents: [folderId],
      }),
    }
  );

  if (initRes.status === 401) {
    await invalidateDriveToken();
    throw new Error('Drive token expired — will retry on next run');
  }
  if (!initRes.ok) {
    const errBody = await initRes.text();
    throw new Error(`Drive session init failed (${initRes.status}): ${errBody}`);
  }

  const uploadUrl = initRes.headers.get('Location');
  if (!uploadUrl) throw new Error('Drive did not return a resumable upload URL');

  // ── Step 2: Upload the file body ──────────────────────────────────────────
  const uploadRes = await fetch(uploadUrl, {
    method:  'PUT',
    headers: {
      'Content-Type':   mimeType,
      'Content-Length': String(buffer.byteLength),
    },
    body: buffer,
  });

  if (!uploadRes.ok) {
    const errBody = await uploadRes.text();
    throw new Error(`Drive file upload failed (${uploadRes.status}): ${errBody}`);
  }

  return uploadRes.json(); // { id, name, ... }
}
