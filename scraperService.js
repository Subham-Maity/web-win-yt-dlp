const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');
const { findBrowserExe } = require('./instagramAuthService');

const SCRAPER_CDP_PORT = 9223;
const SCRAPER_PROFILE_DIR = path.join(os.homedir(), 'AppData', 'Local', 'yt-dlp-web', 'scraper_browser_profile');
const BRIDGE_TTL_MS = 25000;

// ── Extension Bridge State (Long-Polling) ────────────────────────────────────
let bridgeLastSeen = 0;
let bridgeWaiter = null; // { res, timer }
const pendingBridgeTasks = []; // [{ taskId, action, url, options, resolve, reject, timer }]
const activeBridgeTasks = new Map(); // taskId -> { resolve, reject, timer }

function isBridgeConnected() {
  return Date.now() - bridgeLastSeen < BRIDGE_TTL_MS;
}

function getBridgeState() {
  return {
    bridgeConnected: isBridgeConnected(),
    lastSeenAgoMs: bridgeLastSeen ? Date.now() - bridgeLastSeen : null,
    cdpBrowserAvailable: Boolean(findBrowserExe())
  };
}

/**
 * Long-poll handler called by the Chrome/Edge Extension background service worker.
 */
function handleBridgePoll(req, res) {
  bridgeLastSeen = Date.now();

  if (pendingBridgeTasks.length > 0) {
    const next = pendingBridgeTasks.shift();
    return res.json({
      hasTask: true,
      task: {
        taskId: next.taskId,
        action: next.action,
        url: next.url,
        options: next.options || {}
      }
    });
  }

  // Close any stale previous waiter
  if (bridgeWaiter) {
    try {
      clearTimeout(bridgeWaiter.timer);
      bridgeWaiter.res.json({ hasTask: false });
    } catch (_) {}
    bridgeWaiter = null;
  }

  const timer = setTimeout(() => {
    if (bridgeWaiter && bridgeWaiter.res === res) {
      bridgeWaiter = null;
      try { res.json({ hasTask: false }); } catch (_) {}
    }
  }, 18000);

  bridgeWaiter = { res, timer };

  req.on('close', () => {
    if (bridgeWaiter && bridgeWaiter.res === res) {
      clearTimeout(bridgeWaiter.timer);
      bridgeWaiter = null;
    }
  });
}

/**
 * Receives task results submitted by the Chrome/Edge Extension.
 */
function handleBridgeResult(body) {
  bridgeLastSeen = Date.now();
  const { taskId, ok, data, error } = body || {};
  if (!taskId || !activeBridgeTasks.has(taskId)) {
    return false;
  }
  const entry = activeBridgeTasks.get(taskId);
  activeBridgeTasks.delete(taskId);
  clearTimeout(entry.timer);

  if (ok) {
    entry.resolve(data);
  } else {
    entry.reject(new Error(error || 'Extension bridge task failed'));
  }
  return true;
}

function dispatchBridgeTask(action, url, options = {}, timeoutMs = 45000, abortSignal = null) {
  return new Promise((resolve, reject) => {
    if (abortSignal && abortSignal.aborted) {
      return reject(new Error('Task cancelled'));
    }

    const taskId = `ext_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

    const timer = setTimeout(() => {
      activeBridgeTasks.delete(taskId);
      const idx = pendingBridgeTasks.findIndex(t => t.taskId === taskId);
      if (idx !== -1) pendingBridgeTasks.splice(idx, 1);
      reject(new Error(`Extension bridge timed out after ${Math.round(timeoutMs / 1000)}s`));
    }, timeoutMs);

    const taskEntry = { taskId, action, url, options, resolve, reject, timer };
    activeBridgeTasks.set(taskId, taskEntry);

    if (abortSignal) {
      abortSignal.addEventListener('abort', () => {
        clearTimeout(timer);
        activeBridgeTasks.delete(taskId);
        const idx = pendingBridgeTasks.findIndex(t => t.taskId === taskId);
        if (idx !== -1) pendingBridgeTasks.splice(idx, 1);
        reject(new Error('Cancelled by user'));
      }, { once: true });
    }

    if (bridgeWaiter) {
      const waiter = bridgeWaiter;
      bridgeWaiter = null;
      clearTimeout(waiter.timer);
      try {
        waiter.res.json({
          hasTask: true,
          task: { taskId, action, url, options }
        });
        return;
      } catch (_) {
        // Fall through to pending queue if socket closed
      }
    }

    pendingBridgeTasks.push(taskEntry);
  });
}

// ── Generic Cookie Manager (Netscape / JSON / Header String -> cookies.txt) ──
function extractHostname(rawUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') return '';
  try {
    const normalized = /^https?:\/\//i.test(rawUrl.trim()) ? rawUrl.trim() : `https://${rawUrl.trim()}`;
    return new URL(normalized).hostname.toLowerCase();
  } catch (_) {
    return '';
  }
}

function getRootDomain(hostname) {
  if (!hostname) return '';
  const parts = hostname.split('.').filter(Boolean);
  if (parts.length <= 2) return hostname;
  return parts.slice(-2).join('.');
}

function readNetscapeCookieLines(cookiesFilePath) {
  if (!fs.existsSync(cookiesFilePath)) return [];
  try {
    const raw = fs.readFileSync(cookiesFilePath, 'utf8');
    return raw.split(/\r?\n/)
      .map(l => l.trim())
      .filter(l => l && !l.startsWith('#'));
  } catch (_) {
    return [];
  }
}

function getCookiesForUrl(cookiesFilePath, targetUrl) {
  const host = extractHostname(targetUrl);
  const root = getRootDomain(host);
  const lines = readNetscapeCookieLines(cookiesFilePath);
  const matched = [];

  for (const line of lines) {
    const parts = line.split('\t');
    if (parts.length < 7) continue;
    const domain = parts[0].toLowerCase().replace(/^\./, '');
    if (!host || host === domain || host.endsWith('.' + domain) || (root && domain.endsWith(root))) {
      matched.push({
        domain: parts[0],
        path: parts[2],
        secure: parts[3] === 'TRUE',
        expires: parseInt(parts[4], 10) || 0,
        name: parts[5],
        value: parts[6]
      });
    }
  }
  return matched;
}

function getCookiesSummary(cookiesFilePath, targetUrl = '') {
  const lines = readNetscapeCookieLines(cookiesFilePath);
  const domains = new Set();
  for (const line of lines) {
    const parts = line.split('\t');
    if (parts.length >= 7) domains.add(parts[0].toLowerCase().replace(/^\./, ''));
  }
  const matching = targetUrl ? getCookiesForUrl(cookiesFilePath, targetUrl) : [];
  return {
    exists: fs.existsSync(cookiesFilePath),
    totalCookies: lines.length,
    domainsCount: domains.size,
    matchingCount: matching.length,
    targetDomain: extractHostname(targetUrl)
  };
}

/**
 * Parses Cookie-Editor JSON, Netscape cookies.txt, or raw "Cookie: a=1; b=2" text
 * and merges into cookies.txt without overwriting cookies from unrelated domains.
 */
function saveGenericCookies(inputData, targetUrl, cookiesFilePath) {
  if (!inputData || typeof inputData !== 'string' || !inputData.trim()) {
    throw new Error('Cookie data cannot be empty.');
  }

  const text = inputData.trim();
  const fallbackHost = extractHostname(targetUrl);
  const fallbackDomain = fallbackHost ? `.${getRootDomain(fallbackHost)}` : '';
  const parsed = [];

  // 1. JSON array (Cookie-Editor)
  if (text.startsWith('[') && text.endsWith(']')) {
    try {
      const arr = JSON.parse(text);
      if (Array.isArray(arr)) {
        for (const c of arr) {
          if (!c || !c.name || c.value === undefined) continue;
          const dom = (c.domain || fallbackDomain || '').trim();
          if (!dom) continue;
          parsed.push({
            domain: dom,
            path: c.path || '/',
            secure: c.secure !== false,
            expires: Math.round(c.expirationDate > 0 ? c.expirationDate : (Date.now() / 1000) + (365 * 86400)),
            name: String(c.name),
            value: String(c.value)
          });
        }
      }
    } catch (_) {}
  }

  // 2. Netscape format
  if (parsed.length === 0 && text.includes('\t')) {
    const lines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    for (const rawLine of lines) {
      const line = rawLine.replace(/^#HttpOnly_/i, '');
      if (line.startsWith('#')) continue;
      const parts = line.split('\t');
      if (parts.length >= 7) {
        parsed.push({
          domain: parts[0],
          path: parts[2] || '/',
          secure: parts[3] === 'TRUE',
          expires: parseInt(parts[4], 10) || Math.round((Date.now() / 1000) + (365 * 86400)),
          name: parts[5],
          value: parts[6]
        });
      }
    }
  }

  // 3. Raw "a=1; b=2" or "Cookie: a=1; b=2" string (requires targetUrl domain)
  if (parsed.length === 0 && text.includes('=')) {
    if (!fallbackDomain) {
      throw new Error('Please enter the Webpage URL above first so we know which domain these cookies belong to, or paste full Netscape/JSON cookies.');
    }
    const clean = text.replace(/^cookie:\s*/i, '');
    const pairs = clean.split(';').map(s => s.trim()).filter(Boolean);
    for (const pair of pairs) {
      const eq = pair.indexOf('=');
      if (eq <= 0) continue;
      const name = pair.slice(0, eq).trim();
      const value = pair.slice(eq + 1).trim();
      if (!name) continue;
      parsed.push({
        domain: fallbackDomain,
        path: '/',
        secure: true,
        expires: Math.round((Date.now() / 1000) + (365 * 86400)),
        name,
        value
      });
    }
  }

  if (parsed.length === 0) {
    throw new Error('Could not parse cookies. Paste Cookie-Editor JSON, Netscape cookies.txt, or "name=value; ..." header text.');
  }

  const importedDomains = new Set(parsed.map(c => getRootDomain(c.domain.toLowerCase().replace(/^\./, ''))));
  const existingLines = readNetscapeCookieLines(cookiesFilePath).filter(line => {
    const parts = line.split('\t');
    if (parts.length < 7) return false;
    const domRoot = getRootDomain(parts[0].toLowerCase().replace(/^\./, ''));
    return !importedDomains.has(domRoot);
  });

  const deduped = new Map();
  for (const c of parsed) {
    deduped.set(`${c.domain}|${c.path}|${c.name}`, c);
  }

  const newLines = Array.from(deduped.values()).map(c => {
    const sub = c.domain.startsWith('.') ? 'TRUE' : 'FALSE';
    const sec = c.secure ? 'TRUE' : 'FALSE';
    return `${c.domain}\t${sub}\t${c.path}\t${sec}\t${c.expires}\t${c.name}\t${c.value}`;
  });

  const content = [
    '# Netscape HTTP Cookie File',
    '# Managed by yt-dlp-web',
    ...existingLines,
    ...newLines
  ].join('\n') + '\n';

  fs.writeFileSync(cookiesFilePath, content, 'utf8');
  return { count: deduped.size, domains: Array.from(importedDomains) };
}

function clearDomainCookies(cookiesFilePath, targetUrl = '') {
  if (!fs.existsSync(cookiesFilePath)) return { removed: 0 };
  const host = extractHostname(targetUrl);
  const root = getRootDomain(host);
  const lines = readNetscapeCookieLines(cookiesFilePath);

  const kept = lines.filter(line => {
    const parts = line.split('\t');
    if (parts.length < 7) return false;
    const dom = parts[0].toLowerCase().replace(/^\./, '');
    if (root) {
      return !(dom === root || dom.endsWith('.' + root) || root.endsWith('.' + dom));
    }
    // If no targetUrl specified, preserve Instagram cookies and clear others
    return dom.includes('instagram.com');
  });

  const removed = lines.length - kept.length;
  const content = [
    '# Netscape HTTP Cookie File',
    '# Managed by yt-dlp-web',
    ...kept
  ].join('\n') + '\n';
  fs.writeFileSync(cookiesFilePath, content, 'utf8');
  return { removed };
}

// ── Chrome/Edge CDP Browser Manager (No external packages required) ──────────
async function getCdpVersion(port = SCRAPER_CDP_PORT) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/json/version`, {
      signal: AbortSignal.timeout(1200)
    });
    if (!res.ok) return null;
    return await res.json();
  } catch (_) {
    return null;
  }
}

async function ensureCdpBrowser({ visible = false, initialUrl = 'about:blank' } = {}) {
  let info = await getCdpVersion(SCRAPER_CDP_PORT);
  if (info && info.webSocketDebuggerUrl) {
    if (visible && initialUrl && initialUrl !== 'about:blank') {
      await openVisibleCdpTab(info.webSocketDebuggerUrl, initialUrl);
    }
    return info;
  }

  const browserExe = findBrowserExe();
  if (!browserExe) {
    throw new Error('Google Chrome or Microsoft Edge was not found on this machine. Please install Chrome/Edge or use the Extension Bridge.');
  }

  if (!fs.existsSync(SCRAPER_PROFILE_DIR)) {
    fs.mkdirSync(SCRAPER_PROFILE_DIR, { recursive: true });
  }

  const args = [
    `--remote-debugging-port=${SCRAPER_CDP_PORT}`,
    `--user-data-dir=${SCRAPER_PROFILE_DIR}`,
    '--no-first-run',
    '--no-default-browser-check',
    '--mute-audio',
    '--new-window'
  ];

  if (!visible) {
    args.push('--start-minimized');
  }
  args.push(initialUrl || 'about:blank');

  const proc = spawn(browserExe, args, {
    detached: true,
    stdio: 'ignore'
  });
  proc.unref();

  for (let i = 0; i < 25; i++) {
    await new Promise(r => setTimeout(r, 350));
    info = await getCdpVersion(SCRAPER_CDP_PORT);
    if (info && info.webSocketDebuggerUrl) {
      return info;
    }
  }

  throw new Error('Timed out waiting for Chrome/Edge CDP debugging port to start.');
}

function openWebSocket(wsUrl, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch (_) {}
      reject(new Error('CDP WebSocket connection timed out'));
    }, timeoutMs);

    ws.onopen = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(ws);
    };
    ws.onerror = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err instanceof Error ? err : new Error('CDP WebSocket connection error'));
    };
  });
}

function sendCdpCommand(ws, id, method, params = {}, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      ws.removeEventListener('message', onMessage);
      reject(new Error(`CDP command ${method} timed out`));
    }, timeoutMs);

    const onMessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.id === id) {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          ws.removeEventListener('message', onMessage);
          if (msg.error) reject(new Error(msg.error.message || 'CDP error'));
          else resolve(msg.result);
        }
      } catch (_) {}
    };
    ws.addEventListener('message', onMessage);
    try {
      ws.send(JSON.stringify({ id, method, params }));
    } catch (err) {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        ws.removeEventListener('message', onMessage);
        reject(err);
      }
    }
  });
}

async function openVisibleCdpTab(browserWsUrl, url) {
  const ws = await openWebSocket(browserWsUrl);
  try {
    await sendCdpCommand(ws, 1, 'Target.createTarget', { url });
  } finally {
    try { ws.close(); } catch (_) {}
  }
}

// Shared DOM extractor evaluated inside the webpage (both CDP & Extension)
const DOM_VIDEO_EXTRACTOR_SCRIPT = `(() => {
  const pageUrl = window.location.href.split('#')[0];
  const pageOrigin = window.location.origin;
  const pageTitle = (document.title || '').trim();
  const seen = new Map();

  const skipWords = ['login', 'signin', 'sign-in', 'signup', 'sign-up', 'register', 'privacy', 'terms', 'dmca', 'contact', 'cookie', 'about', 'faq', 'logout', 'support', 'policy', 'forgot'];
  const videoHints = ['/video', '/watch', '/play', '/clip', '/reel', '/embed', '/movie', '/v/', '/media', '.mp4', '.m3u8', '.webm'];

  function absUrl(raw) {
    if (!raw) return '';
    try {
      const u = new URL(raw, pageUrl);
      if (u.protocol !== 'http:' && u.protocol !== 'https:') return '';
      return u.href.split('#')[0];
    } catch (_) { return ''; }
  }

  function cleanText(s) {
    return (s || '').replace(/\\s+/g, ' ').trim();
  }

  function pickImgSrc(img) {
    if (!img) return '';
    return absUrl(
      img.getAttribute('data-src') ||
      img.getAttribute('data-original') ||
      img.getAttribute('data-lazy-src') ||
      img.getAttribute('data-thumb') ||
      img.getAttribute('poster') ||
      img.currentSrc ||
      img.getAttribute('src') || ''
    );
  }

  // 1. Direct <video> / <source> elements on page
  document.querySelectorAll('video').forEach((v, idx) => {
    const src = absUrl(v.currentSrc || v.getAttribute('src') || (v.querySelector('source') && v.querySelector('source').getAttribute('src')));
    if (src && !src.startsWith('blob:')) {
      seen.set(src, {
        url: src,
        title: cleanText(v.getAttribute('title') || v.getAttribute('aria-label') || pageTitle || ('Direct Video ' + (idx + 1))),
        thumbnail: absUrl(v.getAttribute('poster') || ''),
        duration: '',
        isDirectMedia: true,
        score: 100
      });
    }
  });

  // 2. Anchor links with thumbnails / video URL patterns
  document.querySelectorAll('a[href]').forEach((a) => {
    const href = absUrl(a.getAttribute('href'));
    if (!href || href === pageUrl) return;
    if (href.startsWith('mailto:') || href.startsWith('tel:') || href.startsWith('javascript:')) return;

    const lower = href.toLowerCase();
    const pathPart = lower.replace(pageOrigin.toLowerCase(), '');
    if (skipWords.some(w => pathPart.includes('/' + w) || pathPart.endsWith(w))) return;

    const card = a.closest('article, li, .video, .card, .item, .thumb, [class*="video"], [class*="thumb"], [class*="card"]') || a;
    const img = a.querySelector('img') || card.querySelector('img');
    const thumb = pickImgSrc(img);
    const hasVideoHint = videoHints.some(h => lower.includes(h));

    // Find duration text near card (e.g. 06:26 or 1:02:15)
    let duration = '';
    const cardText = cleanText(card.textContent || '');
    const durMatch = cardText.match(/\\b(\\d{1,2}:\\d{2}(?::\\d{2})?)\\b/);
    if (durMatch) duration = durMatch[1];

    // Must either look like a video link, have a thumbnail image, or have a duration badge
    if (!hasVideoHint && !thumb && !duration) return;
    // Skip tiny category/tag/pagination links without thumbnails
    if (!thumb && /([?&]page=|\\/page\\/|\\/tag\\/|\\/category\\/)/i.test(lower)) return;

    let title = cleanText(
      a.getAttribute('title') ||
      (img && (img.getAttribute('alt') || img.getAttribute('title'))) ||
      ''
    );

    if (!title || title.length < 4) {
      const heading = card.querySelector('h1,h2,h3,h4,.title,[class*="title"],[class*="caption"],[class*="desc"],[class*="name"]');
      const headingTxt = heading ? cleanText(heading.textContent) : '';
      const anchorTxt = cleanText(a.textContent || '');
      if (headingTxt.length > title.length) title = headingTxt;
      else if (anchorTxt.length > title.length) title = anchorTxt;
    }
    if (!title || title.length < 2) {
      title = cleanText(a.textContent || '');
    }
    if (duration && title && title.startsWith(duration)) {
      title = cleanText(title.slice(duration.length));
    }
    if (!title) title = href.split('/').filter(Boolean).pop() || href;

    const score = (hasVideoHint ? 40 : 0) + (thumb ? 30 : 0) + (duration ? 20 : 0) + (title.length > 5 ? 10 : 0);
    const prev = seen.get(href);
    if (!prev || score > prev.score || (!prev.thumbnail && thumb)) {
      seen.set(href, {
        url: href,
        title: title.slice(0, 160),
        thumbnail: thumb || (prev ? prev.thumbnail : ''),
        duration: duration || (prev ? prev.duration : ''),
        isDirectMedia: /\\.(m3u8|mp4|webm|mov|mpd)(\\?|$)/i.test(lower),
        score
      });
    }
  });

  const items = Array.from(seen.values())
    .filter(item => item.score >= 30)
    .slice(0, 250)
    .map(({ score, ...rest }) => rest);

  return { pageTitle, pageUrl, items };
})()`;

const STREAM_EXT_REGEX = /\.(m3u8|mpd|mp4|webm|mov)(\?|$|#)/i;
const IGNORE_EXT_REGEX = /\.(ts|m4s|aac|vtt|srt|jpg|jpeg|png|webp|gif|svg|css|js|woff2?|ico)(\?|$|#)/i;

function classifyStreamCandidate(url, mimeType = '') {
  if (!url || url.startsWith('blob:') || url.startsWith('data:')) return 0;
  const lower = url.toLowerCase();
  const mime = (mimeType || '').toLowerCase();

  if (IGNORE_EXT_REGEX.test(lower) && !mime.includes('mpegurl') && !mime.includes('dash')) {
    return 0;
  }

  if (lower.includes('.m3u8') || mime.includes('mpegurl')) {
    if (lower.includes('master') || lower.includes('index') || lower.includes('playlist')) return 100;
    return 90;
  }
  if (lower.includes('.mpd') || mime.includes('dash+xml')) return 85;
  if (STREAM_EXT_REGEX.test(lower) || mime.startsWith('video/')) {
    // Prefer non-preview mp4 if possible
    if (lower.includes('preview') || lower.includes('thumb')) return 40;
    return 75;
  }
  return 0;
}

async function runCdpScan(url, cookiesFilePath) {
  const browserInfo = await ensureCdpBrowser({ visible: false });
  const browserWs = await openWebSocket(browserInfo.webSocketDebuggerUrl);

  let targetId = null;
  let pageWs = null;

  try {
    const created = await sendCdpCommand(browserWs, 1, 'Target.createTarget', { url: 'about:blank' });
    targetId = created.targetId;
    pageWs = await openWebSocket(`ws://127.0.0.1:${SCRAPER_CDP_PORT}/devtools/page/${targetId}`);

    let cmdId = 10;
    await sendCdpCommand(pageWs, ++cmdId, 'Page.enable');
    await sendCdpCommand(pageWs, ++cmdId, 'Network.enable');
    await sendCdpCommand(pageWs, ++cmdId, 'Runtime.enable');

    // Inject cookies from cookies.txt if present for this domain
    const fileCookies = getCookiesForUrl(cookiesFilePath, url);
    if (fileCookies.length > 0) {
      try {
        await sendCdpCommand(pageWs, ++cmdId, 'Network.setCookies', {
          cookies: fileCookies.map(c => ({
            name: c.name,
            value: c.value,
            domain: c.domain,
            path: c.path || '/',
            secure: c.secure,
            url
          }))
        });
      } catch (_) {}
    }

    const loadedPromise = new Promise((resolve) => {
      const onMsg = (ev) => {
        try {
          const m = JSON.parse(ev.data);
          if (m.method === 'Page.loadEventFired') {
            pageWs.removeEventListener('message', onMsg);
            resolve();
          }
        } catch (_) {}
      };
      pageWs.addEventListener('message', onMsg);
      setTimeout(resolve, 14000);
    });

    await sendCdpCommand(pageWs, ++cmdId, 'Page.navigate', { url });
    await loadedPromise;
    // Allow dynamic JS cards to render
    await new Promise(r => setTimeout(r, 2200));

    const evalRes = await sendCdpCommand(pageWs, ++cmdId, 'Runtime.evaluate', {
      expression: DOM_VIDEO_EXTRACTOR_SCRIPT,
      returnByValue: true
    });

    const value = evalRes && evalRes.result && evalRes.result.value ? evalRes.result.value : { pageTitle: url, pageUrl: url, items: [] };
    return value;
  } finally {
    try { if (pageWs) pageWs.close(); } catch (_) {}
    if (targetId) {
      try { await sendCdpCommand(browserWs, 999, 'Target.closeTarget', { targetId }); } catch (_) {}
    }
    try { browserWs.close(); } catch (_) {}
  }
}

async function runCdpSniff(url, cookiesFilePath, timeoutMs = 35000, abortSignal = null, { excludeUrls = [] } = {}) {
  if (abortSignal && abortSignal.aborted) {
    throw new Error('Cancelled by user');
  }

  const browserInfo = await ensureCdpBrowser({ visible: false });
  const browserWs = await openWebSocket(browserInfo.webSocketDebuggerUrl);

  let targetId = null;
  let pageWs = null;

  try {
    const created = await sendCdpCommand(browserWs, 1, 'Target.createTarget', { url: 'about:blank' });
    targetId = created.targetId;
    pageWs = await openWebSocket(`ws://127.0.0.1:${SCRAPER_CDP_PORT}/devtools/page/${targetId}`);

    let cmdId = 10;
    await sendCdpCommand(pageWs, ++cmdId, 'Page.enable');
    await sendCdpCommand(pageWs, ++cmdId, 'Network.enable');
    await sendCdpCommand(pageWs, ++cmdId, 'Runtime.enable');
    await sendCdpCommand(pageWs, ++cmdId, 'Network.setCacheDisabled', { cacheDisabled: true }).catch(() => {});
    await sendCdpCommand(pageWs, ++cmdId, 'Network.clearBrowserCache').catch(() => {});

    const fileCookies = getCookiesForUrl(cookiesFilePath, url);
    if (fileCookies.length > 0) {
      try {
        await sendCdpCommand(pageWs, ++cmdId, 'Network.setCookies', {
          cookies: fileCookies.map(c => ({
            name: c.name,
            value: c.value,
            domain: c.domain,
            path: c.path || '/',
            secure: c.secure,
            url
          }))
        });
      } catch (_) {}
    }

    let bestCandidate = null; // { url, priority, referer, userAgent }
    const requestHeadersMap = new Map();

    const result = await new Promise((resolve, reject) => {
      let settled = false;
      let settleTimer = null;
      let playInterval = null;
      let overallTimer = null;
      let reloadTimer = null;

      const finish = async () => {
        if (settled) return;
        settled = true;
        clearTimeout(settleTimer);
        clearTimeout(overallTimer);
        clearTimeout(reloadTimer);
        clearInterval(playInterval);

        if (!bestCandidate) {
          return reject(new Error('No video stream (.m3u8/.mp4) detected on page before timeout.'));
        }

        let pageTitle = '';
        let cookieHeader = '';
        try {
          const tRes = await sendCdpCommand(pageWs, ++cmdId, 'Runtime.evaluate', {
            expression: 'document.title',
            returnByValue: true
          });
          pageTitle = (tRes && tRes.result && tRes.result.value) ? String(tRes.result.value).trim() : '';
        } catch (_) {}

        try {
          const cRes = await sendCdpCommand(pageWs, ++cmdId, 'Network.getCookies', {
            urls: [url, bestCandidate.url]
          });
          if (cRes && Array.isArray(cRes.cookies) && cRes.cookies.length > 0) {
            cookieHeader = cRes.cookies.map(c => `${c.name}=${c.value}`).join('; ');
          }
        } catch (_) {}

        resolve({
          mediaUrl: bestCandidate.url,
          title: pageTitle,
          referer: bestCandidate.referer || url,
          userAgent: bestCandidate.userAgent || '',
          cookieHeader
        });
      };

      const fail = (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(settleTimer);
        clearTimeout(overallTimer);
        clearTimeout(reloadTimer);
        clearInterval(playInterval);
        reject(err);
      };

      if (abortSignal) {
        abortSignal.addEventListener('abort', () => fail(new Error('Cancelled by user')), { once: true });
      }

      overallTimer = setTimeout(() => {
        if (bestCandidate) finish();
        else fail(new Error('Timed out waiting for video stream request on page.'));
      }, timeoutMs);

      const isExcluded = (candidateUrl) => {
        if (!candidateUrl || !Array.isArray(excludeUrls) || excludeUrls.length === 0) return false;
        return excludeUrls.some(ex => {
          if (!ex) return false;
          return candidateUrl === ex;
        });
      };

      const considerCandidate = (candUrl, mimeType, reqHeaders = {}) => {
        if (isExcluded(candUrl)) return;
        const prio = classifyStreamCandidate(candUrl, mimeType);
        if (prio <= 0) return;

        if (!bestCandidate || prio > bestCandidate.priority) {
          bestCandidate = {
            url: candUrl,
            priority: prio,
            referer: reqHeaders.Referer || reqHeaders.referer || url,
            userAgent: reqHeaders['User-Agent'] || reqHeaders['user-agent'] || ''
          };

          // Wait briefly in case a higher-priority master playlist follows right after
          clearTimeout(settleTimer);
          settleTimer = setTimeout(finish, prio >= 90 ? 1200 : 2500);
        }
      };

      pageWs.addEventListener('message', (ev) => {
        try {
          const m = JSON.parse(ev.data);
          if (m.method === 'Network.requestWillBeSent' && m.params) {
            const req = m.params.request || {};
            if (m.params.requestId && req.headers) {
              requestHeadersMap.set(m.params.requestId, req.headers);
            }
            considerCandidate(req.url, '', req.headers || {});
          } else if (m.method === 'Network.responseReceived' && m.params) {
            const resp = m.params.response || {};
            const reqHeaders = requestHeadersMap.get(m.params.requestId) || {};
            considerCandidate(resp.url, resp.mimeType, reqHeaders);
          }
        } catch (_) {}
      });

      // Periodically mute, auto-click Play (trusted CDP mouse click + DOM play), & inspect <video> src
      let autoClickCount = 0;
      playInterval = setInterval(async () => {
        if (settled || (bestCandidate && !isExcluded(bestCandidate.url))) return;
        try {
          const evalOut = await sendCdpCommand(pageWs, ++cmdId, 'Runtime.evaluate', {
            expression: `(() => {
              let foundSrc = '';
              let clickPoint = null;

              function scanDoc(doc, offsetX = 0, offsetY = 0) {
                if (!doc) return;
                doc.querySelectorAll('video, audio').forEach(el => {
                  el.muted = true;
                  el.volume = 0;
                  if (el.tagName === 'VIDEO') {
                    if (el.paused) {
                      try { el.play().catch(() => {}); } catch (_) {}
                    }
                    const s = el.currentSrc || el.getAttribute('src') || '';
                    if (s && !s.startsWith('blob:')) foundSrc = s;
                    if (!clickPoint) {
                      const r = el.getBoundingClientRect();
                      if (r.width >= 100 && r.height >= 80 && r.bottom > 0 && r.right > 0) {
                        clickPoint = {
                          x: Math.round(offsetX + r.left + r.width / 2),
                          y: Math.round(offsetY + r.top + r.height / 2)
                        };
                      }
                    }
                  }
                });

                const playSelectors = [
                  '.vjs-big-play-button',
                  '.plyr__control--overlaid',
                  '.jw-icon-display',
                  '.fp-play',
                  '#play_button',
                  '[id*="play_button" i]',
                  '[id*="play-button" i]',
                  '[id*="play_btn" i]',
                  '[id*="play-btn" i]',
                  '[id*="playBtn" i]',
                  'button[aria-label*="Play" i]',
                  '[role="button"][aria-label*="Play" i]',
                  '[title*="Play" i]',
                  '[class*="play-btn" i]',
                  '[class*="play-button" i]',
                  '[class*="playIcon" i]',
                  '[class*="play_btn" i]',
                  '[class*="video-play" i]',
                  '[class*="play-overlay" i]',
                  '[data-action="play"]'
                ];
                function tryUsePlayEl(btn) {
                  const r = btn.getBoundingClientRect();
                  if (r.width <= 8 || r.height <= 8) return false;
                  const style = doc.defaultView ? doc.defaultView.getComputedStyle(btn) : null;
                  if (style && (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0')) return false;
                  try { btn.scrollIntoView({ block: 'center', inline: 'center' }); } catch (_) {}
                  const r2 = btn.getBoundingClientRect();
                  const cx = Math.round(offsetX + r2.left + r2.width / 2);
                  const cy = Math.round(offsetY + r2.top + r2.height / 2);
                  if (cx > 0 && cy > 0) {
                    clickPoint = { x: cx, y: cy };
                  } else {
                    try { btn.click(); } catch (_) {}
                  }
                  return true;
                }

                for (const sel of playSelectors) {
                  const btns = doc.querySelectorAll(sel);
                  for (const btn of btns) {
                    if (tryUsePlayEl(btn)) break;
                  }
                  if (clickPoint) break;
                }

                if (!clickPoint) {
                  const textBtns = doc.querySelectorAll('button, a, [role="button"]');
                  for (const btn of textBtns) {
                    if (btn.closest('header, nav, footer')) continue;
                    const txt = (btn.innerText || btn.textContent || '').replace(/\s+/g, ' ').trim();
                    if (/^(play|play video|watch|watch video|start video|continue|enter|i am 18\+?)$/i.test(txt)) {
                      if (tryUsePlayEl(btn)) break;
                    }
                  }
                }
              }

              scanDoc(document, 0, 0);
              document.querySelectorAll('iframe').forEach(ifr => {
                try {
                  const ir = ifr.getBoundingClientRect();
                  if (ifr.contentDocument) {
                    scanDoc(ifr.contentDocument, ir.left, ir.top);
                  } else if (!clickPoint && ir.width >= 180 && ir.height >= 120) {
                    clickPoint = {
                      x: Math.round(ir.left + ir.width / 2),
                      y: Math.round(ir.top + ir.height / 2)
                    };
                  }
                } catch (_) {}
              });

              return { foundSrc, clickPoint };
            })()`,
            returnByValue: true
          });

          const val = evalOut && evalOut.result && evalOut.result.value;
          if (val && val.foundSrc && !isExcluded(val.foundSrc)) {
            considerCandidate(val.foundSrc, '', { Referer: url });
          }
          if ((!bestCandidate || isExcluded(bestCandidate.url)) && val && val.clickPoint && autoClickCount < 4) {
            autoClickCount++;
            const { x, y } = val.clickPoint;
            if (x > 0 && y > 0) {
              await sendCdpCommand(pageWs, ++cmdId, 'Input.dispatchMouseEvent', { type: 'mouseMoved', x, y }).catch(() => {});
              await sendCdpCommand(pageWs, ++cmdId, 'Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 }).catch(() => {});
              await sendCdpCommand(pageWs, ++cmdId, 'Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 }).catch(() => {});
            }
          }
        } catch (_) {}
      }, 2000);

      reloadTimer = setTimeout(() => {
        if (!settled && (!bestCandidate || isExcluded(bestCandidate.url))) {
          sendCdpCommand(pageWs, ++cmdId, 'Page.reload', { ignoreCache: true }).catch(() => {});
        }
      }, 18000);

      sendCdpCommand(pageWs, ++cmdId, 'Page.navigate', { url }).catch(fail);
    });

    return result;
  } finally {
    try { if (pageWs) pageWs.close(); } catch (_) {}
    if (targetId) {
      try { await sendCdpCommand(browserWs, 999, 'Target.closeTarget', { targetId }); } catch (_) {}
    }
    try { browserWs.close(); } catch (_) {}
  }
}

// ── Public Unified Scan & Sniff Entrypoints ──────────────────────────────────
async function scanWebpage({ url, mode = 'auto', cookiesFilePath }) {
  if (!url || typeof url !== 'string') {
    throw new Error('Valid webpage URL is required.');
  }

  const useExt = mode === 'extension' || (mode === 'auto' && isBridgeConnected());
  if (mode === 'extension' && !isBridgeConnected()) {
    throw new Error('Browser Extension Bridge is not connected. Load the extension/ folder in Chrome/Edge or switch Sniffer Mode to Auto / Local Browser.');
  }

  if (useExt) {
    const data = await dispatchBridgeTask('scan', url, {}, 40000);
    return { ...data, via: 'extension' };
  }

  const data = await runCdpScan(url, cookiesFilePath);
  return { ...data, via: 'cdp' };
}

async function sniffMediaStream({ url, mode = 'auto', cookiesFilePath, abortSignal = null, excludeUrls = [] }) {
  if (!url || typeof url !== 'string') {
    throw new Error('Valid video page URL is required.');
  }

  // If the URL itself is already a direct stream file, no sniffing needed unless excluded
  if (/\.(m3u8|mpd|mp4|webm|mov)(\?|$)/i.test(url) && (!excludeUrls || !excludeUrls.includes(url))) {
    return {
      mediaUrl: url,
      title: '',
      referer: '',
      userAgent: '',
      cookieHeader: '',
      via: 'direct'
    };
  }

  const useExt = mode === 'extension' || (mode === 'auto' && isBridgeConnected());
  if (mode === 'extension' && !isBridgeConnected()) {
    throw new Error('Browser Extension Bridge is not connected.');
  }

  if (useExt) {
    const data = await dispatchBridgeTask('sniff', url, { excludeUrls }, 50000, abortSignal);
    return { ...data, via: 'extension' };
  }

  const data = await runCdpSniff(url, cookiesFilePath, 45000, abortSignal, { excludeUrls });
  return { ...data, via: 'cdp' };
}

module.exports = {
  isBridgeConnected,
  getBridgeState,
  handleBridgePoll,
  handleBridgeResult,
  getCookiesSummary,
  getCookiesForUrl,
  saveGenericCookies,
  clearDomainCookies,
  ensureCdpBrowser,
  scanWebpage,
  sniffMediaStream
};

