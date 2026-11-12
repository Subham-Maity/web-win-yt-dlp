const SERVER_BASE = 'http://127.0.0.1:3456';

const STREAM_EXT_RE = /\.(m3u8|mpd|mp4|webm|mov)(\?|$|#)/i;
const IGNORE_EXT_RE = /\.(ts|m4s|aac|vtt|srt|jpg|jpeg|png|webp|gif|svg|css|js|woff2?|ico)(\?|$|#)/i;

// Shared DOM extractor evaluated inside the webpage tab
function extractVideosFromDom() {
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
    } catch (_) {
      return '';
    }
  }

  function cleanText(s) {
    return (s || '').replace(/\s+/g, ' ').trim();
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

    let duration = '';
    const cardText = cleanText(card.textContent || '');
    const durMatch = cardText.match(/\b(\d{1,2}:\d{2}(?::\d{2})?)\b/);
    if (durMatch) duration = durMatch[1];

    if (!hasVideoHint && !thumb && !duration) return;
    if (!thumb && /([?&]page=|\/page\/|\/tag\/|\/category\/)/i.test(lower)) return;

    let title = cleanText(
      a.getAttribute('title') ||
      (img && (img.getAttribute('alt') || img.getAttribute('title'))) ||
      a.textContent ||
      ''
    );

    if (!title || title.length < 2) {
      const heading = card.querySelector('h1,h2,h3,h4,.title,[class*="title"],[class*="name"]');
      if (heading) title = cleanText(heading.textContent);
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
        isDirectMedia: /\.(m3u8|mp4|webm|mov|mpd)(\?|$)/i.test(lower),
        score
      });
    }
  });

  const items = Array.from(seen.values())
    .filter(item => item.score >= 30)
    .slice(0, 250)
    .map(({ score, ...rest }) => rest);

  return { pageTitle, pageUrl, items };
}

function classifyStreamCandidate(url, mimeType = '') {
  if (!url || url.startsWith('blob:') || url.startsWith('data:')) return 0;
  const lower = url.toLowerCase();
  const mime = (mimeType || '').toLowerCase();

  if (IGNORE_EXT_RE.test(lower) && !mime.includes('mpegurl') && !mime.includes('dash')) {
    return 0;
  }

  if (lower.includes('.m3u8') || mime.includes('mpegurl')) {
    if (lower.includes('master') || lower.includes('index') || lower.includes('playlist')) return 100;
    return 90;
  }
  if (lower.includes('.mpd') || mime.includes('dash+xml')) return 85;
  if (STREAM_EXT_RE.test(lower) || mime.startsWith('video/')) {
    if (lower.includes('preview') || lower.includes('thumb')) return 40;
    return 75;
  }
  return 0;
}

function waitForTabComplete(tabId, timeoutMs = 15000) {
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      chrome.tabs.onUpdated.removeListener(onUpdated);
      resolve();
    };
    const onUpdated = (id, info) => {
      if (id === tabId && info.status === 'complete') {
        finish();
      }
    };
    chrome.tabs.onUpdated.addListener(onUpdated);
    setTimeout(finish, timeoutMs);
  });
}

async function handleScanTask(url) {
  const tab = await chrome.tabs.create({ url, active: false });
  await chrome.tabs.update(tab.id, { muted: true }).catch(() => {});
  try {
    await waitForTabComplete(tab.id, 15000);
    await new Promise(r => setTimeout(r, 2000));
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func: extractVideosFromDom
    });
    return result || { pageTitle: url, pageUrl: url, items: [] };
  } finally {
    await chrome.tabs.remove(tab.id).catch(() => {});
  }
}

async function handleSniffTask(url) {
  const tab = await chrome.tabs.create({ url: 'about:blank', active: false });
  await chrome.tabs.update(tab.id, { muted: true }).catch(() => {});
  const tabId = tab.id;

  let bestCandidate = null;
  const reqHeadersByReqId = new Map();

  return new Promise((resolve, reject) => {
    let settled = false;
    let settleTimer = null;
    let overallTimer = null;
    let playInterval = null;

    const cleanupListeners = () => {
      chrome.webRequest.onBeforeSendHeaders.removeListener(onBeforeSend);
      chrome.webRequest.onHeadersReceived.removeListener(onHeadersReceived);
      clearTimeout(settleTimer);
      clearTimeout(overallTimer);
      clearInterval(playInterval);
    };

    const finish = async () => {
      if (settled) return;
      settled = true;
      cleanupListeners();

      if (!bestCandidate) {
        await chrome.tabs.remove(tabId).catch(() => {});
        return reject(new Error('No media stream (.m3u8/.mp4) captured before timeout.'));
      }

      let title = '';
      try {
        const tInfo = await chrome.tabs.get(tabId);
        title = (tInfo && tInfo.title) ? tInfo.title.trim() : '';
      } catch (_) {}

      let cookieHeader = '';
      try {
        const list = await chrome.cookies.getAll({ url: bestCandidate.url });
        const pageCookies = await chrome.cookies.getAll({ url });
        const merged = new Map();
        for (const c of [...pageCookies, ...list]) merged.set(c.name, c.value);
        if (merged.size > 0) {
          cookieHeader = Array.from(merged.entries()).map(([k, v]) => `${k}=${v}`).join('; ');
        }
      } catch (_) {}

      await chrome.tabs.remove(tabId).catch(() => {});
      resolve({
        mediaUrl: bestCandidate.url,
        title,
        referer: bestCandidate.referer || url,
        userAgent: bestCandidate.userAgent || navigator.userAgent,
        cookieHeader
      });
    };

    const consider = (candUrl, mimeType, headers = {}) => {
      const prio = classifyStreamCandidate(candUrl, mimeType);
      if (prio <= 0) return;

      if (!bestCandidate || prio > bestCandidate.priority) {
        bestCandidate = {
          url: candUrl,
          priority: prio,
          referer: headers.referer || url,
          userAgent: headers.userAgent || ''
        };
        clearTimeout(settleTimer);
        settleTimer = setTimeout(finish, prio >= 90 ? 1200 : 2500);
      }
    };

    const onBeforeSend = (details) => {
      if (details.tabId !== tabId) return;
      const hdrs = {};
      for (const h of (details.requestHeaders || [])) {
        const n = h.name.toLowerCase();
        if (n === 'referer') hdrs.referer = h.value;
        if (n === 'user-agent') hdrs.userAgent = h.value;
      }
      reqHeadersByReqId.set(details.requestId, hdrs);
      consider(details.url, '', hdrs);
    };

    const onHeadersReceived = (details) => {
      if (details.tabId !== tabId) return;
      let mime = '';
      for (const h of (details.responseHeaders || [])) {
        if (h.name.toLowerCase() === 'content-type') {
          mime = h.value || '';
          break;
        }
      }
      const hdrs = reqHeadersByReqId.get(details.requestId) || {};
      consider(details.url, mime, hdrs);
    };

    chrome.webRequest.onBeforeSendHeaders.addListener(
      onBeforeSend,
      { urls: ['<all_urls>'], tabId },
      ['requestHeaders']
    );

    chrome.webRequest.onHeadersReceived.addListener(
      onHeadersReceived,
      { urls: ['<all_urls>'], tabId },
      ['responseHeaders']
    );

    overallTimer = setTimeout(() => {
      if (bestCandidate) finish();
      else {
        settled = true;
        cleanupListeners();
        chrome.tabs.remove(tabId).catch(() => {});
        reject(new Error('Timed out waiting for video stream request in browser tab.'));
      }
    }, 35000);

    playInterval = setInterval(async () => {
      if (settled) return;
      try {
        const [{ result: domSrc }] = await chrome.scripting.executeScript({
          target: { tabId },
          func: () => {
            let found = '';
            document.querySelectorAll('video, audio').forEach(el => {
              el.muted = true;
              el.volume = 0;
              if (el.tagName === 'VIDEO') {
                if (el.paused) el.play().catch(() => {});
                const s = el.currentSrc || el.getAttribute('src') || '';
                if (s && !s.startsWith('blob:')) found = s;
              }
            });
            const playBtn = document.querySelector('.vjs-big-play-button, .plyr__control--overlaid, [class*="play-btn"], button[aria-label*="Play" i]');
            if (playBtn) { try { playBtn.click(); } catch (_) {} }
            return found;
          }
        });
        if (domSrc) consider(domSrc, '', { referer: url });
      } catch (_) {}
    }, 1500);

    chrome.tabs.update(tabId, { url }).catch((err) => {
      if (!settled) {
        settled = true;
        cleanupListeners();
        chrome.tabs.remove(tabId).catch(() => {});
        reject(err);
      }
    });
  });
}

// ── Long-Polling Loop to Local Server ────────────────────────────────────────
let pollingActive = false;

async function pollLoop() {
  if (pollingActive) return;
  pollingActive = true;

  while (pollingActive) {
    try {
      const res = await fetch(`${SERVER_BASE}/scraper/bridge/poll?extVersion=1.0`);
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const payload = await res.json();

      if (payload && payload.hasTask && payload.task) {
        const { taskId, action, url } = payload.task;
        try {
          let data;
          if (action === 'scan') data = await handleScanTask(url);
          else if (action === 'sniff') data = await handleSniffTask(url);
          else throw new Error('Unknown task action: ' + action);

          await fetch(`${SERVER_BASE}/scraper/bridge/result`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ taskId, ok: true, data })
          });
        } catch (taskErr) {
          await fetch(`${SERVER_BASE}/scraper/bridge/result`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ taskId, ok: false, error: taskErr.message || String(taskErr) })
          }).catch(() => {});
        }
      }
    } catch (_) {
      // Server offline; back off briefly
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

chrome.runtime.onInstalled.addListener(() => {
  chrome.alarms.create('bridgeKeepAlive', { periodInMinutes: 0.4 });
  pollLoop();
});

chrome.runtime.onStartup.addListener(() => {
  chrome.alarms.create('bridgeKeepAlive', { periodInMinutes: 0.4 });
  pollLoop();
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === 'bridgeKeepAlive') {
    pollLoop();
  }
});

// Handle popup actions ("Scan Current Tab & Send to Web UI")
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg && msg.type === 'SCAN_ACTIVE_TAB') {
    (async () => {
      try {
        const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
        if (!tab || !tab.id || !tab.url || !tab.url.startsWith('http')) {
          throw new Error('Open a webpage (http/https) first.');
        }
        const [{ result }] = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          func: extractVideosFromDom
        });
        const scanResult = result || { pageTitle: tab.title, pageUrl: tab.url, items: [] };
        const res = await fetch(`${SERVER_BASE}/scraper/push-scan`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(scanResult)
        });
        if (!res.ok) throw new Error('Local server rejected push-scan');
        sendResponse({ ok: true, count: scanResult.items.length });
      } catch (err) {
        sendResponse({ ok: false, error: err.message });
      }
    })();
    return true;
  }
});

pollLoop();
