const express = require('express');
const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');
const crypto = require('crypto');
const { Worker } = require('bullmq');

const { redisConnection, downloadQueue, scrapeQueue, activeProcesses, isRedisReady, isRedisAlive } = require('./queue');
const { extractInstagramUsername, downloadInstagramProfile, isInstaloaderAvailable } = require('./instagramService');
const {
  startInstagramLoginPopup,
  cancelInstagramLoginPopup,
  openInstagramInBrowser,
  getInstagramSessionStatus,
  clearInstagramSession,
  saveManualSession,
  findBrowserExe
} = require('./instagramAuthService');
const {
  isBridgeConnected,
  getBridgeState,
  handleBridgePoll,
  handleBridgeResult,
  getCookiesSummary,
  saveGenericCookies,
  clearDomainCookies,
  ensureCdpBrowser,
  scanWebpage,
  sniffMediaStream
} = require('./scraperService');

const app = express();
const PORT = 3456;

// ── Process-Level Socket / Error Guards ──────────────────────────────────────
process.on('uncaughtException', (err) => {
  console.warn('  ⚠️ Handled Uncaught Exception:', err && err.message ? err.message : err);
});

process.on('unhandledRejection', (reason) => {
  console.warn('  ⚠️ Handled Unhandled Rejection:', reason && reason.message ? reason.message : reason);
});

// ── Paths ──────────────────────────────────────────────────────────────────
const YTDLP_EXE     = path.join(__dirname, 'yt-dlp.exe');
const GALLERYDL_EXE = path.join(__dirname, 'gallery-dl.exe');
const COOKIES       = path.join(__dirname, 'cookies.txt');
const QUEUE_STATE_FILE = path.join(__dirname, 'queue-state.json');
const DOWNLOAD_DIR  = path.join(os.homedir(), 'Downloads', 'yt-dlp');

// Ensure download directories exist
if (!fs.existsSync(DOWNLOAD_DIR)) {
  fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
}
const IG_DOWNLOAD_DIR = path.join(DOWNLOAD_DIR, 'Instagram');
if (!fs.existsSync(IG_DOWNLOAD_DIR)) {
  fs.mkdirSync(IG_DOWNLOAD_DIR, { recursive: true });
}

// ── In-Memory Job Snapshot (Synchronized with BullMQ & SSE) ──────────────────
// Map of jobId -> { id, type, url, status, progress, title, error, log, badge }
const jobs = new Map();
const sseClients = new Set();

function broadcast(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(msg); } catch (_) {}
  }
}

app.use(express.json({ limit: '5mb' }));
app.use((req, res, next) => {
  if (req.path.startsWith('/scraper/bridge') || req.path === '/scraper/push-scan') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
  }
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

function sanitizeFileName(name) {
  return String(name || '')
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 120);
}

function extractOutputIdFromUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') return '';
  try {
    const u = new URL(rawUrl);
    const segs = u.pathname.split('/').filter(Boolean);
    // Prefer a numeric or alphanumeric media ID segment (skip generic words like 'play', 'watch', 'video')
    const idSeg = [...segs].reverse().find(s => /^[0-9]{4,}$/.test(s))
      || [...segs].reverse().find(s => !/^(play|watch|video|videos|embed|index)$/i.test(s) && /^[A-Za-z0-9_-]{4,24}$/.test(s));
    if (idSeg) return idSeg;
  } catch (_) {}
  return crypto.createHash('sha1').update(String(rawUrl)).digest('hex').slice(0, 8);
}

let saveQueueTimer = null;
function saveQueueStateNow() {
  try {
    const list = [...jobs.entries()].map(([id, j]) => ({
      ...serializeJob(id, j),
      mode: j.mode || 'auto',
      useCookies: j.useCookies !== false
    }));
    const tmp = QUEUE_STATE_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ savedAt: Date.now(), jobs: list }, null, 2), 'utf8');
    fs.renameSync(tmp, QUEUE_STATE_FILE);
  } catch (e) {
    console.warn('  ⚠️ Could not persist queue state:', e.message);
  }
}

function scheduleSaveQueueState() {
  if (saveQueueTimer) clearTimeout(saveQueueTimer);
  saveQueueTimer = setTimeout(() => {
    saveQueueTimer = null;
    saveQueueStateNow();
  }, 250);
}

// ── yt-dlp Worker Handler ────────────────────────────────────────────────────
function runYtdlpDownload(job) {
  return new Promise((resolve, reject) => {
    const { url, resolvedHeaders, customOutputTitle, customOutputId, useCookies = true, abortSignal = null } = job.data;
    const id = job.id;

    if (abortSignal && abortSignal.aborted) {
      return reject(new Error('Cancelled by user'));
    }

    const j = jobs.get(id);
    if (j) {
      j.status = 'downloading';
      j.log = 'Connecting to stream source...';
      broadcast({ type: 'job', ...j });
    }

    const safeCustomTitle = sanitizeFileName(customOutputTitle);
    const safeCustomId = sanitizeFileName(customOutputId);
    const outTemplate = safeCustomTitle
      ? path.join(DOWNLOAD_DIR, `${safeCustomTitle} [${safeCustomId || '%(id)s'}].%(ext)s`)
      : path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].%(ext)s');

    const args = [
      '--no-playlist',
      '--socket-timeout', '20',
      '--retries', '3',
      '--fragment-retries', '3',
      '--js-runtimes', `node:${process.execPath}`,
      '-o', outTemplate,
      '--newline',
      '--progress-template', '%(progress._percent_str)s|%(info.title)s|%(progress._speed_str)s|%(progress._eta_str)s',
    ];

    if (resolvedHeaders && resolvedHeaders.referer) {
      args.push('--referer', resolvedHeaders.referer);
    }
    if (resolvedHeaders && resolvedHeaders.userAgent) {
      args.push('--user-agent', resolvedHeaders.userAgent);
    }
    if (resolvedHeaders && resolvedHeaders.cookieHeader) {
      args.push('--add-header', `Cookie:${resolvedHeaders.cookieHeader}`);
    }

    let tempCookiesPath = null;
    if (useCookies && fs.existsSync(COOKIES)) {
      try {
        tempCookiesPath = path.join(os.tmpdir(), `ytdlp_cookies_${id}.txt`);
        fs.copyFileSync(COOKIES, tempCookiesPath);
        args.push('--cookies', tempCookiesPath);
      } catch (_) {
        args.push('--cookies', COOKIES);
      }
    }

    args.push(url);

    const proc = spawn(YTDLP_EXE, args, { windowsHide: true });
    activeProcesses.set(id, proc);

    let progress = 0;
    let title = job.data.title || url;

    proc.stdout.on('data', (chunk) => {
      const lines = chunk.toString().split(/[\r\n]+/);
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        // Custom template: "XX.X%|Title|Speed|ETA"
        if (trimmed.includes('|')) {
          const parts = trimmed.split('|');
          if (parts.length >= 2) {
            const pct = parts[0];
            const extractedTitle = parts[1];
            const speed = parts[2];
            const eta = parts[3];
            const num = parseFloat(pct.replace('%', ''));
            if (!isNaN(num)) {
              progress = num;
              const isGenericTitle = !extractedTitle || extractedTitle === 'NA' || /^(index|master|playlist|chunklist|video|stream)$/i.test(extractedTitle.trim());
              if (!customOutputTitle && !isGenericTitle) {
                title = extractedTitle;
              }
              let logMsg = `Speed: ${speed || 'N/A'} • ETA: ${eta || 'N/A'}`;
              if (job && typeof job.updateProgress === 'function') {
                try { job.updateProgress({ progress, title, log: logMsg }); } catch (_) {}
              }
              const item = jobs.get(id);
              if (item) {
                item.progress = progress;
                item.title = title;
                item.log = logMsg;
                item.status = 'downloading';
                item.stage = 'downloading';
              }
              broadcast({ type: 'progress', id, status: 'downloading', stage: 'downloading', progress, title, log: logMsg });
              continue;
            }
          }
        }

        if (trimmed.startsWith('[download]')) {
          const m = trimmed.match(/(\d+\.?\d*)%/);
          if (m) {
            progress = parseFloat(m[1]);
          }
          const item = jobs.get(id);
          if (item) {
            item.progress = progress;
            item.log = trimmed;
            item.status = 'downloading';
            item.stage = 'downloading';
          }
          broadcast({ type: 'progress', id, status: 'downloading', stage: 'downloading', progress, title: item ? item.title : title, log: trimmed });
        } else {
          // General informational line (e.g. "[info] Extracting URL: ...")
          const item = jobs.get(id);
          if (item) {
            item.log = trimmed;
            item.status = 'downloading';
            item.stage = 'downloading';
          }
          broadcast({ type: 'progress', id, status: 'downloading', stage: 'downloading', progress, title: item ? item.title : title, log: trimmed });
        }
      }
    });

    let stderrBuf = '';
    proc.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderrBuf += text;
      const lastLine = text.split(/[\r\n]+/).map(l => l.trim()).filter(Boolean).pop();
      if (lastLine && !lastLine.includes('frame=') && !lastLine.includes('fps=')) {
        const item = jobs.get(id);
        if (item) { item.log = lastLine; }
        broadcast({ type: 'progress', id, status: 'downloading', progress, title, log: lastLine });
      }
    });

    let settled = false;
    const cleanup = () => {
      activeProcesses.delete(id);
      if (tempCookiesPath && fs.existsSync(tempCookiesPath)) {
        try { fs.unlinkSync(tempCookiesPath); } catch (_) {}
      }
    };

    if (abortSignal) {
      abortSignal.addEventListener('abort', () => {
        if (settled) return;
        settled = true;
        try {
          proc.kill('SIGTERM');
          if (proc.pid) {
            spawn('taskkill', ['/pid', proc.pid.toString(), '/f', '/t'], { windowsHide: true });
          }
        } catch (_) {}
        cleanup();
        reject(new Error('Cancelled by user'));
      }, { once: true });
    }

    proc.on('close', (code) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (code === 0) {
        resolve({ title, progress: 100 });
      } else {
        const errLines = stderrBuf.split('\n').map(l => l.trim()).filter(Boolean);
        const lastErr = errLines.reverse().find(l => l.includes('ERROR') || l.includes('error') || l.includes('Unable') || l.includes('Sign in') || l.includes('429')) || errLines[0] || `exit code ${code}`;
        let cleanErr = lastErr.replace(/^ERROR:\s*/i, '');
        if (cleanErr.includes('[instagram:user]') || (cleanErr.includes('Unable to extract data') && cleanErr.includes('instagram'))) {
          cleanErr = 'Instagram profiles cannot be downloaded via standard yt-dlp. Please use the Instagram Profile Downloader tab with your cookies.txt.';
        } else if (cleanErr.includes('empty media response') || cleanErr.includes('Instagram API is not granting access')) {
          cleanErr = 'Instagram requires login cookies. Please export cookies (including sessionid) to cookies.txt.';
        }
        reject(new Error(cleanErr));
      }
    });

    proc.on('error', (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    });
  });
}

// ── BullMQ Worker Setup ──────────────────────────────────────────────────────
const downloadWorker = new Worker('downloadQueue', async (job) => {
  const j = jobs.get(job.id);
  if (j) {
    j.status = 'downloading';
    j.log = 'Worker picked up task...';
    broadcast({ type: 'job', ...j });
  }

  if (job.data.type === 'instagram_profile') {
    // 📸 Instagram Profile Downloader Job
    const { username, cookieSource, mediaScope } = job.data;
    const res = await downloadInstagramProfile({
      jobId: job.id,
      username,
      mediaScope: mediaScope || 'all',
      cookieSource,
      downloadDir: DOWNLOAD_DIR,
      cookiesFile: COOKIES,
      galleryDlExe: GALLERYDL_EXE,
      activeProcesses,
      onProgress: ({ downloadedCount, currentFile, log }) => {
        const title = currentFile ? `[#${downloadedCount}] ${currentFile}` : `Downloaded ${downloadedCount} item(s)`;
        const item = jobs.get(job.id);
        const progress = Math.min(downloadedCount * 3, 95); // dynamic indicator
        if (item) {
          item.progress = progress;
          item.title = title;
          item.log = log || `Downloaded ${downloadedCount} item(s)`;
          item.status = 'downloading';
        }
        broadcast({
          type: 'progress',
          id: job.id,
          status: 'downloading',
          progress,
          title,
          log: log || `Downloaded ${downloadedCount} item(s)`
        });
      }
    });
    return res;
  } else {
    // ⬇️ Standard URL Downloader Job
    return await runYtdlpDownload(job);
  }
}, {
  connection: redisConnection,
  concurrency: 3
});

downloadWorker.on('completed', (job, result) => {
  const j = jobs.get(job.id);
  if (j) {
    j.status = 'done';
    j.progress = 100;
    if (result && result.title) j.title = result.title;
    if (result && result.downloadedCount !== undefined) {
      j.title = `Completed: @${job.data.username} (${result.downloadedCount} photo(s) & video(s) saved)`;
      j.log = `All media downloaded to Downloads/yt-dlp/Instagram/${job.data.username}/`;
    } else {
      j.log = 'Download complete. File saved to Downloads/yt-dlp/';
    }
    broadcast({ type: 'job', ...j });
    scheduleSaveQueueState();
  }
});

downloadWorker.on('failed', (job, err) => {
  const j = jobs.get(job.id);
  if (j) {
    j.status = 'error';
    j.error = err.message || 'Download failed';
    j.log = 'Failed: ' + (err.message || 'Error occurred');
    broadcast({ type: 'job', ...j });
    scheduleSaveQueueState();
  }
});

downloadWorker.on('error', () => {
  // Handled silently when Redis is unavailable
});

// ── Full Webpage Scraper Sequential Worker (Concurrency: 1) ──────────────────
const activeScrapeAborts = new Map();
let directScrapeChain = Promise.resolve();
let scrapeGeneration = 0;

async function runScrapeJob(job) {
  const id = job.id;
  const { pageUrl, mode = 'auto', title: presetTitle, useCookies = true } = job.data;

  const j = jobs.get(id);
  if (j && j.status === 'cancelled') {
    throw new Error('Cancelled by user');
  }

  const controller = new AbortController();
  activeScrapeAborts.set(id, controller);

  try {
    if (j) {
      j.status = 'downloading';
      j.stage = 'sniffing';
      j.log = 'Opening muted tab & auto-clicking Play to capture stream...';
      broadcast({ type: 'job', ...j });
    }

    const sniffed = await sniffMediaStream({
      url: pageUrl,
      mode,
      cookiesFilePath: COOKIES,
      abortSignal: controller.signal
    });

    if (j && j.status === 'cancelled') {
      throw new Error('Cancelled by user');
    }

    const resolvedTitle = sniffed.title || presetTitle || pageUrl;
    if (j) {
      j.title = resolvedTitle;
      j.streamUrl = sniffed.mediaUrl;
      j.stage = 'downloading';
      j.log = `Stream link captured via ${sniffed.via || 'browser'}! Starting yt-dlp download...`;
      broadcast({ type: 'job', ...j });
      scheduleSaveQueueState();
    }

    const result = await runYtdlpDownload({
      id,
      data: {
        url: sniffed.mediaUrl,
        title: resolvedTitle,
        customOutputTitle: resolvedTitle,
        customOutputId: extractOutputIdFromUrl(pageUrl),
        useCookies,
        abortSignal: controller.signal,
        resolvedHeaders: {
          referer: sniffed.referer,
          userAgent: sniffed.userAgent,
          cookieHeader: sniffed.cookieHeader
        }
      }
    });

    return {
      ...result,
      title: resolvedTitle,
      streamUrl: sniffed.mediaUrl
    };
  } finally {
    activeScrapeAborts.delete(id);
  }
}

const scrapeWorker = new Worker('scrapeQueue', async (job) => {
  return await runScrapeJob(job);
}, {
  connection: redisConnection,
  concurrency: 1
});

scrapeWorker.on('completed', (job, result) => {
  const j = jobs.get(job.id);
  if (j) {
    j.status = 'done';
    j.stage = 'done';
    j.progress = 100;
    if (result && result.title) j.title = result.title;
    if (result && result.streamUrl) j.streamUrl = result.streamUrl;
    j.log = 'Download complete. File saved to Downloads/yt-dlp/';
    broadcast({ type: 'job', ...j });
    scheduleSaveQueueState();
  }
});

scrapeWorker.on('failed', (job, err) => {
  const j = jobs.get(job.id);
  if (j && j.status !== 'cancelled') {
    j.status = 'error';
    j.stage = 'error';
    j.error = err.message || 'Scrape download failed';
    j.log = 'Failed: ' + (err.message || 'Error occurred');
    broadcast({ type: 'job', ...j });
    scheduleSaveQueueState();
  }
});

scrapeWorker.on('error', () => {
  // Handled silently when Redis is unavailable
});

function serializeJob(id, j) {
  return {
    id,
    type: j.type,
    url: j.url,
    status: j.status,
    stage: j.stage || j.status,
    progress: j.progress,
    title: j.title,
    error: j.error,
    log: j.log || '',
    badge: j.badge,
    streamUrl: j.streamUrl || '',
    queueIndex: j.queueIndex || null,
    queueTotal: j.queueTotal || null
  };
}

// ── SSE Endpoint ─────────────────────────────────────────────────────────────
app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Send current snapshot
  const snapshot = [...jobs.entries()].map(([id, j]) => serializeJob(id, j));
  res.write(`data: ${JSON.stringify({ type: 'snapshot', jobs: snapshot })}\n\n`);

  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// ── Helper to detect Instagram profile URLs vs single posts ──────────────────
function isInstagramProfileUrl(input) {
  if (!input || typeof input !== 'string') return null;
  const username = extractInstagramUsername(input);
  if (!username) return null;
  // If it contains /p/, /reel/, /tv/, or /stories/, it's an individual media item, not a profile
  if (/(?:\/p\/|\/reel\/|\/reels\/[A-Za-z0-9_-]+|\/tv\/|\/stories\/)/i.test(input)) {
    return null;
  }
  return username;
}

// ── Start standard downloads (YouTube, Reels, direct URLs) ────────────────────
app.post('/download', async (req, res) => {
  const { urls } = req.body;
  if (!Array.isArray(urls) || urls.length === 0) {
    return res.status(400).json({ error: 'No URLs provided' });
  }

  const started = [];
  const createdJobs = [];

  for (const rawUrl of urls) {
    const url = rawUrl.trim();
    if (!url) continue;

    // Auto-detect Instagram profile URLs pasted in standard downloader
    const igProfileUser = isInstagramProfileUrl(url);

    if (igProfileUser) {
      const id = `ig_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
      const jobData = {
        id,
        type: 'instagram_profile',
        username: igProfileUser,
        url: `https://www.instagram.com/${igProfileUser}/`,
        status: 'queued',
        progress: 0,
        title: `Instagram Profile: @${igProfileUser}`,
        log: 'Auto-routed to Instagram Profile Worker...',
        error: '',
        badge: 'Instagram Profile Download'
      };
      jobs.set(id, jobData);
      started.push(id);
      createdJobs.push(jobData);
      broadcast({ type: 'job', ...jobData });

      const runDirectIg = () => {
        jobData.status = 'downloading';
        jobData.log = 'Starting scraper directly...';
        broadcast({ type: 'job', ...jobData });
        downloadInstagramProfile({
          jobId: id,
          username: igProfileUser,
          cookieSource: 'cookies.txt',
          downloadDir: DOWNLOAD_DIR,
          cookiesFile: COOKIES,
          galleryDlExe: GALLERYDL_EXE,
          activeProcesses,
          onProgress: ({ downloadedCount, currentFile, log }) => {
            jobData.progress = Math.min(downloadedCount * 5, 95);
            jobData.title = currentFile ? `[#${downloadedCount}] ${currentFile}` : `Downloaded ${downloadedCount} videos`;
            jobData.log = log || `Downloaded ${downloadedCount} videos`;
            broadcast({
              type: 'progress',
              id,
              status: 'downloading',
              progress: jobData.progress,
              title: jobData.title,
              log: jobData.log
            });
          }
        })
          .then((result) => {
            jobData.status = 'done';
            jobData.progress = 100;
            jobData.title = `Completed: @${igProfileUser} (${result.downloadedCount} videos)`;
            jobData.log = 'Finished downloading all profile videos.';
            broadcast({ type: 'job', ...jobData });
          })
          .catch((e) => {
            jobData.status = 'error';
            jobData.error = e.message;
            jobData.log = 'Failed: ' + e.message;
            broadcast({ type: 'job', ...jobData });
          });
      };

      const redisOk = await isRedisAlive();
      if (redisOk) {
        try {
          await Promise.race([
            downloadQueue.add('instagram_profile', {
              type: 'instagram_profile',
              username: igProfileUser,
              cookieSource: 'cookies.txt',
              profileUrl: url
            }, { jobId: id }),
            new Promise((_, reject) => setTimeout(() => reject(new Error('BullMQ timeout')), 1500))
          ]);
        } catch (err) {
          console.warn('⚠️ BullMQ add error for IG profile, running direct:', err.message);
          runDirectIg();
        }
      } else {
        runDirectIg();
      }
      continue;
    }

    const id = `job_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const jobData = {
      id,
      type: 'standard',
      url,
      status: 'downloading',
      progress: 0,
      title: url,
      log: 'Starting download...',
      error: '',
      badge: 'Video Download'
    };

    const runDirectYtdlp = () => {
      jobData.status = 'downloading';
      jobData.log = 'Starting download directly...';
      broadcast({ type: 'job', ...jobData });
      runYtdlpDownload({ id, data: { url } })
        .then((result) => {
          jobData.status = 'done';
          jobData.progress = 100;
          jobData.title = result.title || jobData.title;
          jobData.log = 'Download complete. File saved to Downloads/yt-dlp/';
          broadcast({ type: 'job', ...jobData });
        })
        .catch((e) => {
          jobData.status = 'error';
          jobData.error = e.message;
          jobData.log = 'Failed: ' + e.message;
          broadcast({ type: 'job', ...jobData });
        });
    };

    const redisOk = await isRedisAlive();
    if (redisOk) {
      jobData.status = 'queued';
      jobData.log = 'Queued in BullMQ worker...';
      jobs.set(id, jobData);
      started.push(id);
      createdJobs.push(jobData);
      broadcast({ type: 'job', ...jobData });

      try {
        await Promise.race([
          downloadQueue.add('standard', { type: 'standard', url }, { jobId: id }),
          new Promise((_, reject) => setTimeout(() => reject(new Error('BullMQ timeout')), 1500))
        ]);
      } catch (err) {
        console.warn('⚠️ BullMQ add error, running direct download:', err.message);
        runDirectYtdlp();
      }
    } else {
      jobs.set(id, jobData);
      started.push(id);
      createdJobs.push(jobData);
      broadcast({ type: 'job', ...jobData });
      runDirectYtdlp();
    }
  }

  res.json({ started, jobs: createdJobs });
});

// ── Instagram Profile Download Endpoint ──────────────────────────────────────
app.post('/download/instagram-profile', async (req, res) => {
  const { profileUrl, cookieSource, mediaScope } = req.body;
  if (!profileUrl) {
    return res.status(400).json({ error: 'Instagram profile URL or username is required.' });
  }

  const username = extractInstagramUsername(profileUrl);
  if (!username) {
    return res.status(400).json({
      error: 'Invalid Instagram profile URL or handle. Example: https://www.instagram.com/cristiano/ or @natgeo'
    });
  }

  const scope = mediaScope || 'all';
  const scopeLabel = scope === 'video' ? 'Videos & Reels' : scope === 'image' ? 'Photos' : 'All Media';

  const id = `ig_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
  const jobData = {
    id,
    type: 'instagram_profile',
    username,
    url: `https://www.instagram.com/${username}/`,
    status: 'queued',
    progress: 0,
    title: `Instagram Profile: @${username} (${scopeLabel})`,
    log: 'Queued in BullMQ worker...',
    error: '',
    badge: 'Instagram Profile Download'
  };
  jobs.set(id, jobData);
  broadcast({ type: 'job', ...jobData });

  const runDirectProfile = () => {
    jobData.status = 'downloading';
    jobData.log = 'Starting scraper directly...';
    broadcast({ type: 'job', ...jobData });
    downloadInstagramProfile({
      jobId: id,
      username,
      mediaScope: scope,
      cookieSource: cookieSource || 'cookies.txt',
      downloadDir: DOWNLOAD_DIR,
      cookiesFile: COOKIES,
      galleryDlExe: GALLERYDL_EXE,
      activeProcesses,
      onProgress: ({ downloadedCount, currentFile, log }) => {
        jobData.progress = Math.min(downloadedCount * 3, 95);
        jobData.title = currentFile ? `[#${downloadedCount}] ${currentFile}` : `Downloaded ${downloadedCount} item(s)`;
        jobData.log = log || `Downloaded ${downloadedCount} item(s)`;
        broadcast({
          type: 'progress',
          id,
          status: 'downloading',
          progress: jobData.progress,
          title: jobData.title,
          log: jobData.log
        });
      }
    })
      .then((result) => {
        jobData.status = 'done';
        jobData.progress = 100;
        jobData.title = `Completed: @${username} (${result.downloadedCount} item(s) saved)`;
        jobData.log = 'Finished downloading all profile media.';
        broadcast({ type: 'job', ...jobData });
      })
      .catch((e) => {
        jobData.status = 'error';
        jobData.error = e.message;
        jobData.log = 'Failed: ' + e.message;
        broadcast({ type: 'job', ...jobData });
      });
  };

  const redisOk = await isRedisAlive();
  if (redisOk) {
    try {
      await Promise.race([
        downloadQueue.add('instagram_profile', {
          type: 'instagram_profile',
          username,
          cookieSource: cookieSource || 'cookies.txt',
          mediaScope: scope,
          profileUrl
        }, { jobId: id }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('BullMQ timeout')), 1500))
      ]);
    } catch (err) {
      console.warn('⚠️ BullMQ add error for profile, running direct:', err.message);
      runDirectProfile();
    }
  } else {
    runDirectProfile();
  }

  res.json({ started: [id], username, job: jobData });
});

// ── Get all jobs (Sync endpoint) ─────────────────────────────────────────────
app.get('/jobs', (_req, res) => {
  const list = [...jobs.entries()].map(([id, j]) => serializeJob(id, j));
  res.json({ jobs: list });
});

function stopJobExecution(id) {
  if (activeScrapeAborts.has(id)) {
    try { activeScrapeAborts.get(id).abort(); } catch (_) {}
    activeScrapeAborts.delete(id);
  }
  if (activeProcesses.has(id)) {
    const proc = activeProcesses.get(id);
    try {
      proc.kill('SIGTERM');
      if (proc.pid) {
        spawn('taskkill', ['/pid', proc.pid.toString(), '/f', '/t'], { windowsHide: true });
      }
    } catch (_) {}
    activeProcesses.delete(id);
  }
}

async function removeFromBullQueues(id) {
  if (!isRedisReady()) return;
  try {
    await Promise.race([
      (async () => {
        for (const q of [downloadQueue, scrapeQueue]) {
          const bullJob = await q.getJob(id);
          if (bullJob) await bullJob.remove();
        }
      })(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1000))
    ]);
  } catch (_) {}
}

// ── Cancel a job ─────────────────────────────────────────────────────────────
app.post('/cancel/:id', async (req, res) => {
  const id = req.params.id;
  const job = jobs.get(id);

  if (job) {
    job.status = 'cancelled';
    job.log = 'Download cancelled by user.';
  }
  stopJobExecution(id);
  await removeFromBullQueues(id);

  if (job) {
    broadcast({ type: 'job', ...job });
    scheduleSaveQueueState();
  }

  res.json({ ok: true });
});

// ── Delete single job from list ──────────────────────────────────────────────
app.delete('/job/:id', async (req, res) => {
  const id = req.params.id;
  const job = jobs.get(id);
  if (job) job.status = 'cancelled';

  stopJobExecution(id);
  await removeFromBullQueues(id);

  jobs.delete(id);
  saveQueueStateNow();
  broadcast({ type: 'job_removed', id });
  res.json({ ok: true });
});

// ── Clear finished/errored jobs ───────────────────────────────────────────────
app.post('/clear', async (_req, res) => {
  for (const [id, job] of jobs) {
    if (['done', 'error', 'cancelled'].includes(job.status)) {
      jobs.delete(id);
    }
  }

  if (isRedisReady()) {
    try {
      await Promise.race([
        Promise.all([
          downloadQueue.clean(0, 100, 'completed'),
          downloadQueue.clean(0, 100, 'failed'),
          scrapeQueue.clean(0, 100, 'completed'),
          scrapeQueue.clean(0, 100, 'failed')
        ]),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1000))
      ]);
    } catch (_) {}
  }

  saveQueueStateNow();
  broadcast({ type: 'cleared' });
  res.json({ ok: true });
});

// ── Clear ENTIRE queue (stuck, queued, downloading, finished) ─────────────────
app.post('/clear-all', async (_req, res) => {
  // 1. Mark all queued/active jobs cancelled, reset sequential chain, & abort active sniffs / processes
  scrapeGeneration++;
  directScrapeChain = Promise.resolve();
  for (const [id, job] of jobs.entries()) {
    job.status = 'cancelled';
    stopJobExecution(id);
  }
  activeScrapeAborts.clear();
  activeProcesses.clear();

  // 2. Safely purge BullMQ queues if Redis is connected (with 1.5s timeout)
  if (isRedisReady()) {
    try {
      await Promise.race([
        (async () => {
          for (const q of [downloadQueue, scrapeQueue]) {
            try {
              await q.pause();
              await q.drain(true);
              await q.clean(0, 0, 'active');
              await q.clean(0, 0, 'wait');
              await q.clean(0, 0, 'delayed');
              await q.clean(0, 0, 'completed');
              await q.clean(0, 0, 'failed');
              await q.resume();
            } catch (_) {}
          }
        })(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1500))
      ]);
    } catch (e) {
      console.warn('  ⚠️ BullMQ purge notice:', e.message);
    }
  }

  // 3. Clear in-memory jobs completely & persist empty state
  const totalCleared = jobs.size;
  jobs.clear();
  saveQueueStateNow();

  // 4. Broadcast full wipe to all clients
  broadcast({ type: 'cleared_all' });
  res.json({ ok: true, cleared: totalCleared });
});

// ── Open download folder ──────────────────────────────────────────────────────
app.get('/open-folder', (_req, res) => {
  const { exec } = require('child_process');
  exec(`explorer "${DOWNLOAD_DIR}"`);
  res.json({ ok: true, path: DOWNLOAD_DIR });
});

// ── Info endpoint ─────────────────────────────────────────────────────────────
app.get('/info', (_req, res) => {
  let hasIgSession = false;
  if (fs.existsSync(COOKIES)) {
    try {
      const raw = fs.readFileSync(COOKIES, 'utf8');
      hasIgSession = raw.includes('sessionid') && raw.includes('.instagram.com');
    } catch (_) {}
  }

  res.json({
    downloadDir: DOWNLOAD_DIR,
    ytdlpExists: fs.existsSync(YTDLP_EXE),
    galleryDlExists: fs.existsSync(GALLERYDL_EXE),
    instaloaderExists: isInstaloaderAvailable(),
    cookiesExist: fs.existsSync(COOKIES),
    hasIgSession,
    redisConnected: isRedisReady(),
    bridgeConnected: isBridgeConnected(),
    extensionDir: path.join(__dirname, 'extension')
  });
});

// ── Full Webpage Scraper & Extension Bridge Routes ────────────────────────────
app.get('/scraper/bridge/poll', handleBridgePoll);

app.post('/scraper/bridge/result', (req, res) => {
  const matched = handleBridgeResult(req.body);
  res.json({ ok: true, matched });
});

app.get('/scraper/bridge/status', (req, res) => {
  const targetUrl = typeof req.query.url === 'string' ? req.query.url : '';
  res.json({
    ok: true,
    ...getBridgeState(),
    cookies: getCookiesSummary(COOKIES, targetUrl),
    extensionDir: path.join(__dirname, 'extension')
  });
});

app.post('/scraper/scan', async (req, res) => {
  const { url, mode = 'auto' } = req.body || {};
  if (!url || typeof url !== 'string' || !/^https?:\/\//i.test(url.trim())) {
    return res.status(400).json({ error: 'Please enter a valid http:// or https:// webpage URL.' });
  }
  try {
    const result = await scanWebpage({
      url: url.trim(),
      mode,
      cookiesFilePath: COOKIES
    });
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message || 'Failed to scan webpage.' });
  }
});

app.post('/scraper/push-scan', (req, res) => {
  const { pageTitle, pageUrl, items } = req.body || {};
  if (!Array.isArray(items)) {
    return res.status(400).json({ error: 'Invalid scan payload.' });
  }
  broadcast({
    type: 'scraper_scan_push',
    pageTitle: pageTitle || pageUrl || 'Scanned Webpage',
    pageUrl: pageUrl || '',
    items
  });
  res.json({ ok: true, count: items.length });
});

async function scheduleScrapeExecution(jobData, redisOk) {
  const { id, url: pageUrl, title, mode = 'auto', useCookies = true } = jobData;
  const myGen = scrapeGeneration;

  const runDirectSequential = () => {
    directScrapeChain = directScrapeChain
      .catch(() => {})
      .then(async () => {
        if (myGen !== scrapeGeneration) return;
        const current = jobs.get(id);
        if (!current || current.status === 'cancelled') return;
        try {
          const result = await runScrapeJob({
            id,
            data: { pageUrl, title, mode, useCookies }
          });
          current.status = 'done';
          current.stage = 'done';
          current.progress = 100;
          if (result && result.title) current.title = result.title;
          if (result && result.streamUrl) current.streamUrl = result.streamUrl;
          current.log = 'Download complete. File saved to Downloads/yt-dlp/';
          broadcast({ type: 'job', ...current });
          scheduleSaveQueueState();
        } catch (err) {
          if (current.status !== 'cancelled') {
            current.status = 'error';
            current.stage = 'error';
            current.error = err.message || 'Scrape download failed';
            current.log = 'Failed: ' + (err.message || 'Error occurred');
            broadcast({ type: 'job', ...current });
            scheduleSaveQueueState();
          }
        }
      });
  };

  if (redisOk) {
    try {
      const existingBull = await scrapeQueue.getJob(id).catch(() => null);
      if (!existingBull) {
        await Promise.race([
          scrapeQueue.add('web_scrape', {
            type: 'web_scrape',
            pageUrl,
            title,
            mode,
            useCookies
          }, { jobId: id }),
          new Promise((_, reject) => setTimeout(() => reject(new Error('BullMQ timeout')), 1500))
        ]);
      }
      return;
    } catch (err) {
      console.warn('⚠️ BullMQ add error for scraper, running sequential fallback:', err.message);
    }
  }
  runDirectSequential();
}

async function enqueueScrapeJob({ pageUrl, title, mode, useCookies, queueIndex, queueTotal, redisOk }) {
  const id = `scr_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
  const jobData = {
    id,
    type: 'web_scrape',
    url: pageUrl,
    status: 'queued',
    stage: 'queued',
    progress: 0,
    title,
    log: `Queued (#${queueIndex} of ${queueTotal}) — waiting for 1-by-1 slot...`,
    error: '',
    badge: 'Webpage Scraper (1-by-1)',
    streamUrl: '',
    queueIndex,
    queueTotal,
    mode,
    useCookies
  };

  jobs.set(id, jobData);
  broadcast({ type: 'job', ...jobData });
  scheduleSaveQueueState();
  await scheduleScrapeExecution(jobData, redisOk);

  return jobData;
}

async function restoreQueueState() {
  if (!fs.existsSync(QUEUE_STATE_FILE)) return;
  try {
    const raw = fs.readFileSync(QUEUE_STATE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.jobs) || parsed.jobs.length === 0) return;

    const redisOk = await isRedisAlive();
    let restoredActive = 0;

    for (const saved of parsed.jobs) {
      if (!saved || !saved.id || !saved.url) continue;
      const wasActiveOrQueued = saved.status === 'queued' || saved.status === 'downloading';

      const jobData = {
        id: saved.id,
        type: saved.type || 'standard',
        url: saved.url,
        status: wasActiveOrQueued ? (saved.type === 'web_scrape' ? 'queued' : 'error') : saved.status,
        stage: wasActiveOrQueued ? (saved.type === 'web_scrape' ? 'queued' : 'error') : (saved.stage || saved.status),
        progress: wasActiveOrQueued ? 0 : (saved.progress || 0),
        title: saved.title || saved.url,
        error: wasActiveOrQueued && saved.type !== 'web_scrape' ? 'Server restarted while job was in progress.' : (saved.error || ''),
        log: wasActiveOrQueued
          ? (saved.type === 'web_scrape' ? 'Restored after restart — queued for 1-by-1 resume...' : 'Interrupted by server restart.')
          : (saved.log || ''),
        badge: saved.badge || 'Video Download',
        streamUrl: saved.streamUrl || '',
        queueIndex: saved.queueIndex || null,
        queueTotal: saved.queueTotal || null,
        mode: saved.mode || 'auto',
        useCookies: saved.useCookies !== false
      };

      jobs.set(jobData.id, jobData);

      if (wasActiveOrQueued && jobData.type === 'web_scrape') {
        restoredActive++;
        await scheduleScrapeExecution(jobData, redisOk);
      }
    }

    if (jobs.size > 0) {
      console.log(`  ♻️  Restored ${jobs.size} job(s) from queue-state.json (${restoredActive} pending scrape job(s) resumed)`);
      scheduleSaveQueueState();
    }
  } catch (e) {
    console.warn('  ⚠️ Could not restore queue state:', e.message);
  }
}

app.post('/scraper/download', async (req, res) => {
  const { items, mode = 'auto', useCookies = true } = req.body || {};
  const validItems = Array.isArray(items)
    ? items.filter(it => it && typeof it.url === 'string' && it.url.trim())
    : [];
  if (validItems.length === 0) {
    return res.status(400).json({ error: 'Select at least one video to queue.' });
  }

  const started = [];
  const createdJobs = [];
  const redisOk = await isRedisAlive();
  const queueTotal = validItems.length;

  for (let i = 0; i < validItems.length; i++) {
    const item = validItems[i];
    const pageUrl = item.url.trim();
    const title = (item.title && String(item.title).trim()) || pageUrl;
    const jobData = await enqueueScrapeJob({
      pageUrl,
      title,
      mode,
      useCookies,
      queueIndex: i + 1,
      queueTotal,
      redisOk
    });
    started.push(jobData.id);
    createdJobs.push(jobData);
  }

  res.json({ ok: true, started, jobs: createdJobs });
});

app.post('/scraper/retry/:id', async (req, res) => {
  const old = jobs.get(req.params.id);
  if (!old || old.type !== 'web_scrape') {
    return res.status(404).json({ error: 'Scrape job not found.' });
  }
  const redisOk = await isRedisAlive();
  const jobData = await enqueueScrapeJob({
    pageUrl: old.url,
    title: old.title || old.url,
    mode: old.mode || 'auto',
    useCookies: old.useCookies !== false,
    queueIndex: old.queueIndex || 1,
    queueTotal: old.queueTotal || 1,
    redisOk
  });
  res.json({ ok: true, job: jobData });
});

app.post('/scraper/open-browser', async (req, res) => {
  const { url } = req.body || {};
  const targetUrl = (url && /^https?:\/\//i.test(url.trim())) ? url.trim() : 'about:blank';
  try {
    await ensureCdpBrowser({ visible: true, initialUrl: targetUrl });
    res.json({ ok: true });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/scraper/cookies-status', (req, res) => {
  const targetUrl = typeof req.query.url === 'string' ? req.query.url : '';
  res.json({
    ok: true,
    ...getCookiesSummary(COOKIES, targetUrl),
    cookiesFilePath: COOKIES
  });
});

app.post('/scraper/save-cookies', (req, res) => {
  const { cookieData, targetUrl } = req.body || {};
  try {
    const result = saveGenericCookies(cookieData, targetUrl, COOKIES);
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/scraper/clear-cookies', (req, res) => {
  const { targetUrl } = req.body || {};
  const result = clearDomainCookies(COOKIES, targetUrl);
  res.json({ ok: true, ...result });
});

// ── Instagram Authentication Routes ──────────────────────────────────────────
app.get('/auth/instagram-status', (_req, res) => {
  const status = getInstagramSessionStatus(COOKIES);
  res.json({ ok: true, ...status, cookiesFilePath: COOKIES });
});

app.post('/auth/open-instagram-browser', (_req, res) => {
  openInstagramInBrowser();
  res.json({ ok: true });
});

app.post('/auth/clear-session', (_req, res) => {
  clearInstagramSession(COOKIES);
  broadcast({ type: 'instagram_auth_cleared' });
  res.json({ ok: true });
});

app.post('/auth/instagram-login', (_req, res) => {
  try {
    const result = startInstagramLoginPopup({
      cookiesFilePath: COOKIES,
      onBroadcast: (data) => broadcast(data),
      onCompleted: (data) => {
        broadcast({ type: 'instagram_auth_success', ...data });
      },
      onError: (err) => {
        broadcast({ type: 'instagram_auth_error', error: err.message });
      }
    });
    res.json({ ok: true, browser: result.browser });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/auth/cancel-login', (_req, res) => {
  cancelInstagramLoginPopup();
  res.json({ ok: true });
});

app.post('/auth/save-session', (req, res) => {
  const { sessionId, userId } = req.body;
  if (!sessionId) {
    return res.status(400).json({ error: 'Session ID or cookie content is required.' });
  }
  try {
    const result = saveManualSession(sessionId, userId, COOKIES);
    broadcast({ type: 'instagram_auth_success', userId: result.userId });
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`\n  ======================================================`);
  console.log(`  🚀  yt-dlp + BullMQ Web UI running at http://localhost:${PORT}`);
  console.log(`  📁  Downloads → ${DOWNLOAD_DIR}`);
  console.log(`  📸  Instagram Profile Downloads → ${IG_DOWNLOAD_DIR}`);
  console.log(`  ======================================================\n`);
  restoreQueueState();
});
