const express = require('express');
const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');
const { Worker } = require('bullmq');

const { redisConnection, downloadQueue, activeProcesses, isRedisReady } = require('./queue');
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

const app = express();
const PORT = 3456;

// ── Process-Level Socket / Error Guards ──────────────────────────────────────
process.on('uncaughtException', (err) => {
  if (err && (err.code === 'ECONNRESET' || err.code === 'ECONNABORTED' || err.code === 'ECONNREFUSED')) {
    return;
  }
  console.error('  ⚠️ Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason) => {
  if (reason && (reason.code === 'ECONNRESET' || reason.code === 'ECONNABORTED' || reason.code === 'ECONNREFUSED')) {
    return;
  }
});

// ── Paths ──────────────────────────────────────────────────────────────────
const YTDLP_EXE     = path.join(__dirname, 'yt-dlp.exe');
const GALLERYDL_EXE = path.join(__dirname, 'gallery-dl.exe');
const COOKIES       = path.join(__dirname, 'cookies.txt');
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

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── yt-dlp Worker Handler ────────────────────────────────────────────────────
function runYtdlpDownload(job) {
  return new Promise((resolve, reject) => {
    const { url } = job.data;
    const id = job.id;

    const j = jobs.get(id);
    if (j) {
      j.status = 'downloading';
      j.log = 'Connecting to stream source...';
      broadcast({ type: 'job', ...j });
    }

    const args = [
      '--no-playlist',
      '--js-runtimes', `node:${process.execPath}`,
      '-o', path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].%(ext)s'),
      '--newline',
      '--progress-template', '%(progress._percent_str)s|%(info.title)s|%(progress._speed_str)s|%(progress._eta_str)s',
    ];

    let tempCookiesPath = null;
    if (fs.existsSync(COOKIES)) {
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
      const lines = chunk.toString().split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        // Custom template: "XX.X%|Title|Speed|ETA"
        if (trimmed.includes('|')) {
          const [pct, extractedTitle, speed, eta] = trimmed.split('|');
          const num = parseFloat(pct.replace('%', ''));
          if (!isNaN(num)) {
            progress = num;
            if (extractedTitle) title = extractedTitle;
            let logMsg = `Speed: ${speed || 'N/A'} • ETA: ${eta || 'N/A'}`;
            job.updateProgress({ progress, title, log: logMsg });
            const item = jobs.get(id);
            if (item) {
              item.progress = progress;
              item.title = title;
              item.log = logMsg;
              item.status = 'downloading';
            }
            broadcast({ type: 'progress', id, status: 'downloading', progress, title, log: logMsg });
          }
        } else if (trimmed.startsWith('[download]')) {
          const m = trimmed.match(/(\d+\.?\d*)%/);
          if (m) {
            progress = parseFloat(m[1]);
          }
          const item = jobs.get(id);
          if (item) {
            item.progress = progress;
            item.log = trimmed;
            item.status = 'downloading';
          }
          broadcast({ type: 'progress', id, status: 'downloading', progress, title: item ? item.title : title, log: trimmed });
        } else {
          // General informational line (e.g. "[info] Extracting URL: ...")
          const item = jobs.get(id);
          if (item) {
            item.log = trimmed;
            item.status = 'downloading';
          }
          broadcast({ type: 'progress', id, status: 'downloading', progress, title: item ? item.title : title, log: trimmed });
        }
      }
    });

    let stderrBuf = '';
    proc.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderrBuf += text;
      const lastLine = text.split('\n').map(l => l.trim()).filter(Boolean).pop();
      if (lastLine) {
        const item = jobs.get(id);
        if (item) { item.log = lastLine; }
        broadcast({ type: 'progress', id, status: 'downloading', progress, title, log: lastLine });
      }
    });

    const cleanup = () => {
      activeProcesses.delete(id);
      if (tempCookiesPath && fs.existsSync(tempCookiesPath)) {
        try { fs.unlinkSync(tempCookiesPath); } catch (_) {}
      }
    };

    proc.on('close', (code) => {
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
  }
});

downloadWorker.on('failed', (job, err) => {
  const j = jobs.get(job.id);
  if (j) {
    j.status = 'error';
    j.error = err.message || 'Download failed';
    j.log = 'Failed: ' + (err.message || 'Error occurred');
    broadcast({ type: 'job', ...j });
  }
});

downloadWorker.on('error', () => {
  // Handled silently when Redis is unavailable
});

// ── SSE Endpoint ─────────────────────────────────────────────────────────────
app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Send current snapshot
  const snapshot = [...jobs.entries()].map(([id, j]) => ({
    id,
    type: j.type,
    url: j.url,
    status: j.status,
    progress: j.progress,
    title: j.title,
    error: j.error,
    log: j.log || '',
    badge: j.badge
  }));
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

      if (isRedisReady()) {
        try {
          await downloadQueue.add('instagram_profile', {
            type: 'instagram_profile',
            username: igProfileUser,
            cookieSource: 'cookies.txt',
            profileUrl: url
          }, { jobId: id });
        } catch (err) {
          console.error('BullMQ add error for IG profile:', err.message);
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
      status: 'queued',
      progress: 0,
      title: url,
      log: isRedisReady() ? 'Queued in BullMQ worker...' : 'Direct download mode...',
      error: '',
      badge: 'Video Download'
    };
    jobs.set(id, jobData);
    started.push(id);
    createdJobs.push(jobData);
    broadcast({ type: 'job', ...jobData });

    const runDirectYtdlp = () => {
      jobData.status = 'downloading';
      jobData.log = 'Running direct download...';
      broadcast({ type: 'job', ...jobData });
      runYtdlpDownload({ id, data: { url } })
        .then((result) => {
          jobData.status = 'done';
          jobData.progress = 100;
          jobData.title = result.title || jobData.title;
          jobData.log = 'Download complete.';
          broadcast({ type: 'job', ...jobData });
        })
        .catch((e) => {
          jobData.status = 'error';
          jobData.error = e.message;
          jobData.log = 'Failed: ' + e.message;
          broadcast({ type: 'job', ...jobData });
        });
    };

    if (isRedisReady()) {
      try {
        await downloadQueue.add('standard', { type: 'standard', url }, { jobId: id });
      } catch (err) {
        console.error('Failed to enqueue to BullMQ:', err.message);
        runDirectYtdlp();
      }
    } else {
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

  if (isRedisReady()) {
    try {
      await downloadQueue.add('instagram_profile', {
        type: 'instagram_profile',
        username,
        cookieSource: cookieSource || 'cookies.txt',
        mediaScope: scope,
        profileUrl
      }, { jobId: id });
    } catch (err) {
      console.error('BullMQ add error:', err.message);
      runDirectProfile();
    }
  } else {
    runDirectProfile();
  }

  res.json({ started: [id], username, job: jobData });
});

// ── Get all jobs (Sync endpoint) ─────────────────────────────────────────────
app.get('/jobs', (_req, res) => {
  const list = [...jobs.entries()].map(([id, j]) => ({
    id,
    type: j.type,
    url: j.url,
    status: j.status,
    progress: j.progress,
    title: j.title,
    error: j.error,
    log: j.log || '',
    badge: j.badge
  }));
  res.json({ jobs: list });
});

// ── Cancel a job ─────────────────────────────────────────────────────────────
app.post('/cancel/:id', async (req, res) => {
  const id = req.params.id;
  const job = jobs.get(id);

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

  if (isRedisReady()) {
    try {
      await Promise.race([
        (async () => {
          const bullJob = await downloadQueue.getJob(id);
          if (bullJob) await bullJob.remove();
        })(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1000))
      ]);
    } catch (_) {}
  }

  if (job) {
    job.status = 'cancelled';
    job.log = 'Download cancelled by user.';
    broadcast({ type: 'job', ...job });
  }

  res.json({ ok: true });
});

// ── Delete single job from list ──────────────────────────────────────────────
app.delete('/job/:id', async (req, res) => {
  const id = req.params.id;
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

  if (isRedisReady()) {
    try {
      await Promise.race([
        (async () => {
          const bullJob = await downloadQueue.getJob(id);
          if (bullJob) await bullJob.remove();
        })(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1000))
      ]);
    } catch (_) {}
  }

  jobs.delete(id);
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
          downloadQueue.clean(0, 100, 'failed')
        ]),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1000))
      ]);
    } catch (_) {}
  }

  broadcast({ type: 'cleared' });
  res.json({ ok: true });
});

// ── Clear ENTIRE queue (stuck, queued, downloading, finished) ─────────────────
app.post('/clear-all', async (_req, res) => {
  // 1. Terminate all running download child processes
  for (const [id, proc] of activeProcesses.entries()) {
    try {
      proc.kill('SIGTERM');
      if (proc.pid) {
        spawn('taskkill', ['/pid', proc.pid.toString(), '/f', '/t'], { windowsHide: true });
      }
    } catch (_) {}
  }
  activeProcesses.clear();

  // 2. Safely purge BullMQ queue if Redis is connected (with 1.5s timeout)
  if (isRedisReady()) {
    try {
      await Promise.race([
        (async () => {
          await downloadQueue.pause();
          await downloadQueue.drain(true);
          await downloadQueue.clean(0, 0, 'active');
          await downloadQueue.clean(0, 0, 'wait');
          await downloadQueue.clean(0, 0, 'delayed');
          await downloadQueue.clean(0, 0, 'completed');
          await downloadQueue.clean(0, 0, 'failed');
          await downloadQueue.resume();
        })(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1500))
      ]);
    } catch (e) {
      console.warn('  ⚠️ BullMQ purge notice:', e.message);
    }
  }

  // 3. Clear in-memory jobs completely
  const totalCleared = jobs.size;
  jobs.clear();

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
  });
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
});
