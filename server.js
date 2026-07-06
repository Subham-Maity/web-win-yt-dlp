const express = require('express');
const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

const app = express();
const PORT = 3456;

// ── Paths ──────────────────────────────────────────────────────────────────
const YTDLP_EXE  = path.join(__dirname, 'yt-dlp.exe');
const COOKIES    = path.join(__dirname, 'cookies.txt');
const DOWNLOAD_DIR = path.join(os.homedir(), 'Downloads', 'yt-dlp');

// Ensure download folder exists
if (!fs.existsSync(DOWNLOAD_DIR)) {
  fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
}

// ── State ──────────────────────────────────────────────────────────────────
// Map of jobId -> { url, status, progress, title, process }
const jobs = new Map();
// SSE clients
const sseClients = new Set();

function broadcast(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(msg); } catch (_) {}
  }
}

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── SSE endpoint ────────────────────────────────────────────────────────────
app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Send current state immediately
  const snapshot = [...jobs.entries()].map(([id, j]) => ({
    id, url: j.url, status: j.status, progress: j.progress, title: j.title, error: j.error
  }));
  res.write(`data: ${JSON.stringify({ type: 'snapshot', jobs: snapshot })}\n\n`);

  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// ── Start a download ─────────────────────────────────────────────────────────
app.post('/download', (req, res) => {
  const { urls } = req.body; // array of URL strings
  if (!Array.isArray(urls) || urls.length === 0) {
    return res.status(400).json({ error: 'No URLs provided' });
  }

  const started = [];

  for (const rawUrl of urls) {
    const url = rawUrl.trim();
    if (!url) continue;

    const id = `job_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const job = { url, status: 'queued', progress: 0, title: '', error: '' };
    jobs.set(id, job);
    started.push(id);

    broadcast({ type: 'job', id, ...job });

    // Build yt-dlp arguments
    // --js-runtimes is required for YouTube (PO token / bot detection bypass)
    const args = [
      '--no-playlist',
      '--js-runtimes', `node:${process.execPath}`,
      '-o', path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].%(ext)s'),
      '--newline',          // one line per progress update
      '--progress-template', '%(progress._percent_str)s|%(info.title)s',
    ];

    let tempCookiesPath = null;
    if (fs.existsSync(COOKIES)) {
      try {
        // Copy to a temp file to prevent concurrent access / file lock issues on Windows
        tempCookiesPath = path.join(os.tmpdir(), `ytdlp_cookies_${id}.txt`);
        fs.copyFileSync(COOKIES, tempCookiesPath);
        args.push('--cookies', tempCookiesPath);
      } catch (e) {
        console.error('Failed to copy cookies file:', e);
        // fallback to original if copy fails
        args.push('--cookies', COOKIES);
      }
    }

    args.push(url);

    const proc = spawn(YTDLP_EXE, args, { windowsHide: true });
    job.process = proc;
    job.status = 'downloading';
    broadcast({ type: 'job', id, url: job.url, status: job.status, progress: 0, title: '' });

    proc.stdout.on('data', (chunk) => {
      const lines = chunk.toString().split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        // Parse our custom template: "XX.X%|Title"
        if (trimmed.includes('|')) {
          const [pct, ...rest] = trimmed.split('|');
          const num = parseFloat(pct.replace('%', ''));
          if (!isNaN(num)) {
            job.progress = num;
            job.title = rest.join('|').trim() || job.title;
            broadcast({ type: 'progress', id, progress: job.progress, title: job.title });
          }
        } else if (trimmed.startsWith('[download]')) {
          // Fallback: parse classic yt-dlp output
          const m = trimmed.match(/(\d+\.?\d*)%/);
          if (m) {
            job.progress = parseFloat(m[1]);
            broadcast({ type: 'progress', id, progress: job.progress, title: job.title });
          }
          // Extract title from "Destination:" line
          const dest = trimmed.match(/Destination:\s+.+[/\\](.+)/);
          if (dest && !job.title) {
            job.title = dest[1];
            broadcast({ type: 'progress', id, progress: job.progress, title: job.title });
          }
        } else if (trimmed.startsWith('[info]') || trimmed.startsWith('[youtube]')) {
          // Try to grab title from info lines
          const titleMatch = trimmed.match(/: (.+)$/);
          if (titleMatch && !job.title) {
            job.title = titleMatch[1].trim();
            broadcast({ type: 'progress', id, progress: job.progress, title: job.title });
          }
        }
      }
    });

    let stderrBuf = '';
    proc.stderr.on('data', (chunk) => {
      stderrBuf += chunk.toString();
    });

    const cleanupTempCookie = () => {
      if (tempCookiesPath && fs.existsSync(tempCookiesPath)) {
        try { fs.unlinkSync(tempCookiesPath); } catch (_) {}
      }
    };

    proc.on('close', (code) => {
      cleanupTempCookie();
      if (code === 0) {
        job.status = 'done';
        job.progress = 100;
      } else {
        job.status = 'error';
        // Extract the last meaningful error line from stderr
        const errLines = stderrBuf.split('\n').map(l => l.trim()).filter(Boolean);
        const lastErr = errLines.reverse().find(l => l.includes('ERROR') || l.includes('error') || l.includes('Unable') || l.includes('Sign in')) || errLines[0] || `exit code ${code}`;
        job.error = lastErr.replace(/^ERROR:\s*/i, '');
      }
      broadcast({ type: 'job', id, url: job.url, status: job.status, progress: job.progress, title: job.title, error: job.error });
    });

    proc.on('error', (err) => {
      cleanupTempCookie();
      job.status = 'error';
      job.error = err.message;
      broadcast({ type: 'job', id, url: job.url, status: job.status, progress: job.progress, title: job.title, error: job.error });
    });
  }

  res.json({ started });
});

// ── Cancel a job ─────────────────────────────────────────────────────────────
app.post('/cancel/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  if (job.process) {
    try { job.process.kill('SIGTERM'); } catch (_) {}
  }
  job.status = 'cancelled';
  broadcast({ type: 'job', id: req.params.id, url: job.url, status: 'cancelled', progress: job.progress, title: job.title });
  res.json({ ok: true });
});

// ── Clear finished/errored jobs ───────────────────────────────────────────────
app.post('/clear', (_req, res) => {
  for (const [id, job] of jobs) {
    if (['done', 'error', 'cancelled'].includes(job.status)) {
      jobs.delete(id);
    }
  }
  broadcast({ type: 'cleared' });
  res.json({ ok: true });
});

// ── Open download folder ──────────────────────────────────────────────────────
app.get('/open-folder', (_req, res) => {
  const { exec } = require('child_process');
  exec(`explorer "${DOWNLOAD_DIR}"`);
  res.json({ ok: true, path: DOWNLOAD_DIR });
});

// ── Info endpoint ─────────────────────────────────────────────────────────────
app.get('/info', (_req, res) => {
  res.json({ downloadDir: DOWNLOAD_DIR, ytdlpExists: fs.existsSync(YTDLP_EXE), cookiesExist: fs.existsSync(COOKIES) });
});

app.listen(PORT, () => {
  console.log(`\n  ✅  yt-dlp Web UI running at  http://localhost:${PORT}`);
  console.log(`  📁  Downloads → ${DOWNLOAD_DIR}\n`);
});
