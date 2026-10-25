const { spawn, execSync } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

/**
 * Extracts a clean Instagram username from various input formats:
 * - https://www.instagram.com/cristiano/
 * - https://instagram.com/cristiano/reels/
 * - https://instagram.com/cristiano/posts
 * - @cristiano
 * - cristiano
 */
function extractInstagramUsername(input) {
  if (!input || typeof input !== 'string') return null;
  let str = input.trim();
  str = str.replace(/\/+$/, ''); // trim trailing slashes

  const urlMatch = str.match(/(?:https?:\/\/)?(?:www\.)?instagram\.com\/([a-zA-Z0-9._]+)(?:\/(?:reels|posts|tagged|feed)?)?/i);
  if (urlMatch && urlMatch[1]) {
    const reserved = ['p', 'reel', 'stories', 'explore', 'direct', 'accounts', 'developer', 'about', 'legal'];
    if (!reserved.includes(urlMatch[1].toLowerCase())) {
      return urlMatch[1];
    }
  }

  if (str.startsWith('@')) {
    return str.slice(1).trim();
  }

  if (/^[a-zA-Z0-9._]+$/.test(str)) {
    return str;
  }

  return null;
}

/**
 * Checks whether Python + Instaloader is available
 */
function isInstaloaderAvailable() {
  try {
    execSync('python -m instaloader --version', { stdio: 'ignore' });
    return true;
  } catch (_) {
    return false;
  }
}

/**
 * Downloads media (photos, videos, reels, or all) from an Instagram profile.
 */
function downloadInstagramProfile({
  jobId,
  username,
  mediaScope = 'all', // 'all' (photos + videos), 'video' (videos only), 'image' (photos only)
  cookieSource = 'cookies.txt',
  downloadDir,
  cookiesFile,
  galleryDlExe,
  onProgress,
  activeProcesses
}) {
  return new Promise((resolve, reject) => {
    const targetDir = path.join(downloadDir, 'Instagram', username);
    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }

    let tempCookiesPath = null;
    const cleanupTempCookies = () => {
      if (tempCookiesPath && fs.existsSync(tempCookiesPath)) {
        try { fs.unlinkSync(tempCookiesPath); } catch (_) {}
      }
    };

    const hasGalleryDl = fs.existsSync(galleryDlExe);
    const hasInstaloader = isInstaloaderAvailable();

    let cmd = '';
    let args = [];

    if (hasGalleryDl) {
      // Primary: Use gallery-dl.exe (most reliable for Instagram profiles and Netscape cookies)
      cmd = galleryDlExe;
      args = [
        '-D', targetDir,
        '--no-part'
      ];

      // Scope filter: 'video' = videos only, 'image'/'photo' = photos only, 'all' = both photos & videos
      if (mediaScope === 'video') {
        args.push('--filter', 'extension in exts_video');
      } else if (mediaScope === 'image' || mediaScope === 'photo') {
        args.push('--filter', 'extension in exts_image');
      }

      if (['chrome', 'edge', 'firefox', 'brave'].includes(cookieSource.toLowerCase())) {
        args.push('--cookies-from-browser', cookieSource.toLowerCase());
      } else if (fs.existsSync(cookiesFile)) {
        tempCookiesPath = path.join(os.tmpdir(), `ytdlp_ig_cookies_${jobId}.txt`);
        try {
          let rawCookies = fs.readFileSync(cookiesFile, 'utf8');
          // Normalize #HttpOnly_ prefixes so all HttpOnly cookies (like sessionid) are parsed as standard cookies
          rawCookies = rawCookies.replace(/#HttpOnly_/g, '');
          fs.writeFileSync(tempCookiesPath, rawCookies, 'utf8');
          args.push('--cookies', tempCookiesPath);
        } catch (_) {
          args.push('--cookies', cookiesFile);
        }
      }

      args.push(`https://www.instagram.com/${username}/`);
    } else if (hasInstaloader) {
      // Fallback: Use Python Instaloader
      cmd = 'python';
      args = [
        '--no-video-thumbnails',
        '--no-profile-pic',
        '--no-metadata-json',
        '--no-captions',
        '--abort-on', '429',       // CRITICAL: Do NOT sleep/freeze for 11 minutes on 429
        '--dirname-pattern', targetDir,
      ];

      if (mediaScope === 'video') {
        args.push('--no-pictures', '--reels');
      } else if (mediaScope === 'image' || mediaScope === 'photo') {
        args.push('--no-videos');
      } else {
        args.push('--reels');
      }

      if (['chrome', 'edge', 'firefox', 'brave'].includes(cookieSource.toLowerCase())) {
        args.push('--load-cookies', cookieSource.toLowerCase());
      } else if (fs.existsSync(cookiesFile)) {
        tempCookiesPath = path.join(os.tmpdir(), `ytdlp_ig_cookies_${jobId}.txt`);
        try {
          fs.copyFileSync(cookiesFile, tempCookiesPath);
          args.push('--cookiefile', tempCookiesPath);
        } catch (_) {
          args.push('--cookiefile', cookiesFile);
        }
      }

      args.push(username);
    } else {
      return reject(new Error('Neither gallery-dl.exe nor Instaloader (Python) was found to download Instagram profiles.'));
    }

    const proc = spawn(cmd, args, { windowsHide: true });
    if (activeProcesses && jobId) {
      activeProcesses.set(jobId, proc);
    }

    let downloadedCount = 0;
    let currentFile = 'Connecting to Instagram...';
    let stderrBuf = '';
    let stdoutBuf = '';

    if (onProgress) {
      onProgress({ downloadedCount: 0, currentFile, log: `Starting scraper for @${username}...` });
    }

    const handleOutputChunk = (chunk) => {
      const text = chunk.toString();
      stdoutBuf += text;
      const lines = text.split('\n');

      for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line) continue;

        // Detect downloaded media file patterns (photos and videos)
        const mediaMatch = line.match(/([a-zA-Z0-9_\-\.]+\.(?:mp4|webm|mov|mkv|jpg|jpeg|png|webp))/i);
        if (mediaMatch) {
          currentFile = mediaMatch[1];
          downloadedCount++;
          if (onProgress) {
            onProgress({ downloadedCount, currentFile, log: `Downloaded ${currentFile}` });
          }
        } else if (line.includes('429') || line.includes('Too Many Requests')) {
          if (onProgress) {
            onProgress({ downloadedCount, currentFile: 'Rate Limit (429)', log: 'Instagram rate limit (429) hit. Switch to browser session!' });
          }
        } else if (line.includes('Retrieving posts') || line.includes('Scanning') || line.includes('JSON Query')) {
          if (onProgress) {
            onProgress({ downloadedCount, currentFile: 'Scanning profile media...', log: line });
          }
        } else {
          if (onProgress) {
            onProgress({ downloadedCount, currentFile: currentFile || 'Scanning profile...', log: line });
          }
        }
      }
    };

    proc.stdout.on('data', handleOutputChunk);
    proc.stderr.on('data', (chunk) => {
      const str = chunk.toString();
      stderrBuf += str;
      handleOutputChunk(chunk);
    });

    proc.on('close', (code) => {
      cleanupTempCookies();
      if (activeProcesses && jobId) {
        activeProcesses.delete(jobId);
      }

      // Check if files actually exist in targetDir
      let totalFiles = 0;
      try {
        const files = fs.readdirSync(targetDir);
        totalFiles = files.filter(f => /\.(mp4|webm|mov|mkv|jpg|jpeg|png|webp)$/i.test(f)).length;
      } catch (_) {}

      if (code === 0 || totalFiles > 0) {
        resolve({
          success: true,
          downloadedCount: Math.max(downloadedCount, totalFiles),
          folder: targetDir
        });
      } else {
        // Parse error message
        let errMsg = 'Failed to download Instagram profile.';
        if (stderrBuf.includes('429') || stdoutBuf.includes('429') || stderrBuf.includes('Too Many Requests')) {
          errMsg = 'Instagram rate-limit (429): Instagram blocked profile scraping without an active login. Fix: Export cookies from your logged-in browser session into cookies.txt (ensure "sessionid" is included).';
        } else if (stderrBuf.includes('login') || stderrBuf.includes('Login') || stdoutBuf.includes('Login') || stderrBuf.includes('checkpoint_required')) {
          errMsg = 'Instagram login session required. Please export cookies from your logged-in browser into cookies.txt (ensure "sessionid" is included).';
        } else if (stderrBuf.includes('BrowserCookieError') || stderrBuf.includes('Permission denied') || stderrBuf.includes('DPAPI') || stderrBuf.includes('browser_cookie3')) {
          errMsg = 'Browser cookies could not be read directly (browser database locked or encrypted). Please export your cookies using the browser extension into cookies.txt and select "cookies.txt" as Cookie Source.';
        } else if (stderrBuf.includes('not found') || stdoutBuf.includes('not found') || stderrBuf.includes('ProfileNotExists') || stderrBuf.includes('NotFoundError')) {
          errMsg = `Instagram profile '@${username}' not found, is private, or requires authentication to view.`;
        } else {
          const errLines = (stderrBuf || stdoutBuf).split('\n').map(l => l.trim()).filter(Boolean);
          errMsg = errLines.reverse().find(l => l.toLowerCase().includes('error')) || errLines[0] || `Exit code ${code}`;
        }
        reject(new Error(errMsg));
      }
    });

    proc.on('error', (err) => {
      cleanupTempCookies();
      if (activeProcesses && jobId) {
        activeProcesses.delete(jobId);
      }
      reject(err);
    });
  });
}

module.exports = {
  extractInstagramUsername,
  isInstaloaderAvailable,
  downloadInstagramProfile
};
