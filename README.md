# yt-dlp + BullMQ Pro Downloader ⬇️
<img width="1896" height="876" alt="image" src="https://github.com/user-attachments/assets/3dd4176a-7b3d-42b0-af4c-01788c81dfce" />

A high-performance, modern web downloader user interface for downloading videos and media from YouTube, Instagram, TikTok, and other platforms using `yt-dlp`, `gallery-dl`, and a robust **BullMQ (Redis)** job queue.

Now features a dedicated **Instagram Profile Media Downloader** that can scrape and download all **Photos, Reels, and Video Posts** from any Instagram profile till the end!

---

## ✨ Features

- **BullMQ Distributed Queue (Docker Redis)**: Powered by Redis for reliable job processing, concurrency control, and live status tracking.
- **📸 Instagram Profile Media Downloader (New & Enhanced)**:
  - Input any Instagram profile URL or `@username` (e.g., `https://www.instagram.com/cristiano/` or `@natgeo`).
  - **Photos + Videos Support**: Scrapes and downloads all photos, carousels, Reels, and video posts till completion.
  - **Download Target Scope**:
    - `📸 + 🎬 All Media (Photos + Videos + Reels)` *(Default)*
    - `🎬 Videos & Reels Only`
    - `📸 Photos & Images Only`
  - Files are neatly organized into `Downloads/yt-dlp/Instagram/<username>/`.
  - **Auto-Detection**: Pasting an Instagram profile URL into the Standard Downloader automatically detects and routes it to the Profile Worker.
- **🔑 Connect Instagram Session Modal**:
  - Live session detection (`🟢 Instagram Session Active` with User ID & cookie count).
  - **1-Click Browser Popup**: Opens Instagram directly in Chrome for seamless login.
  - **Quick Sync & Upload**: 1-click clipboard paste (`📋 Paste & Sync`), multi-line paste for Netscape / Cookie-Editor JSON, and `📁 Upload File` for `cookies.txt`.
- **Standard Video Downloader**:
  - Download individual videos or multiple URLs in bulk from YouTube, Instagram Reels, TikTok, X (Twitter), Facebook, and 1,000+ supported sites.
  - Multi-URL paste detection (splits pasted lines or spaces automatically).
- **Queue Management & Quick Cancellation**:
  - **🛑 Clear Queue**: 1-click button in the header and panel to abort in-flight downloads and purge the entire queue.
  - **Individual Job Controls**: Cancel running tasks or remove finished entries using the `✕` action button on each card.
  - **Automatic Direct Mode Fallback**: Automatically switches to standalone direct downloading if Redis/Docker is stopped or unresponsive, ensuring zero downtime.
- **Real-Time SSE Dashboard**: Live progress bars, speed, titles, and item completion tickers.
- **One-Click Startup (`run.bat`)**: Automatically checks and starts Docker Redis, installs dependencies, and launches the browser UI at `http://localhost:3456`.

---

## 🚀 Quick Start Guide (Windows)

### Step 1: Prerequisites
1. **Node.js (LTS)**:
   - Download and install from [nodejs.org](https://nodejs.org).
   - Ensure **"Add to PATH"** is selected during installation.
2. **Docker Desktop (For BullMQ Redis Queue)**:
   - Make sure **Docker Desktop** is installed and running on your system.
   - The launcher script will automatically start the lightweight `ytdlp-redis` container via Docker Compose.
3. **Python (Optional Fallback)**:
   - `gallery-dl.exe` is bundled for fast, standalone profile downloading. Python + `instaloader` is available as a secondary fallback.

---

### Step 2: Set Up Cookies (Instagram & YouTube Authentication)

To bypass bot detection walls (like YouTube's bot block or Instagram's Error 429 login requirements):

#### 1-Click Interactive Modal (Recommended)
1. Open the Web UI at `http://localhost:3456`.
2. Click the **📸 Instagram Profile Downloader** tab.
3. Click **`🔑 Login Popup / Sync`**.
4. Check your status banner:
   - If connected: `🟢 Instagram Session Active` will show your User ID.
   - If not connected: Click **`🚀 1. Open Instagram Window`** to log in, export cookies from **Cookie-Editor**, and click **`📋 2. Paste & Sync`** (or upload your `cookies.txt`).

#### Manual `cookies.txt` Export
1. Use the Chrome extension **Cookie-Editor** or **Get cookies.txt LOCALLY**.
2. Export your cookies in Netscape format.
3. Save as `cookies.txt` in the project root:
   ```text
   C:\Github\yt dlp\cookies.txt
   ```

---

### Step 3: Run the Downloader

You do not need to manually configure Docker, install packages, or run manual commands:

1. Open the project folder `C:\Github\yt dlp`.
2. Double-click **`run.bat`**.
3. **What `run.bat` does automatically:**
   - [1/4] Verifies Docker and launches the Redis container (`docker compose up -d`).
   - [2/4] Installs required npm packages (`express`, `bullmq`, `ioredis`) if missing.
   - [3/4] Verifies scraper binaries and dependencies.
   - [4/4] Launches the server and automatically opens `http://localhost:3456` in your default browser.

---

## 📺 How to Use the Web UI

### 1. Standard Video Downloader Tab
1. Open `http://localhost:3456`.
2. In the **🎬 Standard Downloader** tab, paste one or more video URLs (YouTube, Reels, TikTok, etc.).
3. Click **`⬇ Download Queue`**.
4. Track live download progress and file titles in the BullMQ queue below.

### 2. Instagram Profile Downloader Tab
1. Switch to the **📸 Instagram Profile Downloader** tab.
2. Enter the target Instagram profile URL or handle (e.g. `https://www.instagram.com/cristiano/` or `@natgeo`).
3. Choose your **Download Target Scope**:
   - `📸 + 🎬 All Media (Photos + Videos + Reels)`
   - `🎬 Videos & Reels Only`
   - `📸 Photos & Images Only`
4. Select your **Cookie Source** (`cookies.txt` or browser).
5. Click **`⚡ Fetch & Download Profile Media Till End`**.
6. Files will be saved into:
   ```text
   C:\Users\<YourUsername>\Downloads\yt-dlp\Instagram\<username>\
   ```

---

## 📁 Download Directory

Files are saved by default to your user Downloads folder:
- **General Videos**: `C:\Users\<YourUsername>\Downloads\yt-dlp\`
- **Instagram Profiles**: `C:\Users\<YourUsername>\Downloads\yt-dlp\Instagram\<username>\`

Click **`Open Downloads ↗`** in the top bar of the Web UI to open this folder immediately in Windows Explorer.

---

## 📁 Project Structure

```text
C:\Github\yt dlp\
├── public/                 # Web UI frontend (HTML/CSS/JS with tabs, modal & badges)
│   └── index.html
├── docker-compose.yml      # Docker Compose configuration for Redis container
├── queue.js                # BullMQ queue & Redis client configuration
├── instagramService.js     # Instagram profile media scraper (gallery-dl & instaloader)
├── instagramAuthService.js # Instagram session verification, cookie merging & modal API
├── server.js               # Express API server with BullMQ Worker, auto-routing & SSE
├── yt-dlp.exe              # yt-dlp binary for individual media downloads
├── gallery-dl.exe          # gallery-dl binary for Instagram profiles & photo galleries
├── cookies.txt             # User cookies file (ignored by git for security)
├── run.bat                 # One-click Windows startup script
├── bulk.txt                # CLI URLs list
├── bulk_download.bat       # CLI utility for bulk downloads
└── README.md               # Documentation
```

---

## ⏹ Stopping the Downloader

- To stop the server, go to the Command Prompt window opened by `run.bat` and press **`Ctrl + C`**, or close the terminal window.
- To stop the Redis Docker container when not in use:
  ```bash
  docker compose down
  ```

