# yt-dlp Web Downloader ⬇️
<img width="1896" height="876" alt="image" src="https://github.com/user-attachments/assets/3dd4176a-7b3d-42b0-af4c-01788c81dfce" />


A lightweight, modern web-based user interface for downloading videos from YouTube, Instagram, and other websites using `yt-dlp`. 

This downloader includes support for cookies to bypass bot detection, a convenient Windows startup script, and a beautiful queue manager dashboard.

---

## 🚀 Quick Start Guide (Windows)

Follow these step-by-step instructions to get the downloader up and running on your system.

### Step 1: Install Node.js & npm
Node.js and npm (Node Package Manager) are required to run the web server.

1. **Download Node.js:**
   - Go to the official Node.js website: [nodejs.org](https://nodejs.org)
   - Download the **LTS (Long Term Support)** installer for Windows (recommended for most users).

2. **Run the Installer:**
   - Double-click the downloaded `.msi` file.
   - Click **Next** through the setup wizard.
   - **Crucial:** When you reach the **Custom Setup** screen, ensure that **"Add to PATH"** is selected (it is checked by default). This allows Node.js to be run from command lines and batch scripts.
   - Click **Install** and then **Finish**.

3. **Verify the Installation:**
   - Open **Command Prompt** (press `Win + R`, type `cmd`, and press Enter) or **PowerShell**.
   - Run the following command to check if Node.js is installed:
     ```bash
     node -v
     ```
   - Run the following command to check if npm is installed:
     ```bash
     npm -v
     ```
   - If both commands return version numbers (e.g., `v18.x.x` or `v20.x.x`), you are ready to proceed!

---

### Step 2: Set Up Cookies (Required for YouTube & Instagram)
To bypass website bot-detection mechanisms (like YouTube's bot block or Instagram's sign-in walls), the downloader needs to load session cookies from your browser.

1. **Install Cookie Extension:**
   - Install the safe and private Chrome Web Store extension: **[Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc?hl=en)**.
   - *Note: This extension runs entirely locally in your browser and does not share your data with external servers.*

2. **Export Your Cookies:**
   - Open your browser and navigate to the website you want to download from (e.g., [youtube.com](https://www.youtube.com) or [instagram.com](https://www.instagram.com)).
   - **Make sure you are logged in to your account.**
   - Click on the **Get cookies.txt LOCALLY** extension icon in your browser's toolbar.
   - Click the **Export** button to download the cookie file. It will export your cookies in the standard Netscape format.

3. **Place cookies.txt in the Project Folder:**
   - Find the exported file (it might be named something like `youtube.com_cookies.txt` or `cookies.txt`).
   - **Rename** it to exactly `cookies.txt`.
   - Move or copy this file into the root of this project folder:
     `C:\Github\yt dlp\cookies.txt`

---

### Step 3: Run the Downloader
You do not need to manually install dependencies or type commands to start the server!

1. Open the project folder `C:\Github\yt dlp` in Windows Explorer.
2. Double-click the **`run.bat`** file.
3. **What happens on launch:**
   - The batch script automatically checks if Node.js is installed.
   - **On the first launch**, it will automatically download and install the required dependencies (such as Express) via `npm install`.
   - It will start the server and automatically launch your default web browser to the Web UI at **`http://localhost:3456`**.
4. **Keep the Command Prompt window open** while downloading. The console will display active progress logs.

---

## 📺 How to Download Videos

1. **Open the Web UI:**
   - Go to `http://localhost:3456` in your browser (opened automatically by `run.bat`).
2. **Add URLs:**
   - Paste the link of the video you want to download in the input box.
   - To download multiple videos, click **`＋ Add URL`** or simply paste multiple links (separated by newlines or spaces) directly into a single box. They will automatically split into individual fields.
3. **Download:**
   - Click **`⬇ Download All`**.
   - You will see the download progress bars update in real time in the **Download Queue**.
4. **Locate Downloaded Files:**
   - By default, files are saved to your system's download directory under a folder called `yt-dlp`:
     `C:\Users\<YourUsername>\Downloads\yt-dlp`
   - You can quickly open this folder by clicking the **`Open Folder ↗`** link at the top of the Web UI page.

---

## ⏹ Stopping the Downloader

- To stop the web server, simply go to the Command Prompt window opened by `run.bat`, press **`Ctrl + C`**, and type `Y` to confirm. Or simply close the Command Prompt window.

---

## 📁 Project Structure

```
C:\Github\yt dlp\
├── public/                 # Web UI frontend (HTML/CSS/JS)
├── node_modules/           # Node.js dependencies (installed automatically)
├── server.js               # Node.js backend server
├── yt-dlp.exe              # The core downloader executable
├── cookies.txt             # Exported cookies file (Add your own)
├── run.bat                 # Click to automatically install dependencies and run
├── bulk.txt                # CLI utility URLs list
├── bulk_download.bat       # CLI utility for bulk downloads
└── readme.md               # This documentation
```

Enjoy downloading! 🎥
