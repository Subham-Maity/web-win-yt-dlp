const SERVER_BASE = 'http://127.0.0.1:3456';

async function checkServer() {
  const box = document.getElementById('statusBox');
  try {
    const res = await fetch(`${SERVER_BASE}/scraper/bridge/status`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const d = await res.json();
    box.className = 'status ok';
    box.textContent = d.bridgeConnected
      ? '🟢 Connected to local downloader (Bridge Active)'
      : '🟢 Local server reachable (syncing bridge...)';
  } catch (_) {
    box.className = 'status err';
    box.textContent = '🔴 Local server offline (Start run.bat first)';
  }
}

document.getElementById('btnScanTab').addEventListener('click', () => {
  const msg = document.getElementById('msg');
  msg.textContent = 'Scanning active tab for video links...';
  chrome.runtime.sendMessage({ type: 'SCAN_ACTIVE_TAB' }, (resp) => {
    if (resp && resp.ok) {
      msg.textContent = `✅ Sent ${resp.count} video link(s) to the Full Webpage Scraper tab!`;
    } else {
      msg.textContent = '❌ ' + ((resp && resp.error) || 'Failed to scan tab');
    }
  });
});

document.getElementById('btnOpenUi').addEventListener('click', () => {
  chrome.tabs.create({ url: SERVER_BASE });
});

checkServer();
