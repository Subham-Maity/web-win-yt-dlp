const { Queue, QueueEvents } = require('bullmq');
const Redis = require('ioredis');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);

const redisOptions = {
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null,
  enableReadyCheck: false
};

const redisConnection = new Redis({
  ...redisOptions,
  enableReadyCheck: true,
  connectTimeout: 2000,
  commandTimeout: 2000,
  retryStrategy(times) {
    // Retry connection every 3 seconds
    return Math.min(times * 300, 3000);
  }
});

let isRedisConnected = false;
let loggedRedisError = false;

redisConnection.on('ready', () => {
  isRedisConnected = true;
  loggedRedisError = false;
  console.log('  📦 BullMQ: Connected & Ready on Redis ' + REDIS_HOST + ':' + REDIS_PORT);
});

redisConnection.on('close', () => {
  isRedisConnected = false;
});

redisConnection.on('end', () => {
  isRedisConnected = false;
});

redisConnection.on('error', (err) => {
  isRedisConnected = false;
  if (!loggedRedisError) {
    loggedRedisError = true;
    console.warn('  ⚠️ Redis connection unavailable (' + err.message + '). Direct execution mode active.');
  }
});

// Primary download queue for yt-dlp, Lulustream & Instagram downloads
// Passing redisOptions allows BullMQ to manage dedicated connection pools per Queue and Worker
const downloadQueue = new Queue('downloadQueue', {
  connection: redisOptions,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 100 }
  }
});

downloadQueue.on('error', () => {
  // Handled silently when Redis is disconnected; direct mode handles requests
});

// Sequential 1-by-1 queue for Full Webpage Scraper (resolves fresh stream just-in-time)
const scrapeQueue = new Queue('scrapeQueue', {
  connection: redisOptions,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 100 }
  }
});

scrapeQueue.on('error', () => {
  // Handled silently when Redis is disconnected; direct sequential mode handles requests
});

// Map of jobId -> child_process instance (for killing active downloads on cancel)
const activeProcesses = new Map();

async function isRedisAlive() {
  if (!isRedisConnected || redisConnection.status !== 'ready') return false;
  try {
    const res = await Promise.race([
      redisConnection.ping(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 1000))
    ]);
    return res === 'PONG';
  } catch (_) {
    isRedisConnected = false;
    return false;
  }
}

module.exports = {
  redisOptions,
  redisConnection,
  downloadQueue,
  scrapeQueue,
  activeProcesses,
  isRedisReady: () => isRedisConnected && redisConnection.status === 'ready',
  isRedisAlive
};
