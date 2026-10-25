const { Queue, QueueEvents } = require('bullmq');
const Redis = require('ioredis');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);

const redisConnection = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  retryStrategy(times) {
    // Retry connection every 2 seconds
    return Math.min(times * 200, 2000);
  }
});

let isRedisConnected = false;

redisConnection.on('connect', () => {
  isRedisConnected = true;
  console.log('  📦 BullMQ: Connected to Redis on ' + REDIS_HOST + ':' + REDIS_PORT);
});

redisConnection.on('error', (err) => {
  isRedisConnected = false;
  console.error('  ⚠️ Redis connection error:', err.message);
});

// Primary download queue for yt-dlp & Instagram downloads
const downloadQueue = new Queue('downloadQueue', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 100 }
  }
});

// Map of jobId -> child_process instance (for killing active downloads on cancel)
const activeProcesses = new Map();

module.exports = {
  redisConnection,
  downloadQueue,
  activeProcesses,
  isRedisReady: () => isRedisConnected
};
