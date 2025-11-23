/**
 * UPR OS Worker - Unified Async Pipeline Processor
 *
 * Handles:
 * - Enrichment batch jobs
 * - Signal aggregation
 * - Scheduled pipeline runs
 * - Background data processing
 */

import express from 'express';
import { PubSub } from '@google-cloud/pubsub';
import { JobProcessor } from './processor.js';
import { healthCheck } from './health.js';

const app = express();
const PORT = process.env.PORT || 8080;

// Initialize Pub/Sub client
const pubsub = new PubSub();
const SUBSCRIPTION_NAME = process.env.PUBSUB_SUBSCRIPTION || 'upr-os-worker-sub';

// Job processor instance
const processor = new JobProcessor();

// Middleware
app.use(express.json());

// Health endpoint (required for Cloud Run)
app.get('/health', async (req, res) => {
  const status = await healthCheck();
  res.status(status.healthy ? 200 : 503).json(status);
});

// Ready endpoint
app.get('/ready', async (req, res) => {
  res.json({ status: 'ready', worker: 'upr-os-worker', version: '1.0.0' });
});

// Manual job trigger (for testing/admin)
app.post('/jobs/trigger', async (req, res) => {
  try {
    const { jobType, payload } = req.body;

    if (!jobType) {
      return res.status(400).json({ error: 'jobType is required' });
    }

    const result = await processor.process({
      type: jobType,
      payload: payload || {},
      triggeredAt: new Date().toISOString(),
      source: 'manual'
    });

    res.json({ success: true, result });
  } catch (error) {
    console.error('Job trigger error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Pub/Sub push endpoint (Cloud Run receives messages here)
app.post('/pubsub/push', async (req, res) => {
  try {
    const message = req.body.message;

    if (!message || !message.data) {
      return res.status(400).json({ error: 'Invalid Pub/Sub message' });
    }

    // Decode message
    const data = JSON.parse(Buffer.from(message.data, 'base64').toString());

    console.log(`[Worker] Received job: ${data.type}`);

    // Process the job
    await processor.process(data);

    // Acknowledge by returning 200
    res.status(200).json({ success: true });
  } catch (error) {
    console.error('Pub/Sub processing error:', error);
    // Return 500 to trigger retry
    res.status(500).json({ error: error.message });
  }
});

// Job status endpoint
app.get('/jobs/status/:jobId', async (req, res) => {
  try {
    const status = await processor.getStatus(req.params.jobId);
    res.json(status);
  } catch (error) {
    res.status(404).json({ error: 'Job not found' });
  }
});

// List recent jobs
app.get('/jobs/recent', async (req, res) => {
  try {
    const jobs = await processor.getRecentJobs(parseInt(req.query.limit) || 10);
    res.json({ jobs });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════════╗
║           UPR OS WORKER v1.0.0                           ║
║           Unified Async Pipeline Processor               ║
╠══════════════════════════════════════════════════════════╣
║  Port: ${PORT}                                            ║
║  Environment: ${process.env.NODE_ENV || 'development'}                       ║
║  Pub/Sub: ${SUBSCRIPTION_NAME}                ║
╚══════════════════════════════════════════════════════════╝
  `);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Worker] SIGTERM received, shutting down...');
  process.exit(0);
});
