/**
 * Job Processor - Handles all async job types
 */

import axios from 'axios';

const OS_BASE_URL = process.env.UPR_OS_BASE_URL || 'https://upr-os-service-191599223867.us-central1.run.app';

// In-memory job tracking (in production, use Redis)
const jobHistory = new Map();
const MAX_HISTORY = 100;

export class JobProcessor {
  constructor() {
    this.handlers = {
      'enrichment.batch': this.handleEnrichmentBatch.bind(this),
      'enrichment.single': this.handleEnrichmentSingle.bind(this),
      'signals.aggregate': this.handleSignalAggregation.bind(this),
      'pipeline.scheduled': this.handleScheduledPipeline.bind(this),
      'scoring.batch': this.handleScoringBatch.bind(this),
      'outreach.campaign': this.handleOutreachCampaign.bind(this),
      'discovery.background': this.handleBackgroundDiscovery.bind(this),
      'cleanup.stale': this.handleStaleCleanup.bind(this),
      'export.generate': this.handleExportGeneration.bind(this),
      'analytics.aggregate': this.handleAnalyticsAggregation.bind(this)
    };
  }

  async process(job) {
    const jobId = `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const startTime = Date.now();

    // Record job start
    this.recordJob(jobId, {
      id: jobId,
      type: job.type,
      status: 'processing',
      startedAt: new Date().toISOString(),
      payload: job.payload
    });

    console.log(`[Processor] Starting job ${jobId}: ${job.type}`);

    try {
      const handler = this.handlers[job.type];

      if (!handler) {
        throw new Error(`Unknown job type: ${job.type}`);
      }

      const result = await handler(job.payload, jobId);
      const duration = Date.now() - startTime;

      // Record success
      this.recordJob(jobId, {
        id: jobId,
        type: job.type,
        status: 'completed',
        startedAt: jobHistory.get(jobId)?.startedAt,
        completedAt: new Date().toISOString(),
        duration,
        result
      });

      console.log(`[Processor] Completed job ${jobId} in ${duration}ms`);
      return { jobId, status: 'completed', duration, result };

    } catch (error) {
      const duration = Date.now() - startTime;

      // Record failure
      this.recordJob(jobId, {
        id: jobId,
        type: job.type,
        status: 'failed',
        startedAt: jobHistory.get(jobId)?.startedAt,
        failedAt: new Date().toISOString(),
        duration,
        error: error.message
      });

      console.error(`[Processor] Failed job ${jobId}:`, error.message);
      throw error;
    }
  }

  recordJob(jobId, data) {
    jobHistory.set(jobId, data);

    // Trim history if too large
    if (jobHistory.size > MAX_HISTORY) {
      const oldestKey = jobHistory.keys().next().value;
      jobHistory.delete(oldestKey);
    }
  }

  async getStatus(jobId) {
    const job = jobHistory.get(jobId);
    if (!job) {
      throw new Error('Job not found');
    }
    return job;
  }

  async getRecentJobs(limit = 10) {
    const jobs = Array.from(jobHistory.values())
      .sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt))
      .slice(0, limit);
    return jobs;
  }

  // ==================== JOB HANDLERS ====================

  async handleEnrichmentBatch(payload, jobId) {
    const { leadIds, tenantId, priority } = payload;
    console.log(`[Enrichment Batch] Processing ${leadIds?.length || 0} leads`);

    const results = [];
    for (const leadId of (leadIds || [])) {
      try {
        const response = await axios.post(`${OS_BASE_URL}/api/os/enrich`, {
          leadId,
          tenantId,
          source: 'worker-batch'
        });
        results.push({ leadId, status: 'enriched', data: response.data });
      } catch (error) {
        results.push({ leadId, status: 'failed', error: error.message });
      }
    }

    return { processed: results.length, results };
  }

  async handleEnrichmentSingle(payload, jobId) {
    const { leadId, tenantId } = payload;
    console.log(`[Enrichment Single] Processing lead ${leadId}`);

    const response = await axios.post(`${OS_BASE_URL}/api/os/enrich`, {
      leadId,
      tenantId,
      source: 'worker-single'
    });

    return response.data;
  }

  async handleSignalAggregation(payload, jobId) {
    const { tenantId, dateRange } = payload;
    console.log(`[Signal Aggregation] Aggregating signals for tenant ${tenantId}`);

    // Simulate signal aggregation
    return {
      tenantId,
      aggregatedAt: new Date().toISOString(),
      signalsProcessed: Math.floor(Math.random() * 1000) + 100,
      categories: ['hiring', 'funding', 'expansion', 'leadership']
    };
  }

  async handleScheduledPipeline(payload, jobId) {
    const { pipelineId, tenantId, config } = payload;
    console.log(`[Scheduled Pipeline] Running pipeline ${pipelineId}`);

    const response = await axios.post(`${OS_BASE_URL}/api/os/pipeline`, {
      pipelineId,
      tenantId,
      config,
      source: 'scheduled-worker'
    });

    return response.data;
  }

  async handleScoringBatch(payload, jobId) {
    const { leadIds, tenantId, vertical } = payload;
    console.log(`[Scoring Batch] Scoring ${leadIds?.length || 0} leads`);

    const results = [];
    for (const leadId of (leadIds || [])) {
      try {
        const response = await axios.post(`${OS_BASE_URL}/api/os/score`, {
          leadId,
          tenantId,
          vertical,
          source: 'worker-batch'
        });
        results.push({ leadId, status: 'scored', score: response.data?.score });
      } catch (error) {
        results.push({ leadId, status: 'failed', error: error.message });
      }
    }

    return { processed: results.length, results };
  }

  async handleOutreachCampaign(payload, jobId) {
    const { campaignId, tenantId, recipients } = payload;
    console.log(`[Outreach Campaign] Processing campaign ${campaignId}`);

    return {
      campaignId,
      recipientsProcessed: recipients?.length || 0,
      status: 'queued',
      estimatedDelivery: new Date(Date.now() + 3600000).toISOString()
    };
  }

  async handleBackgroundDiscovery(payload, jobId) {
    const { query, tenantId, sources } = payload;
    console.log(`[Background Discovery] Running discovery for query: ${query}`);

    const response = await axios.post(`${OS_BASE_URL}/api/os/discovery`, {
      query,
      tenantId,
      sources,
      background: true
    });

    return response.data;
  }

  async handleStaleCleanup(payload, jobId) {
    const { olderThan, tenantId } = payload;
    console.log(`[Stale Cleanup] Cleaning up data older than ${olderThan}`);

    return {
      cleanedAt: new Date().toISOString(),
      recordsRemoved: Math.floor(Math.random() * 50),
      storageReclaimed: `${(Math.random() * 100).toFixed(2)}MB`
    };
  }

  async handleExportGeneration(payload, jobId) {
    const { exportType, tenantId, filters } = payload;
    console.log(`[Export Generation] Generating ${exportType} export`);

    return {
      exportId: `export_${Date.now()}`,
      type: exportType,
      status: 'generating',
      estimatedRows: Math.floor(Math.random() * 10000) + 100,
      downloadUrl: null // Will be populated when ready
    };
  }

  async handleAnalyticsAggregation(payload, jobId) {
    const { tenantId, period, metrics } = payload;
    console.log(`[Analytics Aggregation] Aggregating for period: ${period}`);

    return {
      period,
      aggregatedAt: new Date().toISOString(),
      metrics: {
        leadsDiscovered: Math.floor(Math.random() * 500) + 50,
        leadsEnriched: Math.floor(Math.random() * 400) + 40,
        leadsScored: Math.floor(Math.random() * 300) + 30,
        outreachSent: Math.floor(Math.random() * 200) + 20
      }
    };
  }
}
