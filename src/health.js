/**
 * Health Check Module
 */

import axios from 'axios';

const OS_BASE_URL = process.env.UPR_OS_BASE_URL || 'https://upr-os-service-191599223867.us-central1.run.app';

export async function healthCheck() {
  const checks = {
    worker: { status: 'healthy' },
    os: { status: 'unknown' },
    memory: { status: 'healthy' }
  };

  // Check OS connectivity
  try {
    const response = await axios.get(`${OS_BASE_URL}/health`, { timeout: 5000 });
    checks.os = {
      status: response.data?.status === 'ok' ? 'healthy' : 'degraded',
      reachable: true
    };
  } catch (error) {
    checks.os = {
      status: 'unhealthy',
      reachable: false,
      error: error.message
    };
  }

  // Check memory
  const memUsage = process.memoryUsage();
  const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
  const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);

  checks.memory = {
    status: heapUsedMB < heapTotalMB * 0.9 ? 'healthy' : 'warning',
    heapUsedMB,
    heapTotalMB,
    percentage: Math.round((heapUsedMB / heapTotalMB) * 100)
  };

  // Overall health
  const healthy = checks.worker.status === 'healthy' && checks.os.status !== 'unhealthy';

  return {
    healthy,
    status: healthy ? 'ok' : 'degraded',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    version: '1.0.0',
    checks
  };
}
