import { rpcCall } from '@/api/rpc.ts'
import type { JobId, JobStatusResult } from '@/api/types.ts'

export interface JobProgress {
  done: number
  total: number
}

export async function waitForJob<T>(
  jobId: JobId,
  onProgress?: (progress: JobProgress) => void,
  intervalMs = 400,
): Promise<T> {
  for (;;) {
    const status = await rpcCall<JobStatusResult>('jobStatus', { job_id: jobId })

    if (status.state === 'running') {
      onProgress?.({ done: status.done, total: status.total })
      await sleep(intervalMs)
      continue
    }
    if (status.state === 'error') {
      throw new Error(status.error ?? 'Job failed')
    }
    onProgress?.({ done: status.total, total: status.total })
    return status.result as T
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
