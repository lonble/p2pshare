// Mirrors internal/rpcapi/api.go. Keep in sync with the backend by hand -
// there's no code generation wired up (yet).

export interface Contact {
  id: string
  addr: string
}

export interface StatusResult {
  id: string
  peers: number
}

export type PeersResult = Contact[]

export interface ListFilesResultEntry {
  id: string
  name: string
  size: number
  chunk_size: number
  chunks: number
}

export type ListFilesResult = ListFilesResultEntry[]

export interface PublishParams {
  path: string
}

export interface Manifest {
  name: string
  size: number
  chunk_size: number
  chunks: string[]
}

export interface PublishResult {
  id: string
  manifest: Manifest
}

export interface DownloadParams {
  id: string
  outdir: string
}

export interface DownloadResult {
  ok: boolean
  output: string
}

export type BootstrapParams = Contact[]

export interface BootstrapResult {
  ok: boolean
}

export type JobId = string

export type JobState = 'running' | 'done' | 'error'

export interface JobStatusParams {
  job_id: JobId
}

export interface PublishAsyncResult {
  job_id: JobId
}

export interface DownloadAsyncResult {
  job_id: JobId
}

export interface JobStatusResult {
  state: JobState
  done: number
  total: number
  result?: unknown
  error?: string
}
