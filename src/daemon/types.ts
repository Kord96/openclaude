export interface BackendConfig {
  name: string
  profile: string
  provider?: string
  model: string
  base_url?: string | null
  api_key_env?: string | null
  api_key_ref?: string | null
  env_passthrough?: string[]
  extra_env?: Record<string, string>
}

export interface BackendPool {
  version?: number
  selection?: 'first' | 'random' | 'hash'
  backends?: BackendConfig[]
}

export interface ProfileData {
  profile: string
  provider?: string
  model?: string
  backend_name?: string
  base_url?: string | null
  api_key_env?: string | null
  api_key_ref?: string | null
  env_passthrough?: string[]
  extra_env?: Record<string, string>
  selection?: string
  version?: number
}

export interface Job {
  id: string
  type?: string
  from?: string
  agent: string
  skill?: string | null
  prompt: string
  project?: string | null
  repo?: string | null
  correlation_id: string
  created_at: string
  timeout_ms?: number
  metadata?: Record<string, unknown>
}

export type JobStatus = 'success' | 'error' | 'timeout' | 'cancelled'

export interface BackendInfo {
  name: string | null
  profile: string | null
  provider: string
  model: string
  model_spec: string
  selection: string
}

export interface JobResult {
  type: 'result'
  id: string
  from: string
  correlation_id: string | null
  agent: string
  skill: string | null
  status: JobStatus
  output: string
  started_at: string
  finished_at: string
  pod_name: string
  job_number: number
  backend: BackendInfo
}

export interface MemoryChanges {
  modified: string[]
  added: string[]
  removed: string[]
}

export interface Watcher {
  dir: string
  snapshot: () => number
  detectChanges: () => MemoryChanges
}

export interface ActiveJob {
  id: string
  correlationId: string | null
  startedAt: string
  backend?: Partial<BackendInfo>
}

export interface ClaudeResult {
  text: string
  is_error: boolean
}
