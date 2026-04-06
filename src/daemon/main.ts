// Agent Pod Daemon — Kafka transport for openclaude headless mode.
//
// openclaude owns the persistent session, queue, and turn loop.
// This file only adapts Kafka messages into headless input and publishes
// headless result messages back to Kafka.

import { Kafka } from 'kafkajs'
import { createServer } from 'node:http'
import { randomUUID, createHash } from 'node:crypto'
import { readFileSync, readdirSync, existsSync, mkdirSync } from 'node:fs'
import { join, relative } from 'node:path'
import { execSync } from 'node:child_process'
import { runHeadless } from '../cli/print.js'
import { StructuredIO } from '../cli/structuredIO.js'
import { getCommands } from '../commands.js'
import { getTools } from '../tools.js'
import { getDefaultAppState } from '../state/AppStateStore.js'
import { createStore } from '../state/store.js'
import { onChangeAppState } from '../state/onChangeAppState.js'
import { getEmptyToolPermissionContext } from '../Tool.js'
import { setCwd } from '../utils/Shell.js'
import { Stream } from '../utils/stream.js'
import type { AppState } from '../state/AppStateStore.js'
import type {
  BackendConfig,
  BackendPool,
  ProfileData,
  Job,
  JobStatus,
  JobResult,
  ActiveJob,
  Watcher,
  MemoryChanges,
} from './types.js'

// ─── Config ───

const AGENT_NAME = process.env.AGENT_NAME
if (!AGENT_NAME) {
  console.error('AGENT_NAME required')
  process.exit(1)
}

const AGENT_PROJECT_DIR = process.env.AGENT_PROJECT_DIR ?? `/runtime/${AGENT_NAME}`
const AGENT_STATE_DIR = process.env.AGENT_STATE_DIR ?? `/kord/${AGENT_NAME}`
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'kafka-kafka-bootstrap.dev.svc.cluster.local:9092').split(',')
const STATUS_PORT = parseInt(process.env.STATUS_PORT ?? '9090')
const DEFAULT_TIMEOUT_MS = 1_800_000 // 30 minutes
const POD_NAME = process.env.HOSTNAME ?? `agent-${AGENT_NAME}-local`
const AGENT_ID = process.env.AGENT_ID ?? POD_NAME
const PROJECTS_ROOT = process.env.PROJECTS_ROOT ?? '/kord/shared/repos'
const SHARED_MEMORY_DIR = process.env.SHARED_MEMORY_DIR ?? '/kord/shared/memory'

// ─── Backend resolution ───

function readJson<T>(path: string): T | null {
  try {
    return JSON.parse(readFileSync(path, 'utf8')) as T
  } catch {
    return null
  }
}

function selectBackend(pool: BackendPool, podName: string): BackendConfig | null {
  if (!pool.backends?.length) return null
  const selection = pool.selection ?? 'first'
  if (selection === 'random') {
    return pool.backends[Math.floor(Math.random() * pool.backends.length)]!
  }
  if (selection === 'hash') {
    const seed = createHash('md5').update(podName).digest('hex')
    const index = parseInt(seed.slice(0, 8), 16) % pool.backends.length
    return pool.backends[index]!
  }
  return pool.backends[0]!
}

function resolveBackendEnv(backend: BackendConfig | ProfileData | null): Record<string, string> {
  if (!backend) return {}
  const env: Record<string, string> = {}
  if ('base_url' in backend && backend.base_url) {
    if (backend.profile === 'gemini') env.GEMINI_BASE_URL = backend.base_url
    else env.OPENAI_BASE_URL = backend.base_url
  }
  const model = 'model' in backend ? backend.model : undefined
  if (model) {
    if (backend.profile === 'anthropic') env.MODEL = model
    else if (backend.profile === 'gemini') env.GEMINI_MODEL = model
    else env.OPENAI_MODEL = model
  }
  const apiKeyEnv = 'api_key_env' in backend ? backend.api_key_env : undefined
  if (apiKeyEnv && process.env[apiKeyEnv]) {
    if (backend.profile === 'anthropic') env.ANTHROPIC_API_KEY = process.env[apiKeyEnv]!
    else if (backend.profile === 'gemini') env.GEMINI_API_KEY = process.env[apiKeyEnv]!
    else if (backend.profile !== 'ollama') env.OPENAI_API_KEY = process.env[apiKeyEnv]!
  }
  if (backend.profile === 'ollama' && !env.OPENAI_API_KEY) {
    env.OPENAI_API_KEY = 'ollama'
  }
  if (backend.extra_env && typeof backend.extra_env === 'object') {
    Object.assign(env, backend.extra_env)
  }
  if (Array.isArray(backend.env_passthrough)) {
    for (const key of backend.env_passthrough) {
      if (process.env[key] !== undefined) env[key] = process.env[key]!
    }
  }
  return env
}

let agentModel = 'sonnet'
let agentProvider = 'anthropic'
let agentModelSpec = 'anthropic:sonnet'
let profileData: ProfileData | null = null
let backendPool: BackendPool | null = null
let activeBackend: BackendConfig | null = null
let modelEnv: Record<string, string> = {}

backendPool = readJson<BackendPool>(`${AGENT_PROJECT_DIR}/.openclaude-backends.json`)
profileData = readJson<ProfileData>(`${AGENT_PROJECT_DIR}/.openclaude-profile.json`)

const manuallySelectedBackend =
  profileData?.selection === 'manual' && profileData.backend_name && backendPool?.backends?.length
    ? (backendPool.backends.find((b) => b.name === profileData!.backend_name) ?? null)
    : null

activeBackend = manuallySelectedBackend ?? (backendPool ? selectBackend(backendPool, POD_NAME) : null)

if (activeBackend) {
  profileData = {
    ...profileData,
    version: backendPool?.version ?? profileData?.version ?? 2,
    selection: manuallySelectedBackend ? 'manual' : (backendPool?.selection ?? profileData?.selection ?? 'first'),
    backend_name: activeBackend.name,
    profile: activeBackend.profile,
    provider: activeBackend.provider ?? activeBackend.profile,
    model: activeBackend.model,
    base_url: activeBackend.base_url ?? null,
    api_key_env: activeBackend.api_key_env ?? null,
    api_key_ref: activeBackend.api_key_ref ?? null,
    env_passthrough: activeBackend.env_passthrough ?? [],
    extra_env: activeBackend.extra_env ?? {},
  } as ProfileData
}

if (!profileData) {
  profileData = readJson<ProfileData>(`${AGENT_PROJECT_DIR}/.openclaude-profile.json`)
}

if (profileData) {
  agentProvider = profileData.provider ?? profileData.profile ?? agentProvider
  agentModel = profileData.model ?? agentModel
  agentModelSpec = `${agentProvider}:${agentModel}`
}

modelEnv = resolveBackendEnv(activeBackend ?? profileData)

if (!profileData) {
  try { agentModel = readFileSync(`${AGENT_PROJECT_DIR}/.model`, 'utf8').trim() } catch {}
  try { agentProvider = readFileSync(`${AGENT_PROJECT_DIR}/.provider`, 'utf8').trim() } catch {}
  try { agentModelSpec = readFileSync(`${AGENT_PROJECT_DIR}/.model-spec`, 'utf8').trim() } catch {}
}

if (agentProvider === 'claude') agentProvider = 'anthropic'
if (agentModelSpec.startsWith('claude:')) agentModelSpec = `anthropic:${agentModelSpec.slice('claude:'.length)}`
if (!agentModelSpec || agentModelSpec === 'anthropic:sonnet') agentModelSpec = `${agentProvider}:${agentModel}`

if (!profileData?.profile) {
  profileData = { profile: agentProvider, provider: agentProvider, model: agentModel, backend_name: agentProvider }
}

if (!activeBackend) {
  activeBackend = {
    name: profileData.backend_name ?? profileData.provider ?? profileData.profile,
    profile: profileData.profile,
    provider: profileData.provider ?? profileData.profile,
    model: profileData.model ?? agentModel,
    base_url: profileData.base_url ?? null,
    api_key_env: profileData.api_key_env ?? null,
    api_key_ref: profileData.api_key_ref ?? null,
    env_passthrough: profileData.env_passthrough ?? [],
    extra_env: profileData.extra_env ?? {},
  }
}

modelEnv = { ...modelEnv, ...resolveBackendEnv(activeBackend) }
agentProvider = activeBackend.provider ?? profileData.provider ?? profileData.profile ?? agentProvider
agentModel = activeBackend.model ?? profileData.model ?? agentModel
agentModelSpec = `${agentProvider}:${agentModel}`

for (const [k, v] of Object.entries(modelEnv)) {
  process.env[k] = v
}
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'
process.env.CLAUDE_CODE_DISABLE_MEMORY = '1'

// ─── Memory dirs / change detection ───

const AGENT_GLOBAL_DIR = `${AGENT_STATE_DIR}/memory/global`

function hashFile(filePath: string): string | null {
  try {
    return createHash('md5').update(readFileSync(filePath)).digest('hex')
  } catch {
    return null
  }
}

function walkDir(dir: string, base = dir): string[] {
  const entries: string[] = []
  if (!existsSync(dir)) return entries
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)
    if (entry.isDirectory()) entries.push(...walkDir(full, base))
    else entries.push(relative(base, full))
  }
  return entries
}

function createWatcher(dir: string): Watcher {
  const hashes = new Map<string, string | null>()

  function snapshot(): number {
    if (!existsSync(dir)) return 0
    const files = walkDir(dir)
    for (const rel of files) hashes.set(rel, hashFile(join(dir, rel)))
    return files.length
  }

  function detectChanges(): MemoryChanges {
    const changes: MemoryChanges = { modified: [], added: [], removed: [] }
    if (!existsSync(dir)) return changes
    const currentFiles = new Set(walkDir(dir))
    for (const rel of currentFiles) {
      const currentHash = hashFile(join(dir, rel))
      const prevHash = hashes.get(rel)
      if (!prevHash) {
        hashes.set(rel, currentHash)
        changes.added.push(rel)
      } else if (currentHash !== prevHash) {
        hashes.set(rel, currentHash)
        changes.modified.push(rel)
      }
    }
    for (const rel of hashes.keys()) {
      if (!currentFiles.has(rel)) {
        hashes.delete(rel)
        changes.removed.push(rel)
      }
    }
    return changes
  }

  return { dir, snapshot, detectChanges }
}

const globalWatcher = createWatcher(AGENT_GLOBAL_DIR)
const sharedWatcher = createWatcher(SHARED_MEMORY_DIR)

// ─── Logging / state ───

let currentJob: ActiveJob | null = null
let jobCount = 0
let sessionReady = false
let sessionFailed: string | null = null

function log(level: string, event: string, data: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({
    level,
    agent: AGENT_NAME,
    agent_id: AGENT_ID,
    job_id: currentJob?.id ?? null,
    correlation_id: currentJob?.correlationId ?? null,
    event,
    pod_name: POD_NAME,
    timestamp: new Date().toISOString(),
    ...data,
  }))
}

type ActiveExecution = {
  job: Job
  startedAt: string
  timeoutMs: number
  timedOut: boolean
  resolve: () => void
  reject: (error: Error) => void
  timeoutHandle: ReturnType<typeof setTimeout> | null
}

let activeExecution: ActiveExecution | null = null

// ─── Kafka ───

const kafka = new Kafka({
  clientId: `agent-${AGENT_NAME}-${POD_NAME}`,
  brokers: KAFKA_BROKERS,
  retry: { retries: 5, initialRetryTime: 1000 },
})

const consumer = kafka.consumer({
  groupId: `agent.${AGENT_NAME}`,
  sessionTimeout: 2_100_000,
  heartbeatInterval: 30_000,
  rebalanceTimeout: 120_000,
  maxWaitTimeInMs: 5000,
})
const producer = kafka.producer()

// ─── Headless session ownership ───

const inputStream = new Stream<string>()

const defaultState = getDefaultAppState()
const store = createStore<AppState>({
  ...defaultState,
  toolPermissionContext: {
    ...getEmptyToolPermissionContext(),
    mode: 'bypassPermissions',
  },
}, onChangeAppState)

function buildPrompt(job: Job): string {
  const globalChanges = globalWatcher.detectChanges()
  const sharedChanges = sharedWatcher.detectChanges()
  const allLines: string[] = []

  for (const f of globalChanges.modified) allLines.push(`- ${AGENT_GLOBAL_DIR}/${f} (updated — re-read this file)`)
  for (const f of globalChanges.added) allLines.push(`- ${AGENT_GLOBAL_DIR}/${f} (new — read this file)`)
  for (const f of globalChanges.removed) allLines.push(`- ${f} (removed from global memory — disregard previous content)`)
  for (const f of sharedChanges.modified) allLines.push(`- ${SHARED_MEMORY_DIR}/${f} (updated — re-read this file)`)
  for (const f of sharedChanges.added) allLines.push(`- ${SHARED_MEMORY_DIR}/${f} (new — read this file)`)
  for (const f of sharedChanges.removed) allLines.push(`- ${f} (removed from shared memory — disregard)`)

  let memoryRefresh = ''
  if (allLines.length) {
    memoryRefresh = `\nMemory changes since your last job:\n${allLines.join('\n')}\n`
    log('info', 'memory_changes', {
      global: { modified: globalChanges.modified.length, added: globalChanges.added.length, removed: globalChanges.removed.length },
      shared: { modified: sharedChanges.modified.length, added: sharedChanges.added.length, removed: sharedChanges.removed.length },
    })
  }

  if (job.project) {
    const projPath = `${PROJECTS_ROOT}/${job.project}`
    if (!existsSync(projPath)) {
      if (job.repo) {
        log('info', 'cloning_repo', { repo: job.repo, dest: projPath })
        mkdirSync(PROJECTS_ROOT, { recursive: true })
        execSync(`cd /tmp && git clone --depth 1 ${job.repo} clone-${job.project} && mv clone-${job.project} ${projPath}`, { timeout: 120_000 })
      } else {
        throw new Error(`Project not found: ${projPath}. Provide "repo" field to clone.`)
      }
    }
  }

  let context = `\n[Memory]\nGlobal: ${AGENT_GLOBAL_DIR}/\nShared: ${SHARED_MEMORY_DIR}/\n`
  if (job.project) {
    const projMemPath = `${AGENT_STATE_DIR}/memory/projects/${job.project}`
    context += `Project: ${projMemPath}/\nProject code: ${PROJECTS_ROOT}/${job.project}\n`
      + `Read the files in ${projMemPath}/ for your prior findings on this project (if the directory exists).\n`
  }

  const backendContext = `\n[Backend]\nName: ${activeBackend?.name ?? profileData?.backend_name ?? 'unknown'}\nProfile: ${profileData?.profile ?? 'unknown'}\nProvider: ${agentProvider}\nModel: ${agentModel}\nSelection: ${backendPool?.selection ?? profileData?.selection ?? 'first'}\n`
  return memoryRefresh + job.prompt + context + backendContext + `\n[Job ${job.id}]`
}

async function publishJobResult(job: Job, startedAt: string, status: JobStatus, output: string): Promise<void> {
  const result: JobResult = {
    type: 'result',
    id: randomUUID(),
    from: AGENT_ID,
    correlation_id: job.correlation_id,
    agent: AGENT_NAME!,
    skill: job.skill ?? null,
    status,
    output,
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    pod_name: POD_NAME,
    job_number: jobCount,
    backend: {
      name: activeBackend?.name ?? profileData?.backend_name ?? null,
      profile: profileData?.profile ?? null,
      provider: agentProvider,
      model: agentModel,
      model_spec: agentModelSpec,
      selection: backendPool?.selection ?? profileData?.selection ?? 'first',
    },
  }

  const replyTo = (job as Job & { reply_to?: string }).reply_to
  const replyTopic = typeof replyTo === 'string'
    ? replyTo
    : (job.from ? `agent.${job.from}` : null)

  if (!replyTopic) return

  await producer.send({
    topic: replyTopic,
    messages: [{ key: job.correlation_id, value: JSON.stringify(result) }],
  })
}

class KafkaStructuredIO extends StructuredIO {
  constructor(input: AsyncIterable<string>) {
    super(input, false)
  }

  async write(message: Parameters<StructuredIO['write']>[0]): Promise<void> {
    if (message.type === 'system' && message.subtype === 'init') {
      sessionReady = true
      log('info', 'session_ready_event', {
        session_id: message.session_id,
        model: agentModel,
      })
      return
    }

    if (message.type !== 'result') {
      return
    }

    const execution = activeExecution
    if (!execution) {
      log('warn', 'unexpected_result_without_active_job', {
        subtype: message.subtype,
      })
      return
    }

    if (execution.timeoutHandle) {
      clearTimeout(execution.timeoutHandle)
      execution.timeoutHandle = null
    }

    let status: JobStatus = 'success'
    let output = ''

    if (execution.timedOut) {
      status = 'timeout'
      output = `Job timed out after ${execution.timeoutMs}ms`
    } else if (message.is_error) {
      status = 'error'
      output = 'errors' in message && Array.isArray(message.errors)
        ? message.errors.join('\n')
        : ('result' in message ? message.result : 'Execution error')
    } else {
      output = 'result' in message ? message.result : ''
    }

    try {
      await publishJobResult(execution.job, execution.startedAt, status, output)
      if (status === 'success') {
        log('info', 'job_complete', { duration_ms: Date.now() - Date.parse(execution.startedAt), job_number: jobCount })
      } else {
        log(status === 'timeout' ? 'warn' : 'error', 'job_complete_with_status', {
          status,
          duration_ms: Date.now() - Date.parse(execution.startedAt),
          output: output.slice(0, 300),
        })
      }
      activeExecution = null
      currentJob = null
      execution.resolve()
    } catch (error) {
      const err = error as Error
      log('error', 'result_publish_failed', { error: err.message })
      activeExecution = null
      currentJob = null
      execution.reject(err)
    }
  }
}

function enqueueControlInterrupt(): void {
  inputStream.enqueue(JSON.stringify({
    type: 'control_request',
    request_id: randomUUID(),
    request: { subtype: 'interrupt' },
  }) + '\n')
}

async function startHeadlessSession(): Promise<void> {
  process.chdir(AGENT_PROJECT_DIR)
  setCwd(AGENT_PROJECT_DIR)

  const commands = await getCommands(AGENT_PROJECT_DIR)
  const commandsHeadless = commands.filter(command =>
    command.type === 'prompt' && !command.disableNonInteractive ||
    command.type === 'local' && command.supportsNonInteractive,
  )
  const tools = getTools(store.getState().toolPermissionContext)
  const structuredIO = new KafkaStructuredIO(inputStream)

  log('info', 'headless_boot', {
    tool_count: tools.length,
    command_count: commandsHeadless.length,
    model: agentModel,
    provider: agentProvider,
  })

  void runHeadless(
    inputStream,
    () => store.getState(),
    store.setState,
    commandsHeadless,
    tools,
    {},
    [],
    {
      continue: undefined,
      resume: undefined,
      resumeSessionAt: undefined,
      verbose: true,
      outputFormat: 'stream-json',
      jsonSchema: undefined,
      permissionPromptToolName: undefined,
      allowedTools: undefined,
      thinkingConfig: undefined,
      maxTurns: undefined,
      maxBudgetUsd: undefined,
      taskBudget: undefined,
      systemPrompt: undefined,
      appendSystemPrompt: undefined,
      userSpecifiedModel: agentModel,
      fallbackModel: undefined,
      teleport: undefined,
      sdkUrl: undefined,
      replayUserMessages: false,
      includePartialMessages: false,
      forkSession: false,
      rewindFiles: undefined,
      enableAuthStatus: false,
      agent: undefined,
      workload: undefined,
      structuredIOOverride: structuredIO,
    },
  ).catch(async (error: Error) => {
    sessionFailed = error.message
    sessionReady = false
    log('error', 'headless_failed', { error: error.message })

    const execution = activeExecution
    activeExecution = null
    currentJob = null

    if (execution) {
      if (execution.timeoutHandle) clearTimeout(execution.timeoutHandle)
      try {
        await publishJobResult(execution.job, execution.startedAt, 'error', error.message)
        execution.resolve()
      } catch (publishError) {
        execution.reject(publishError as Error)
      }
    }
  })
}

async function enqueueJob(job: Job): Promise<void> {
  if (activeExecution) {
    throw new Error(`Cannot enqueue job ${job.id} while ${activeExecution.job.id} is active`)
  }

  const startedAt = new Date().toISOString()
  const timeoutMs = job.timeout_ms ?? DEFAULT_TIMEOUT_MS
  const prompt = buildPrompt(job)

  currentJob = {
    id: job.id,
    correlationId: job.correlation_id,
    startedAt,
  }
  jobCount++

  log('info', 'job_start', {
    skill: job.skill,
    job_number: jobCount,
  })

  await new Promise<void>((resolve, reject) => {
    const execution: ActiveExecution = {
      job,
      startedAt,
      timeoutMs,
      timedOut: false,
      resolve,
      reject,
      timeoutHandle: null,
    }

    execution.timeoutHandle = setTimeout(() => {
      if (activeExecution !== execution) return
      execution.timedOut = true
      log('warn', 'job_timeout_interrupt', { timeout_ms: timeoutMs })
      enqueueControlInterrupt()
    }, timeoutMs)

    activeExecution = execution

    inputStream.enqueue(JSON.stringify({
      type: 'user',
      session_id: '',
      message: {
        role: 'user',
        content: prompt,
      },
      parent_tool_use_id: null,
      uuid: randomUUID(),
      priority: 'later',
    }) + '\n')
  })
}

// ─── Status server ───

const statusServer = createServer((req, res) => {
  if (req.url === '/status' && req.method === 'GET') {
    const body = currentJob
      ? { state: 'busy', job_id: currentJob.id, since: currentJob.startedAt, jobs_completed: jobCount - 1 }
      : { state: 'idle', session_ready: sessionReady, jobs_completed: jobCount }
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify(body))

  } else if (req.url === '/health' && req.method === 'GET') {
    if (sessionFailed) {
      res.writeHead(503)
      res.end(sessionFailed)
      return
    }
    res.writeHead(sessionReady ? 200 : 503)
    res.end(sessionReady ? 'ok' : 'session not ready')

  } else if (req.url === '/memory-update' && req.method === 'POST') {
    let body = ''
    req.on('data', (chunk: Buffer) => { body += chunk.toString() })
    req.on('end', async () => {
      try {
        const update = JSON.parse(body) as { path: string }
        await producer.send({
          topic: `memory.updates.${AGENT_NAME}`,
          messages: [{ key: update.path, value: body }],
        })
        res.writeHead(200)
        res.end('queued')
      } catch (e) {
        res.writeHead(500)
        res.end((e as Error).message)
      }
    })

  } else {
    res.writeHead(404)
    res.end('not found')
  }
})

// ─── Main ───

export async function daemonMain(): Promise<void> {
  log('info', 'agent_boot', {
    brokers: KAFKA_BROKERS,
    project_dir: AGENT_PROJECT_DIR,
    state_dir: AGENT_STATE_DIR,
    model: agentModel,
    provider: agentProvider,
    agent_id: AGENT_ID,
  })

  statusServer.listen(STATUS_PORT, () => {
    log('info', 'status_server_ready', { port: STATUS_PORT })
  })

  const globalFiles = globalWatcher.snapshot()
  const sharedFiles = sharedWatcher.snapshot()
  log('info', 'memory_init', { global_files: globalFiles, shared_files: sharedFiles })

  await producer.connect()
  await startHeadlessSession()

  await consumer.connect()

  const topic = `agent.${AGENT_NAME}`
  await consumer.subscribe({ topic, fromBeginning: false })
  log('info', 'kafka_ready', { topic })

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      let job: Job
      try {
        job = JSON.parse(message.value!.toString()) as Job
      } catch (e) {
        log('error', 'job_parse_failed', { error: (e as Error).message })
        return
      }

      consumer.pause([{ topic }])
      try {
        await enqueueJob(job)
        await consumer.commitOffsets([{
          topic,
          partition,
          offset: (Number(message.offset) + 1).toString(),
        }])
        log('info', 'offset_committed', { partition, offset: message.offset })
      } finally {
        consumer.resume([{ topic }])
      }
    },
  })
}

// ─── Shutdown ───

async function shutdown(): Promise<void> {
  log('info', 'agent_shutdown', { jobs_completed: jobCount })
  try {
    inputStream.done()
    await consumer.disconnect()
    await producer.disconnect()
  } catch {}
  statusServer.close()
  process.exit(0)
}

process.on('SIGTERM', () => { void shutdown() })
process.on('SIGINT', () => { void shutdown() })
