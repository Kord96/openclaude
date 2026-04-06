import { daemonMain } from '../daemon/main.js'

daemonMain().catch((e: Error) => {
  console.error(JSON.stringify({
    level: 'error',
    event: 'agent_fatal',
    error: e.message,
    timestamp: new Date().toISOString(),
  }))
  process.exit(1)
})
