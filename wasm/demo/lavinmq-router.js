/**
 * LavinMQ Router — embeddable JS module
 *
 * Usage:
 *   import { loadRouter } from './lavinmq-router.js';
 *
 *   const router = await loadRouter('./lavinmq-router.wasm');
 *
 *   const { matchedQueues } = await router.route({
 *     exchangeType: 'topic',
 *     routingKey:   'logs.error.critical',
 *     bindings:     [{ queue: 'all', routingKey: 'logs.#' }],
 *   });
 *
 *   const { matchedPolicy, effectiveArgs } = await router.matchPolicy({
 *     policies:     [{ name: 'ttl', pattern: '^ha[.]', applyTo: 'all', priority: 5, definition: { 'message-ttl': 60000 } }],
 *     resourceName: 'ha.orders',
 *     resourceType: 'queue',
 *   });
 */

// ── WASI shim ─────────────────────────────────────────────────────────────────
// Minimal WASI preview1 sufficient for Crystal's stdio loop.
// No native deps, no CDN — runs entirely in the browser.

class WasiMemory {
  constructor (memory) { this.memory = memory }
  getUint32 (ptr) { return new DataView(this.memory.buffer).getUint32(ptr, true) }
  setUint32 (ptr, v) { new DataView(this.memory.buffer).setUint32(ptr, v, true) }
}

function buildWasi (stdinLines, stdoutCallback) {
  let mem
  const stdinBuf = new TextEncoder().encode(stdinLines.join('\n') + '\n')
  let stdinPos = 0
  let stdoutAcc = ''

  const wasi = {
    setMemory (m) { mem = new WasiMemory(m) },

    fd_read (fd, iovsPtr, iovsLen, nreadPtr) {
      if (fd !== 0) return 8
      let total = 0
      for (let i = 0; i < iovsLen; i++) {
        const bufPtr = mem.getUint32(iovsPtr + i * 8)
        const bufLen = mem.getUint32(iovsPtr + i * 8 + 4)
        const avail = Math.min(bufLen, stdinBuf.length - stdinPos)
        if (avail <= 0) break
        new Uint8Array(mem.memory.buffer).set(stdinBuf.subarray(stdinPos, stdinPos + avail), bufPtr)
        stdinPos += avail
        total += avail
      }
      mem.setUint32(nreadPtr, total)
      return 0
    },

    fd_write (fd, iovsPtr, iovsLen, nwrittenPtr) {
      let total = 0
      for (let i = 0; i < iovsLen; i++) {
        const bufPtr = mem.getUint32(iovsPtr + i * 8)
        const bufLen = mem.getUint32(iovsPtr + i * 8 + 4)
        const chunk = new TextDecoder().decode(new Uint8Array(mem.memory.buffer, bufPtr, bufLen))
        if (fd === 1) stdoutAcc += chunk
        total += bufLen
      }
      mem.setUint32(nwrittenPtr, total)
      if (fd === 1 && stdoutAcc.includes('\n')) {
        const lines = stdoutAcc.split('\n')
        stdoutAcc = lines.pop()
        lines.forEach(l => l && stdoutCallback(l))
      }
      return 0
    },

    fd_close () { return 0 },
    fd_seek () { return 0 },
    fd_fdstat_get (fd, statPtr) {
      const dv = new DataView(mem.memory.buffer)
      dv.setUint8(statPtr, fd < 3 ? 2 : 0)
      dv.setUint8(statPtr + 1, 0)
      dv.setUint16(statPtr + 2, 0, true)
      dv.setBigUint64(statPtr + 8, 0n, true)
      dv.setBigUint64(statPtr + 16, 0n, true)
      return 0
    },
    fd_fdstat_set_flags () { return 0 },
    fd_pread (fd, iovsPtr, iovsLen, _offset, nreadPtr) {
      if (fd !== 0) { mem.setUint32(nreadPtr, 0); return 8 }
      return wasi.fd_read(fd, iovsPtr, iovsLen, nreadPtr)
    },
    environ_sizes_get (countPtr, bufSizePtr) { mem.setUint32(countPtr, 0); mem.setUint32(bufSizePtr, 0); return 0 },
    environ_get () { return 0 },
    args_sizes_get (argc, argvBufSize) { mem.setUint32(argc, 0); mem.setUint32(argvBufSize, 0); return 0 },
    args_get () { return 0 },
    clock_time_get (_id, _precision, timePtr) {
      new DataView(mem.memory.buffer).setBigUint64(timePtr, BigInt(Date.now()) * 1_000_000n, true)
      return 0
    },
    proc_exit (code) { throw { wasiExit: code } },
    random_get (bufPtr, bufLen) { crypto.getRandomValues(new Uint8Array(mem.memory.buffer, bufPtr, bufLen)); return 0 },
    sched_yield () { return 0 },
    poll_oneoff () { return 0 },
    path_open () { return 8 },
    path_filestat_get () { return 8 }
  }

  const imports = {
    wasi_snapshot_preview1: Object.fromEntries(
      ['fd_read', 'fd_write', 'fd_close', 'fd_seek', 'fd_fdstat_get', 'fd_fdstat_set_flags',
        'fd_pread', 'environ_sizes_get', 'environ_get', 'args_sizes_get', 'args_get',
        'clock_time_get', 'proc_exit', 'random_get', 'sched_yield', 'poll_oneoff',
        'path_open', 'path_filestat_get']
        .map(k => [k, wasi[k].bind(wasi)])
    )
  }

  return { imports, setMemory: wasi.setMemory.bind(wasi) }
}

// ── Core call ─────────────────────────────────────────────────────────────────

async function callWasm (wasmModule, requests) {
  const results = []
  await new Promise((resolve, reject) => {
    const lines = requests.map(r => JSON.stringify(r))
    const wasi = buildWasi(lines, line => {
      try { results.push(JSON.parse(line)) } catch {}
    })
    WebAssembly.instantiate(wasmModule, wasi.imports).then(instance => {
      wasi.setMemory(instance.exports.memory)
      try { instance.exports._start() } catch (e) { if (e?.wasiExit == null) reject(e) }
      resolve()
    }).catch(reject)
  })
  return results
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Load the router from a .wasm URL and return a router object.
 *
 * @param {string} wasmUrl  URL of lavinmq-router.wasm (absolute or relative)
 * @returns {Promise<Router>}
 */
export async function loadRouter (wasmUrl = './lavinmq-router.wasm') {
  const buf = await fetch(wasmUrl).then(r => r.arrayBuffer())
  const wasmModule = await WebAssembly.compile(buf)

  return {
    /**
     * Determine which queues a message would be delivered to.
     *
     * @param {object} opts
     * @param {'topic'|'direct'|'fanout'|'headers'} opts.exchangeType
     * @param {string}  opts.routingKey
     * @param {Array<{queue:string, routingKey?:string, arguments?:object}>} opts.bindings
     * @param {object}  [opts.headers]   key/value headers (for headers exchange)
     * @returns {Promise<{matchedQueues: string[]}>}
     */
    async route ({ exchangeType, routingKey = '', bindings = [], headers = {}, exchangeArguments = {} }) {
      const req = {
        action: 'route',
        exchange_type: exchangeType,
        routing_key: routingKey,
        headers,
        exchange_arguments: exchangeArguments,
        bindings: bindings.map(b => ({
          queue: b.queue,
          routing_key: b.routingKey ?? b.routing_key ?? '',
          arguments: b.arguments ?? {}
        }))
      }
      const [res] = await callWasm(wasmModule, [req])
      if (res?.error) throw new Error(res.error)
      return { matchedQueues: res?.matched_queues ?? [] }
    },

    /**
     * Find the highest-priority policy matching a resource.
     *
     * @param {object} opts
     * @param {Array<{name:string, pattern:string, applyTo:string, priority:number, definition:object}>} opts.policies
     * @param {string} opts.resourceName
     * @param {'queue'|'exchange'} opts.resourceType
     * @returns {Promise<{matchedPolicy: object|null, effectiveArgs: object}>}
     */
    async matchPolicy ({ policies = [], resourceName, resourceType }) {
      const normalized = policies.map(p => ({
        name: p.name,
        pattern: p.pattern,
        apply_to: p.applyTo ?? p.apply_to,
        priority: p.priority,
        definition: p.definition ?? {}
      }))
      const req = { action: 'match_policy', policies: normalized, resource_name: resourceName, resource_type: resourceType }
      const [res] = await callWasm(wasmModule, [req])
      if (res?.error) throw new Error(res.error)
      return { matchedPolicy: res?.matched_policy ?? null, effectiveArgs: res?.effective_args ?? {} }
    },

    /**
     * Run multiple requests in a single WASM instantiation.
     * Useful for batching N policy checks in one shot.
     *
     * @param {object[]} requests  Raw action objects (same shape as above)
     * @returns {Promise<object[]>}
     */
    async batch (requests) {
      return callWasm(wasmModule, requests)
    }
  }
}
