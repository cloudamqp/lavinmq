const INTERVAL_MS = 5000

const fns = new Set()
const inFlight = new Set()
let timer = null
let lastTickAt = 0

function run (fn) {
  if (inFlight.has(fn)) return
  inFlight.add(fn)
  Promise.resolve()
    .then(fn)
    .catch(console.error)
    .finally(() => inFlight.delete(fn))
}

function schedule (delay = INTERVAL_MS) {
  window.clearTimeout(timer)
  timer = null
  if (document.hidden || fns.size === 0) return
  timer = window.setTimeout(tick, delay)
}

function tick () {
  lastTickAt = Date.now()
  fns.forEach(run)
  schedule()
}

function start (fn) {
  fns.add(fn)
  run(fn)
  if (timer === null) {
    lastTickAt = Date.now()
    schedule()
  }
}

document.addEventListener('visibilitychange', () => {
  const remaining = lastTickAt + INTERVAL_MS - Date.now()
  if (document.hidden) schedule()
  else if (remaining > 0) schedule(remaining)
  else tick()
})

export { start }
