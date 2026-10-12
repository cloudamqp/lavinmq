const RATES = [5000, 10000, 30000, 60000]
const RATE_KEY = 'lmq.refreshInterval'
const PAUSED_KEY = 'lmq.refreshPaused'

const fns = new Set()
const inFlight = new Set()
const events = new EventTarget()
let timer = null
let lastTickAt = 0
let paused = window.sessionStorage.getItem(PAUSED_KEY) === 'true'
let rate = RATES.find(ms => ms === Number(window.localStorage.getItem(RATE_KEY))) ?? RATES[0]

function emit () {
  events.dispatchEvent(new Event('change'))
}

function run (fn) {
  if (inFlight.has(fn)) return
  inFlight.add(fn)
  Promise.resolve()
    .then(fn)
    .catch(console.error)
    .finally(() => inFlight.delete(fn))
}

function schedule (delay = rate) {
  window.clearTimeout(timer)
  timer = null
  if (!paused && !document.hidden && fns.size > 0) {
    timer = window.setTimeout(tick, delay)
  }
  events.dispatchEvent(new window.CustomEvent('schedule', { detail: timer === null ? null : { rate, delay } }))
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
  if (fns.size === 1) emit()
}

function isActive () {
  return fns.size > 0
}

function isPaused () {
  return paused
}

function pause () {
  if (paused) return
  paused = true
  window.sessionStorage.setItem(PAUSED_KEY, 'true')
  schedule()
  emit()
}

function resume () {
  if (!paused) return
  paused = false
  window.sessionStorage.removeItem(PAUSED_KEY)
  tick()
  emit()
}

function getRate () {
  return rate
}

function setRate (ms) {
  if (!RATES.includes(ms) || ms === rate) return
  rate = ms
  window.localStorage.setItem(RATE_KEY, String(ms))
  schedule()
  emit()
}

document.addEventListener('visibilitychange', () => {
  const remaining = lastTickAt + rate - Date.now()
  if (document.hidden || paused) schedule()
  else if (remaining > 0) schedule(remaining)
  else tick()
})

export { RATES, start, isActive, isPaused, pause, resume, getRate, setRate, events }
