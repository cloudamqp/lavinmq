import * as Poller from './poller.js'

const events = new EventTarget()
let state = 'live'
let lastSuccessAt = null
let failingSince = null
let lastError = null
let successWaiting = false

function update (next) {
  state = next
  events.dispatchEvent(new Event('change'))
}

function recordSuccess () {
  if (Poller.isStalled()) {
    successWaiting = true
    return
  }
  successWaiting = false
  lastSuccessAt = new Date()
  failingSince = null
  lastError = null
  update('live')
}

function recordFailure (reason) {
  successWaiting = false
  const now = Date.now()
  failingSince ??= now
  lastError = reason
  update(now - failingSince >= Poller.getRate() ? 'stale' : 'reconnecting')
}

function getState () {
  return { state, lastSuccessAt, lastError }
}

window.addEventListener('offline', () => recordFailure('Browser is offline'))

Poller.events.addEventListener('settled', () => {
  if (successWaiting && !Poller.isStalled()) recordSuccess()
})

Poller.events.addEventListener('stalled', () => {
  if (failingSince === null) update('slow')
  else recordFailure(lastError)
})

export { recordSuccess, recordFailure, getState, events }
