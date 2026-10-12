import * as Poller from './poller.js'
import * as Reachability from './reachability.js'

const control = document.getElementById('refresh-control')
const toggle = document.getElementById('refresh-toggle')
const rateSelect = document.getElementById('refresh-rate')
const ring = control.querySelector('.refresh-ring')
const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)')
let sweepAnimation = null

for (const ms of Poller.RATES) {
  rateSelect.add(new window.Option(`${ms / 1000}s`, ms))
}

function describe (paused, { state, lastSuccessAt, lastError }) {
  const updated = lastSuccessAt ? lastSuccessAt.toLocaleTimeString() : 'never'
  if (state === 'stale') return `No new data since ${updated}, retrying.\nLast error: ${lastError}`
  if (state === 'reconnecting') return `Connection trouble, retrying. Last update ${updated}.\nLast error: ${lastError}`
  if (state === 'slow') return `Waiting for a slow response. Last update ${updated}`
  if (paused) return `Paused, last update ${updated}`
  return lastSuccessAt ? `Live, updated ${updated}` : 'Waiting for the first update'
}

function render () {
  const paused = Poller.isPaused()
  const reachability = Reachability.getState()
  control.hidden = !Poller.isActive()
  control.dataset.state = reachability.state
  control.title = describe(paused, reachability)
  toggle.setAttribute('aria-pressed', String(paused))
  toggle.setAttribute('aria-label', paused ? 'Resume auto-refresh' : 'Pause auto-refresh')
  rateSelect.value = String(Poller.getRate())
}

function sweep (event) {
  sweepAnimation?.cancel()
  sweepAnimation = null
  if (event.detail === null) return
  const { rate, delay } = event.detail
  sweepAnimation = ring.animate([{ '--sweep': '0%' }, { '--sweep': '100%' }], {
    duration: rate,
    easing: reducedMotion.matches ? 'steps(8, end)' : 'linear'
  })
  sweepAnimation.currentTime = rate - delay
}

toggle.addEventListener('click', () => {
  if (Poller.isPaused()) Poller.resume()
  else Poller.pause()
})
rateSelect.addEventListener('change', () => Poller.setRate(Number(rateSelect.value)))
Poller.events.addEventListener('schedule', sweep)
Poller.events.addEventListener('change', render)
Reachability.events.addEventListener('change', render)
render()
