import * as Poller from './poller.js'

const control = document.getElementById('refresh-control')
const toggle = document.getElementById('refresh-toggle')
const rateSelect = document.getElementById('refresh-rate')
const ring = control.querySelector('.refresh-ring')
const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)')
let sweepAnimation = null

for (const ms of Poller.RATES) {
  rateSelect.add(new window.Option(`${ms / 1000}s`, ms))
}

function render () {
  const paused = Poller.isPaused()
  control.hidden = !Poller.isActive()
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
render()
