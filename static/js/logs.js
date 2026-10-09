/* global localStorage */

let shouldAutoScroll = true
const evtSource = new window.EventSource('api/livelog')
const livelog = document.getElementById('livelog')
const tbody = document.getElementById('livelog-body')
const btnToTop = document.getElementById('to-top')
const btnToBottom = document.getElementById('to-bottom')
const MAX_LINES = 10000
const pending = []
let paintScheduled = false

evtSource.onmessage = (event) => {
  pending.push(event)
  if (pending.length > MAX_LINES * 1.1) pending.splice(0, pending.length - MAX_LINES)
  schedulePaint()
}

function schedulePaint () {
  if (paintScheduled) return
  paintScheduled = true
  window.requestAnimationFrame(paint)
}

function paint () {
  paintScheduled = false
  const rows = document.createDocumentFragment()
  for (const event of pending.splice(0).slice(-MAX_LINES)) {
    rows.appendChild(buildRow(event))
  }
  tbody.appendChild(rows)
  trimRows()
  if (shouldAutoScroll) livelog.scrollTop = livelog.scrollHeight
  lastScrollTop = livelog.scrollTop
}

function trimRows () {
  const excess = tbody.rows.length - MAX_LINES
  if (excess <= 0) return
  const heightBefore = livelog.scrollHeight
  const range = document.createRange()
  range.setStartBefore(tbody.rows[0])
  range.setEndAfter(tbody.rows[excess - 1])
  range.deleteContents()
  if (!shouldAutoScroll) livelog.scrollTop -= heightBefore - livelog.scrollHeight
}

function buildRow (event) {
  const timestamp = new Date(parseInt(event.lastEventId))
  const [severity, source, message] = JSON.parse(event.data)

  const tdTs = document.createElement('td')
  tdTs.textContent = timestamp.toLocaleString()
  const tdSev = document.createElement('td')
  tdSev.textContent = severity
  const tdSrc = document.createElement('td')
  tdSrc.title = source
  tdSrc.textContent = source
  const preMsg = document.createElement('pre')
  preMsg.textContent = message
  const tdMsg = document.createElement('td')
  tdMsg.appendChild(preMsg)

  const tr = document.createElement('tr')
  tr.append(tdTs, tdSev, tdSrc, tdMsg)
  return tr
}

evtSource.onerror = () => {
  window.fetch('api/whoami')
    .then(response => response.json())
    .then(whoami => {
      if (!whoami.tags.includes('administrator')) {
        forbidden()
      }
    })
}

function forbidden () {
  const tblError = document.getElementById('table-error')
  tblError.textContent = 'Access denied, administator access required'
  tblError.style.display = 'block'
}

// Scrolling
function setScrollMode (toBottom) {
  shouldAutoScroll = toBottom
  localStorage.setItem('lmq.logScrollMode', toBottom ? 'bottom' : 'top')
  btnToBottom.setAttribute('aria-pressed', String(toBottom))
  btnToTop.setAttribute('aria-pressed', String(!toBottom))
}

// Initialize from saved preference, default to newest
const savedMode = localStorage.getItem('lmq.logScrollMode')
const initialMode = savedMode ? savedMode === 'bottom' : true
setScrollMode(initialMode)

btnToTop.addEventListener('click', () => {
  setScrollMode(false)
  livelog.scrollTop = 0
})

btnToBottom.addEventListener('click', () => {
  setScrollMode(true)
  livelog.scrollTop = livelog.scrollHeight
})

let lastScrollTop = livelog.pageYOffset || livelog.scrollTop
livelog.addEventListener('scroll', event => {
  const { scrollHeight, scrollTop, clientHeight } = event.target
  const st = livelog.pageYOffset || livelog.scrollTop
  if (st > lastScrollTop && shouldAutoScroll === false) {
    shouldAutoScroll = (Math.abs(scrollHeight - clientHeight - scrollTop) < 3)
  } else if (st < lastScrollTop) {
    shouldAutoScroll = false
  }
  lastScrollTop = st <= 0 ? 0 : st
})

window.addEventListener('beforeunload', () => evtSource.close())
