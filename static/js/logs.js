/* global localStorage */

let shouldAutoScroll = true
const evtSource = new window.EventSource('api/livelog')
const livelog = document.getElementById('livelog')
const tbody = document.getElementById('livelog-body')
const btnToTop = document.getElementById('to-top')
const btnToBottom = document.getElementById('to-bottom')

evtSource.onmessage = (event) => {
  const timestamp = new Date(parseInt(event.lastEventId))
  const [severity, source, message] = JSON.parse(event.data)

  const tdTs = document.createElement('td')
  tdTs.textContent = timestamp.toLocaleString()
  const tdSev = document.createElement('td')
  tdSev.textContent = severity
  const tdSrc = document.createElement('td')
  tdSrc.textContent = source
  const preMsg = document.createElement('pre')
  preMsg.textContent = message
  const tdMsg = document.createElement('td')
  tdMsg.appendChild(preMsg)

  const tr = document.createElement('tr')
  tr.append(tdTs, tdSev, tdSrc, tdMsg)
  const row = tbody.appendChild(tr)

  if (shouldAutoScroll) row.scrollIntoView()
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
  localStorage.setItem('logScrollMode', toBottom ? 'bottom' : 'top')
  btnToBottom.setAttribute('aria-pressed', String(toBottom))
  btnToTop.setAttribute('aria-pressed', String(!toBottom))
}

// Initialize from saved preference, default to newest
const savedMode = localStorage.getItem('logScrollMode')
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

livelog.addEventListener('beforeunload', () => livelog.close())
