const evtSource = new window.EventSource('api/livelog')
const livelog = document.getElementById('livelog')
const tbody = document.getElementById('livelog-body')
const filterInput = document.getElementById('log-filter')
const btnToTop = document.getElementById('to-top')
const btnToBottom = document.getElementById('to-bottom')
const buffer = []
let shouldAutoScroll = true
let currentRegex = null

// Helpers
const joinForSearch = (log) => {
  return `${log.timestamp.toISOString()} ${log.severity} ${log.source}${log.message ?? ''}`
}

const matches = (log) => {
  return !currentRegex || currentRegex.test(joinForSearch(log))
}

// Build Table
const buildRow = (log) => {
  const tdTs = document.createElement('td')
  tdTs.textContent = log.timestamp.toLocaleString()
  const tdSev = document.createElement('td')
  tdSev.textContent = log.severity
  const tdSrc = document.createElement('td')
  tdSrc.textContent = log.source
  const preMsg = document.createElement('pre')
  preMsg.textContent = log.message
  const tdMsg = document.createElement('td')
  tdMsg.appendChild(preMsg)

  const tr = document.createElement('tr')
  tr.append(tdTs, tdSev, tdSrc, tdMsg)
  return tr
}

// DocumentFragment = build off-DOM, append once → single layout/paint (fast).
const rebuildFromBuffer = () => {
  const frag = document.createDocumentFragment()
  for (const log of buffer) {
    if (matches(log)) frag.appendChild(buildRow(log))
  }
  tbody.textContent = ''
  tbody.appendChild(frag)
  if (shouldAutoScroll) livelog.scrollTop = livelog.scrollHeight
}

// SSE: append-only
evtSource.onmessage = (event) => {
  const timestamp = new Date(parseInt(event.lastEventId))
  const [severity, source, message] = JSON.parse(event.data)
  const log = { timestamp, severity, source, message }
  buffer.push(log)

  if (matches(log)) {
    tbody.appendChild(buildRow(log))
    if (shouldAutoScroll) livelog.scrollTop = livelog.scrollHeight
  }
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

// Live typing: compile & rebuild
filterInput.addEventListener('input', () => {
  const q = filterInput.value.trim()
  try {
    currentRegex = q ? new RegExp(q, 'i') : null
    filterInput.classList.toggle('invalid', false)
  } catch {
    currentRegex = null
    filterInput.classList.toggle('invalid', true)
  }
  rebuildFromBuffer()
})

// Fires when the native clear “×” is clicked (because type="search")
filterInput.addEventListener('search', () => {
  if (filterInput.value === '') {
    currentRegex = null
    rebuildFromBuffer()
  }
})

filterInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault()
    rebuildFromBuffer()
  } else if (e.key === 'Escape' && filterInput.value) {
    currentRegex = null
    filterInput.value = ''
    rebuildFromBuffer()
  }
})

// Scrolling
btnToTop?.addEventListener('click', () => {
  livelog.scrollTop = 0
  shouldAutoScroll = false
})

btnToBottom?.addEventListener('click', () => {
  livelog.scrollTop = livelog.scrollHeight
  shouldAutoScroll = true
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
