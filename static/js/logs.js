const evtSource = new window.EventSource('api/livelog')
const livelog = document.getElementById('livelog')
const tbody = document.getElementById('livelog-body')
const filterInput = document.getElementById('log-filter')
const table = document.getElementById('table')
const loading = document.getElementById('log-loading')
const bootTime = performance.now()

const allLogs = []
const pendingLogs = []

const MAX_ROWS = 2000
const INITIAL_ROWS = 1000
const ROWS_PER_FLUSH = 200
const BOOT_MIN_LOGS = INITIAL_ROWS
const BOOT_MAX = 10000
const FLUSH_MS = 1000

let shouldAutoScroll = true
let currentRegex = null
let booting = true
let flushTimer = null

// SSE
evtSource.onmessage = (event) => {
  const timestamp = new Date(parseInt(event.lastEventId))
  const [severity, source, message] = JSON.parse(event.data)
  const log = { timestamp, severity, source, message }
  pendingLogs.push(log)
  if (booting) {
    table.style.visibility = 'hidden'
    loading.hidden = false
  }
  scheduleFlush()
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

// Build Table
function buildRow (log) {
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

// Paint table with Document Fragment. Batches depend on booting or streaming.
function paintIncoming () {
  flushTimer = null
  const now = performance.now()

  // BOOTING PHASE: wait for a big chunk to render
  if (booting) {
    const minLogs = pendingLogs.length >= BOOT_MIN_LOGS
    const minWait = (now - bootTime) >= BOOT_MAX

    if (!minLogs && !minWait) {
      scheduleFlush()
      return
    }

    const bootTake = Math.min(pendingLogs.length, INITIAL_ROWS)
    const bootBatch = pendingLogs.splice(0, bootTake)

    // Build off-DOM
    const frag = document.createDocumentFragment()
    for (const log of bootBatch) {
      allLogs.push(log)
      if (allLogs.length > MAX_ROWS) allLogs.shift()
      if (matches(log)) frag.appendChild(buildRow(log))
    }

    tbody.textContent = ''
    if (frag.childNodes.length) tbody.appendChild(frag)

    while (tbody.rows.length > MAX_ROWS) tbody.deleteRow(0)

    table.style.visibility = 'visible'
    loading.hidden = true
    booting = false

    livelog.scrollTop = livelog.scrollHeight

    if (pendingLogs.length) scheduleFlush()
    return
  }

  // STREAM PHASE: small batches regularly
  if (pendingLogs.length === 0) return

  const take = Math.min(pendingLogs.length, ROWS_PER_FLUSH)
  const batch = pendingLogs.splice(0, take)

  const frag = document.createDocumentFragment()
  for (const log of batch) {
    allLogs.push(log)
    if (allLogs.length > MAX_ROWS) allLogs.shift()
    if (matches(log)) frag.appendChild(buildRow(log))
  }

  if (frag.childNodes.length) tbody.appendChild(frag)

  while (tbody.rows.length > MAX_ROWS) tbody.deleteRow(0)

  livelog.scrollTop = livelog.scrollHeight

  if (pendingLogs.length) {
    scheduleFlush()
  }
}

// Helpers
function scheduleFlush () {
  if (flushTimer) return
  flushTimer = setTimeout(paintIncoming, FLUSH_MS)
}

function matches (log) {
  if (!currentRegex) return true
  const searchString = `${log.timestamp.toISOString()} ${log.severity} ${log.source}${log.message ?? ''}`
  return currentRegex.test(searchString)
}

// On filter change:
function rebuildFromAllLogs () {
  const frag = document.createDocumentFragment()
  for (const log of allLogs) {
    if (matches(log)) frag.appendChild(buildRow(log))
  }
  tbody.textContent = ''
  tbody.appendChild(frag)

  while (tbody.rows.length > MAX_ROWS) {
    tbody.deleteRow(0)
  }

  if (shouldAutoScroll) livelog.scrollTop = livelog.scrollHeight
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
  rebuildFromAllLogs()
})

// Fires when the native clear “×” is clicked (because type="search")
filterInput.addEventListener('search', () => {
  if (filterInput.value === '') {
    currentRegex = null
    rebuildFromAllLogs()
  }
})

filterInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault()
    rebuildFromAllLogs()
  } else if (e.key === 'Escape' && filterInput.value) {
    currentRegex = null
    filterInput.value = ''
    rebuildFromAllLogs()
  }
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
