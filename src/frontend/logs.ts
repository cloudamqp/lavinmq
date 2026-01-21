interface WhoAmIResponse {
  tags: string[]
}

let shouldAutoScroll = true
const evtSource = new EventSource('api/livelog')
const livelog = document.getElementById('livelog')
const tbody = document.getElementById('livelog-body')
const btnToTop = document.getElementById('to-top')
const btnToBottom = document.getElementById('to-bottom')

evtSource.onmessage = (event: MessageEvent): void => {
  const timestamp = new Date(parseInt(event.lastEventId, 10))
  const [severity, source, message] = JSON.parse(event.data) as [string, string, string]

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
  const row = tbody?.appendChild(tr)

  if (shouldAutoScroll && row) row.scrollIntoView()
}

evtSource.onerror = (): void => {
  fetch('api/whoami')
    .then((response) => response.json())
    .then((whoami: WhoAmIResponse) => {
      if (!whoami.tags.includes('administrator')) {
        forbidden()
      }
    })
}

function forbidden(): void {
  const tblError = document.getElementById('table-error')
  if (tblError) {
    tblError.textContent = 'Access denied, administator access required'
    tblError.style.display = 'block'
  }
}

// Scrolling
function setScrollMode(toBottom: boolean): void {
  shouldAutoScroll = toBottom
  localStorage.setItem('logScrollMode', toBottom ? 'bottom' : 'top')
  btnToBottom?.setAttribute('aria-pressed', String(toBottom))
  btnToTop?.setAttribute('aria-pressed', String(!toBottom))
}

// Initialize from saved preference, default to newest
const savedMode = localStorage.getItem('logScrollMode')
const initialMode = savedMode ? savedMode === 'bottom' : true
setScrollMode(initialMode)

btnToTop?.addEventListener('click', () => {
  setScrollMode(false)
  if (livelog) livelog.scrollTop = 0
})

btnToBottom?.addEventListener('click', () => {
  setScrollMode(true)
  if (livelog) livelog.scrollTop = livelog.scrollHeight
})

let lastScrollTop = (livelog as HTMLElement | null)?.scrollTop ?? 0
livelog?.addEventListener('scroll', (event) => {
  const target = event.target as HTMLElement
  const { scrollHeight, scrollTop, clientHeight } = target
  const st = livelog.scrollTop
  if (st > lastScrollTop && shouldAutoScroll === false) {
    shouldAutoScroll = Math.abs(scrollHeight - clientHeight - scrollTop) < 3
  } else if (st < lastScrollTop) {
    shouldAutoScroll = false
  }
  lastScrollTop = st <= 0 ? 0 : st
})

window.addEventListener('beforeunload', () => evtSource.close())
