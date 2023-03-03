import * as Auth from './auth.js'

const url = new URL(`${window.baseHref}/api/livelog`, window.location.origin)
url.username = Auth.getUsername()
url.password = Auth.getPassword()

let shouldAutoScroll = true
const evtSource = new EventSource(url)
const livelog = document.getElementById('livelog')
const tbody = document.getElementById("livelog-body")

evtSource.onmessage = (event) => {
  const timestamp = new Date(parseInt(event.lastEventId))
  const [ severity, source, message ] = JSON.parse(event.data)

  const tdTs = document.createElement("td")
  tdTs.textContent = timestamp.toLocaleString()
  const tdSev = document.createElement("td")
  tdSev.textContent = severity
  const tdSrc = document.createElement("td")
  tdSrc.textContent = source
  const preMsg = document.createElement("pre")
  preMsg.textContent = message
  const tdMsg = document.createElement("td")
  tdMsg.appendChild(preMsg)

  const tr = document.createElement("tr")
  tr.append(tdTs, tdSev, tdSrc, tdMsg)
  const row = tbody.appendChild(tr)

  if (shouldAutoScroll) row.scrollIntoView()
}

evtSource.onerror = (err) => {
  console.error("EventSource error", err)
  //document.getElementById("table-error").textContent = `Lost connection to server`
  //document.getElementById("table-error").style.display = "block"
  //document.getElementById("table-error").style.position = "fixed"
}

let lastScrollTop = livelog.pageYOffset || livelog.scrollTop
livelog.addEventListener('scroll', event => {
  const {scrollHeight, scrollTop, clientHeight} = event.target
  const st = livelog.pageYOffset || livelog.scrollTop
  if (st > lastScrollTop && shouldAutoScroll === false) {
    shouldAutoScroll = (Math.abs(scrollHeight - clientHeight - scrollTop) < 3)
  } else if (st < lastScrollTop) {
    shouldAutoScroll = false
  }
  lastScrollTop = st <= 0 ? 0 : st
})
