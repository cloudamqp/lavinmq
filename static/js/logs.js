import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import { LogDataSource } from './datasource.js'

let shouldAutoScroll = true
let logItems =[]
let data = new LogDataSource(logItems)
let url = "api/livelog"
const evtSource = new window.EventSource('api/livelog')
const livelog = document.getElementById('livelog')
const tbody = document.getElementById('livelog-body')

evtSource.onmessage = (event) => {
  const timestamp = new Date(parseInt(event.lastEventId))
  const [severity, source, message] = JSON.parse(event.data)

  // const tdTs = document.createElement('td')
  // tdTs.textContent = timestamp.toLocaleString()
  // const tdSev = document.createElement('td')
  // tdSev.textContent = severity
  // const tdSrc = document.createElement('td')
  // tdSrc.textContent = source
  // const preMsg = document.createElement('pre')
  // preMsg.textContent = message
  // const tdMsg = document.createElement('td')
  // tdMsg.appendChild(preMsg)

  // const tr = document.createElement('tr')
  // tr.append(tdTs, tdSev, tdSrc, tdMsg)
  // const row = tbody.appendChild(tr)

  // if (shouldAutoScroll) row.scrollIntoView()
  logItems.push([timestamp, severity, source, message])
}


const tableOptions = {
  dataSource: data,
  keyColumns: ['timestamp'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: true
}

Table.renderLiveTable('table', tableOptions, (tr, item, all) => {
  Table.renderCell(tr, 0, item.timestamp)
  Table.renderCell(tr, 1, item.severity, 'center')
  Table.renderCell(tr, 2, item.source, 'center')
  Table.renderCell(tr, 3, item.message, 'center')
})

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
