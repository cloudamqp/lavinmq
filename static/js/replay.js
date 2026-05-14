import * as HTTP from './http.js'
import * as DOM from './dom.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const vhost = search.get('vhost')
const containerName = search.get('name')

const listSection = document.querySelector('#container-list')
const detailSection = document.querySelector('#container-detail')

function setSubtitle (text) {
  const el = document.getElementById('pagename-label')
  if (el) el.textContent = text
}

function fmtTimestamp (ms) {
  if (ms === null || ms === undefined) return ''
  try {
    return new Date(Number(ms)).toISOString().replace('T', ' ').replace(/\..+/, ' UTC')
  } catch (e) {
    return String(ms)
  }
}

function chooseVhost () {
  if (vhost) return Promise.resolve(vhost)
  return HTTP.request('GET', 'api/vhosts').then(vhosts => {
    if (vhosts && vhosts.length > 0) return vhosts[0].name
    throw new Error('No vhosts available')
  })
}

function loadList () {
  listSection.classList.remove('hide')
  detailSection.classList.add('hide')
  chooseVhost().then(currentVhost => {
    setSubtitle(`vhost: ${currentVhost}`)
    HTTP.request('GET', HTTP.url`api/replay/${currentVhost}`)
      .then(items => renderContainerList(currentVhost, items || []))
  })
}

function renderContainerList (currentVhost, items) {
  const tbody = document.querySelector('#containers-table tbody')
  tbody.innerHTML = ''
  if (items.length === 0) {
    const tr = document.createElement('tr')
    const td = document.createElement('td')
    td.colSpan = 4
    td.textContent = 'No replay queues in this vhost.'
    tr.appendChild(td)
    tbody.appendChild(tr)
    return
  }
  items.forEach(c => {
    const tr = document.createElement('tr')
    tr.style.cursor = 'pointer'
    tr.addEventListener('click', () => {
      window.location.hash = `vhost=${encodeURIComponent(c.vhost)}&name=${encodeURIComponent(c.name)}`
      window.location.reload()
    })
    tr.appendChild(td(c.vhost))
    tr.appendChild(td(c.name))
    tr.appendChild(td(String(c.messages), 'right'))
    tr.appendChild(td(c.durable ? '●' : '○', 'center'))
    tbody.appendChild(tr)
  })
}

function loadDetail () {
  listSection.classList.add('hide')
  detailSection.classList.remove('hide')
  setSubtitle(`${vhost} / ${containerName}`)
  document.querySelector('#container-name-heading').textContent = containerName
  HTTP.request('GET', HTTP.url`api/replay/${vhost}/${containerName}`)
    .then(items => renderMessages(items || []))
}

function renderMessages (items) {
  document.querySelector('#container-message-count').textContent = String(items.length)
  const tbody = document.querySelector('#messages-table tbody')
  tbody.innerHTML = ''
  if (items.length === 0) {
    const tr = document.createElement('tr')
    const cell = document.createElement('td')
    cell.colSpan = 10
    cell.textContent = 'Replay queue is empty.'
    tr.appendChild(cell)
    tbody.appendChild(tr)
    return
  }
  items.forEach(m => {
    const tr = document.createElement('tr')
    const idLink = document.createElement('a')
    idLink.href = HTTP.url`replay-message#vhost=${vhost}&name=${containerName}&id=${m.id}`
    idLink.textContent = (m.id || '').substring(0, 8)
    idLink.title = m.id || ''
    const idCell = document.createElement('td')
    idCell.appendChild(idLink)
    tr.appendChild(idCell)
    tr.appendChild(td(m.source || ''))
    tr.appendChild(td(m.exchange || ''))
    tr.appendChild(td(m.routing_key || ''))
    tr.appendChild(td(m.rule_id || ''))
    tr.appendChild(td(m.delivery_count !== null && m.delivery_count !== undefined ? String(m.delivery_count) : '', 'right'))
    tr.appendChild(td(String(m.payload_bytes), 'right'))
    tr.appendChild(td(m.content_type || ''))
    tr.appendChild(td(fmtTimestamp(m.timestamp)))
    const actions = document.createElement('td')
    actions.appendChild(actionButton('Release', () => releaseMessage(m.id)))
    actions.appendChild(actionButton('Delete', () => deleteMessage(m.id), 'btn-red'))
    tr.appendChild(actions)
    tbody.appendChild(tr)
  })
}

function td (text, align) {
  const cell = document.createElement('td')
  cell.textContent = text
  if (align) cell.classList.add(align)
  return cell
}

function actionButton (label, onClick, extraClass) {
  const btn = document.createElement('button')
  btn.type = 'button'
  btn.className = `btn btn-outlined ${extraClass || ''}`
  btn.textContent = label
  btn.style.marginRight = '4px'
  btn.addEventListener('click', e => { e.preventDefault(); onClick() })
  return btn
}

function releaseMessage (id) {
  if (!window.confirm('Release this message back to its source?')) return
  HTTP.request('POST', HTTP.url`api/replay/${vhost}/${containerName}/${id}/release`)
    .then(() => {
      DOM.toast('Message released')
      loadDetail()
    })
}

function deleteMessage (id) {
  if (!window.confirm('Permanently delete this replay message?')) return
  HTTP.request('DELETE', HTTP.url`api/replay/${vhost}/${containerName}/${id}`)
    .then(() => {
      DOM.toast('Message deleted')
      loadDetail()
    })
}

if (vhost && containerName) {
  loadDetail()
} else {
  loadList()
}
