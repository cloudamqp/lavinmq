import * as HTTP from './http.js'
import * as Table from './table.js'
import * as Users from './users.js'
import * as DOM from './dom.js'

const vhost = new URLSearchParams(window.location.search).get('name')
const urlEncodedVhost = encodeURIComponent(vhost)
document.title = vhost + ' | LavinMQ'
document.querySelector('#vhost2').textContent = vhost

const vhostUrl = '/api/vhosts/' + urlEncodedVhost
HTTP.request('GET', vhostUrl).then(item => {
  document.getElementById('ready').textContent = item.messages_ready.toLocaleString()
  document.getElementById('unacked').textContent = item.messages_unacknowledged.toLocaleString()
  document.getElementById('total').textContent = item.messages.toLocaleString()
})

function fetchLimits() {
  HTTP.request('GET', '/api/vhost-limits/' + urlEncodedVhost).then(arr => {
    const limits = arr[0] || { value: {} }
    const maxConnections = limits.value["max-connections"] || ""
    document.getElementById('max-connections').textContent = maxConnections.toLocaleString()
    document.forms.setLimits["max-connections"].value = maxConnections
    const maxQueues = limits.value["max-queues"] || ""
    document.getElementById('max-queues').textContent = maxQueues.toLocaleString()
    document.forms.setLimits["max-queues"].value = maxQueues
  })
}
fetchLimits()

const permissionsUrl = '/api/vhosts/' + urlEncodedVhost + '/permissions'
const tableOptions = { url: permissionsUrl, keyColumns: ['user'] }
const permissionsTable = Table.renderTable('table', tableOptions, (tr, item, all) => {
  Table.renderCell(tr, 1, item.configure)
  Table.renderCell(tr, 2, item.write)
  Table.renderCell(tr, 3, item.read)
  if (all) {
    const btn = document.createElement('button')
    btn.classList.add('btn-secondary')
    btn.innerHTML = 'Clear'
    btn.onclick = function () {
      const url = '/api/permissions/' + urlEncodedVhost + '/' + encodeURIComponent(item.user)
      HTTP.request('DELETE', url)
        .then(() => { DOM.removeNodes(tr) })
        .catch(HTTP.standardErrorHandler)
    }
    const userLink = document.createElement('a')
    userLink.href = '/user?name=' + encodeURIComponent(item.user)
    userLink.textContent = item.user
    Table.renderCell(tr, 0, userLink)
    Table.renderCell(tr, 4, btn, 'right')
  }
})

function addUserOptions (users) {
  const select = document.forms.setPermission.elements.user
  while (select.options.length) select.remove(0)
  for (let i = 0; i < users.length; i++) {
    const opt = document.createElement('option')
    opt.text = users[i].name
    select.add(opt)
  }
}

Users.fetch(addUserOptions)

document.querySelector('#setPermission').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = '/api/permissions/' + urlEncodedVhost + '/' + encodeURIComponent(data.get('user'))
  const body = {
    configure: data.get('configure'),
    write: data.get('write'),
    read: data.get('read')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      permissionsTable.fetchAndUpdate()
      evt.target.reset()
    }).catch(HTTP.standardErrorHandler)
})

document.forms.setLimits.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const maxConnectionsUrl = '/api/vhost-limits/' + urlEncodedVhost + '/max-connections'
  const maxConnectionsBody = { value: Number(this["max-connections"].value || -1) }
  const maxQueuesUrl = '/api/vhost-limits/' + urlEncodedVhost + '/max-queues'
  const maxQueuesBody = { value: Number(this["max-queues"].value || -1) }
  Promise.all([
    HTTP.request('PUT', maxConnectionsUrl, { body: maxConnectionsBody }),
    HTTP.request('PUT', maxQueuesUrl, { body: maxQueuesBody })
  ]).
    then(fetchLimits).
    catch(HTTP.standardErrorHandler)
})

document.querySelector('#deleteVhost').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = '/api/vhosts/' + urlEncodedVhost
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = '/vhosts' })
      .catch(HTTP.standardErrorHandler)
  }
})
document.querySelector('#resetVhost').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = '/api/vhosts/' + urlEncodedVhost + '/purge_and_close_consumers'
  if (window.confirm('This will purge all queues and close the consumers on this vhost\nAre you sure?')) {
    HTTP.request('POST', url)
      .then(() => { window.location = 'vhost?name=' + urlEncodedVhost })
      .catch(HTTP.standardErrorHandler)
  }
})
