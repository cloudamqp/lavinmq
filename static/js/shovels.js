import * as HTTP from './http.js'
import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Vhosts from './vhosts.js'

const escapeHTML = DOM.escapeHTML
Vhosts.addVhostOptions('createShovel')

function renderState(item) {
  if (item.error) {
    const state = document.createElement('a')
    state.classList.add("arg-tooltip")
    state.appendChild(document.createTextNode(item.state))
    const tooltip = document.createElement('span')
    tooltip.classList.add("tooltiptext")
    tooltip.innerText = item.error
    state.appendChild(tooltip)
    return state
  } else {
    return item.state
  }
}

const tableOptions = { keyColumns: ['vhost', 'name'], columnSelector: true }
const shovelsTable = Table.renderTable('table', tableOptions, (tr, item, all) => {
  if (!all) {
    Table.renderCell(tr, 10, renderState(item))
    return
  }
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, decodeURI(item.value['src-uri'].replace(/:([^:]+)@/, ':***@')))
  let srcEndpoint = ''
  let srcEndpointType = ''
  if (item.value['src-queue']) {
    srcEndpoint = escapeHTML(item.value['src-queue'])
    srcEndpointType = 'queue'
  } else {
    srcEndpoint = escapeHTML(item.value['src-exchange'])
    srcEndpointType = 'exchange'
  }
  Table.renderHtmlCell(tr, 3, srcEndpoint + '<br><small>' + srcEndpointType + '</small>')
  Table.renderCell(tr, 4, item.value['src-prefetch-count'])
  Table.renderCell(tr, 5, decodeURI(item.value['dest-uri'].replace(/:([^:]+)@/, ':***@')))
  let destEndpoint = ''
  let destEndpointType = ''
  if (item.value['dest-queue']) {
    destEndpoint = escapeHTML(item.value['dest-queue'])
    destEndpointType = 'queue'
  } else if (item.value['dest-exchange']) {
    destEndpoint = escapeHTML(item.value['dest-exchange'])
    destEndpointType = 'exchange'
  } else {
    destEndpoint = 'http'
  }
  Table.renderHtmlCell(tr, 6, destEndpoint + '<br><small>' + destEndpointType + '</small>')
  Table.renderCell(tr, 7, item.value['reconnect-delay'])
  Table.renderCell(tr, 8, item.value['ack-mode'])
  Table.renderCell(tr, 9, item.value['src-delete-after'])
  Table.renderCell(tr, 10, renderState(item))
  const btn = document.createElement('button')
  btn.classList.add('btn-danger')
  btn.innerHTML = 'Delete'
  btn.onclick = function () {
    const name = encodeURIComponent(item.name)
    const vhost = encodeURIComponent(item.vhost)
    const url = '/api/parameters/shovel/' + vhost + '/' + name
    if (window.confirm('Are you sure? This shovel can not be restored after deletion.')) {
      HTTP.request('DELETE', url)
        .then(() => {
          DOM.removeNodes(tr)
        }).catch(HTTP.standardErrorHandler)
    }
  }
  Table.renderCell(tr, 11, btn, 'right')
})

const vhost = window.sessionStorage.getItem('vhost')
let url = '/api/parameters/shovel'
let statusUrl = '/api/shovels'
if (vhost && vhost !== '_all') {
  const urlEncodedVhost = encodeURIComponent(vhost)
  url += '/' + urlEncodedVhost
  statusUrl += '/' + urlEncodedVhost
}
function updateShovelsTable () {
  const p1 = HTTP.request('GET', url)
  const p2 = HTTP.request('GET', statusUrl)

  Promise.all([p1, p2]).then(values => {
    const items = values[0]
    const status = values[1]
    const shovels = items.map(item => {
      item.state = status.find(s => s.name === item.name && s.vhost === item.vhost).state
      item.error = status.find(s => s.name === item.name && s.vhost === item.vhost).error
      return item
    })
    shovelsTable.updateTable(shovels)
  })
}
updateShovelsTable()
setInterval(updateShovelsTable, 5000)

document.querySelector('[name=src-type]').addEventListener('change', function () {
  document.getElementById('srcRoutingKey').classList.toggle('hide', this.value === 'queue')
})

document.querySelector('[name=dest-uri]').addEventListener('change', function () {
  let is_http = this.value.startsWith("http")
  document.querySelectorAll('.amqp-dest-field').forEach(e => {
    e.classList.toggle('hide', is_http)
  })
})

document.querySelector('#createShovel').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = encodeURIComponent(data.get('name').trim())
  const vhost = encodeURIComponent(data.get('vhost'))
  const url = '/api/parameters/shovel/' + vhost + '/' + name
  const body = {
    value: {
      'src-uri': data.get('src-uri'),
      'dest-uri': data.get('dest-uri'),
      'src-prefetch-count': parseInt(data.get('src-prefetch-count')),
      'src-delete-after': data.get('src-delete-after'),
      'reconnect-delay': parseInt(data.get('reconnect-delay')),
      'ack-mode': data.get('ack-mode')
    }
  }
  if (data.get('src-type') === 'queue') {
    body.value['src-queue'] = data.get('src-endpoint')
  } else {
    body.value['src-exchange'] = data.get('src-endpoint')
    body.value['src-exchange-key'] = data.get('src-exchange-key')
  }
  if (data.get('dest-type') === 'queue') {
    body.value['dest-queue'] = data.get('dest-endpoint')
  } else {
    body.value['dest-exchange'] = data.get('dest-endpoint')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      updateShovelsTable()
      evt.target.reset()
      DOM.toast('Shovel ' + name + ' created')
    }).catch(HTTP.standardErrorHandler)
})

function updateAutocomplete(e, id) {
  const type = e === 'queue' ? 'queues' : 'exchanges'
  Helpers.autoCompleteDatalist(id, type)
}
updateAutocomplete('queue',"shovel-src-list")
updateAutocomplete('queue',"shovel-dest-list")
