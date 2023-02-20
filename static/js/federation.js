import * as HTTP from './http.js'
import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Vhosts from './vhosts.js'

const escapeHTML = DOM.escapeHTML
Vhosts.addVhostOptions('createUpstream')

let url = 'api/parameters/federation-upstream'
let linksUrl = 'api/federation-links'
const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  const urlEncodedVhost = encodeURIComponent(vhost)
  url += '/' + urlEncodedVhost
  linksUrl += '/' + urlEncodedVhost
}

const utOpts = { url, keyColumns: ['vhost', 'name'], interval: 5000 }
const upstreamsTable = Table.renderTable('upstreamTable', utOpts, (tr, item) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, decodeURI(item.value.uri.replace(/:([^:]+)@/, ':***@')))
  Table.renderCell(tr, 3, item.value['prefetch-count'])
  Table.renderCell(tr, 4, item.value['reconnect-delay'])
  Table.renderCell(tr, 5, item.value['ack-mode'])
  Table.renderCell(tr, 6, item.value.exchange)
  // Table.renderCell(tr, 7, item.value['max-hops'])
  Table.renderCell(tr, 7, item.value.expires)
  Table.renderCell(tr, 8, item.value['message-ttl'])
  Table.renderCell(tr, 9, item.value.queue)
  Table.renderCell(tr, 10, item.value['consumer-tag'])
  const btn = document.createElement('button')
  btn.classList.add('btn-danger')
  btn.textContent = 'Delete'
  btn.onclick = function () {
    const name = encodeURIComponent(item.name)
    const vhost = encodeURIComponent(item.vhost)
    const url = 'api/parameters/federation-upstream/' + vhost + '/' + name
    if (!window.confirm(`Delete federation upstream ${item.name} ?`)) return
    HTTP.request('DELETE', url)
      .then(() => {
        DOM.removeNodes(tr)
      }).catch(HTTP.standardErrorHandler)
  }
  Table.renderCell(tr, 11, btn, 'right')
})

const linksOpts = { url: linksUrl, keyColumns: ['vhost', 'name'], interval: 5000 }
Table.renderTable('linksTable', linksOpts, (tr, item) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, decodeURI(item.uri))
  Table.renderHtmlCell(tr, 3, escapeHTML(item.resource) + '<br><small>' + escapeHTML(item.type) + '</small>')
  Table.renderCell(tr, 4, item.timestamp)
})

document.querySelector('#createUpstream').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = encodeURIComponent(data.get('name').trim())
  const vhost = encodeURIComponent(data.get('vhost'))
  const url = 'api/parameters/federation-upstream/' + vhost + '/' + name
  const body = {
    value: {
      uri: data.get('uri'),
      'prefetch-count': parseInt(data.get('prefetch-count')),
      'reconnect-delay': parseInt(data.get('reconnect-delay')),
      'ack-mode': data.get('ack-mode'),
      exchange: data.get('exchange'),
      'max-hops': parseInt(data.get('max-hops')),
      expires: parseInt(data.get('expires')),
      'message-ttl': parseInt(data.get('message-ttl')),
      queue: data.get('queue'),
      'consumer-tag': data.get('consumer-tag')
    }
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      upstreamsTable.fetchAndUpdate()
      evt.target.reset()
    }).catch(HTTP.standardErrorHandler)
})
Helpers.autoCompleteDatalist('queue-datalist', 'queues')
Helpers.autoCompleteDatalist('exchange-datalist', 'exchanges')
