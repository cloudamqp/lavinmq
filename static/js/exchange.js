import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Chart from './chart.js'
import { UrlDataSource } from './datasource.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const exchange = search.get('name')
const vhost = search.get('vhost')
const chart = Chart.render('chart', 'msgs/s')

document.title = exchange + ' | LavinMQ'

const exchangeUrl = HTTP.url`api/exchanges/${vhost}/${exchange}`
function updateExchange () {
  HTTP.request('GET', exchangeUrl).then(item => {
    Chart.update(chart, item.message_stats)
    let features = ''
    features += item.durable ? ' D' : ''
    features += item.auto_delete ? ' AD' : ''
    features += item.internal ? ' I' : ''
    features += item.arguments['x-delayed-exchange'] ? ' d' : ''
    document.getElementById('e-features').textContent = features
    document.getElementById('e-type').textContent = item.type
    document.querySelector('#pagename-label').textContent = exchange + ' in virtual host ' + item.vhost
    const argList = document.createElement('div')
    Object.keys(item.arguments).forEach(key => {
      if (key === 'x-delayed-exchange' && item.arguments[key] === false) {
        return
      }
      const el = document.createElement('div')
      el.textContent = key + ' = ' + item.arguments[key]
      if (item.effective_arguments.includes(key)) {
        el.classList.add('active-argument')
        el.title = 'Active argument'
      } else {
        el.classList.add('inactive-argument')
        el.title = 'Inactive argument'
      }
      argList.appendChild(el)
    })
    document.getElementById('e-arguments').appendChild(argList)
    if (item.policy) {
      const policyLink = document.createElement('a')
      policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
      policyLink.textContent = item.policy
      document.getElementById('e-policy').appendChild(policyLink)
    }
  })
}
updateExchange()

const tableOptions = {
  dataSource: new UrlDataSource(exchangeUrl + '/bindings/source', { useQueryState: false }),
  pagination: true,
  keyColumns: ['destination', 'properties_key'],
  search: true,
  countId: 'bindings-table-count'
}
const bindingsTable = Table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
  if (!all) return
  if (item.source === '') {
    const td = Table.renderCell(tr, 0, '(Default exchange binding)')
    td.setAttribute('colspan', 5)
  } else {
    const btn = DOM.button.delete({
      text: 'Unbind',
      click: function () {
        const type = item.destination_type === 'exchange' ? 'e' : 'q'
        const url = HTTP.url`api/bindings/${vhost}/e/${item.source}/${type}/${item.destination}/${item.properties_key}`
        HTTP.request('DELETE', url).then(() => { tr.parentNode.removeChild(tr) })
      }
    })

    const destinationLink = document.createElement('a')
    destinationLink.href = HTTP.url`${item.destination_type}#vhost=${vhost}&name=${item.destination}`
    destinationLink.textContent = item.destination
    const argsPre = document.createElement('pre')
    argsPre.textContent = JSON.stringify(item.arguments || {})
    Table.renderCell(tr, 0, item.destination_type)
    Table.renderCell(tr, 1, destinationLink, 'left')
    Table.renderCell(tr, 2, item.routing_key, 'left')
    Table.renderCell(tr, 3, argsPre, 'left')
    Table.renderCell(tr, 4, btn, 'right')
  }
})

document.querySelector('#addBinding').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const d = data.get('destination').trim()
  const t = data.get('dest-type')
  const url = HTTP.url`api/bindings/${vhost}/e/${exchange}/${t}/${d}`
  const args = DOM.parseJSON(data.get('arguments'))
  const body = {
    routing_key: data.get('routing_key').trim(),
    arguments: args
  }
  HTTP.request('POST', url, { body })
    .then(() => {
      bindingsTable.reload()
      evt.target.reset()
    })
})

document.querySelector('#publishMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = HTTP.url`api/exchanges/${vhost}/${exchange}/publish`
  const properties = DOM.parseJSON(data.get('properties'))
  properties.delivery_mode = parseInt(data.get('delivery_mode'))
  properties.headers = { ...properties.headers, ...DOM.parseJSON(data.get('headers')) }
  const body = {
    payload: data.get('payload'),
    payload_encoding: data.get('payload_encoding'),
    routing_key: data.get('routing_key').trim(),
    properties
  }
  HTTP.request('POST', url, { body })
    .then(res => {
      DOM.toast('Published message to ' + exchange + (res.routed ? '.' : ', but not routed.'))
    })
})

document.querySelector('#deleteExchange').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = HTTP.url`api/exchanges/${vhost}/${exchange}`
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = 'exchanges' })
  }
})

function updateAutocomplete (val) {
  const type = val === 'q' ? 'queues' : 'exchanges'
  Helpers.autoCompleteDatalist('exchange-dest-list', type, vhost)
}
updateAutocomplete('q')
document.getElementById('dest-type').onchange = (e) => updateAutocomplete(e.target.value)

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON('publishMessage', 'properties', e)
}
