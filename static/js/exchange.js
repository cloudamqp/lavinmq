import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Chart from './chart.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const exchange = search.get('name')
const vhost = search.get('vhost')
const urlEncodedExchange = encodeURIComponent(exchange)
const urlEncodedVhost = encodeURIComponent(vhost)
const chart = Chart.render('chart', 'msgs/s')

document.title = exchange + ' | LavinMQ'

const exchangeUrl = 'api/exchanges/' + urlEncodedVhost + '/' + urlEncodedExchange
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
    let argList = document.createElement('div')
    let args = Object.keys(item.arguments).forEach(key => {
      if (key == 'x-delayed-exchange' && item.arguments[key] === false) {
        return
      }
      let el = document.createElement('div');
      el.textContent = key + " = " + item.arguments[key];
      argList.appendChild(el)
    })
    document.getElementById("e-arguments").appendChild(argList)
    if (item.policy) {
      const policyLink = document.createElement('a')
      policyLink.href = 'policies#name=' + encodeURIComponent(item.policy) + '&vhost=' + encodeURIComponent(item.vhost)
      policyLink.textContent = item.policy
      document.getElementById("e-policy").appendChild(policyLink)
    }
  }).catch(HTTP.standardErrorHandler)
}
updateExchange()

const tableOptions = { url: exchangeUrl + '/bindings/source', keyColumns: ['properties_key'], interval: 5000 }
const bindingsTable = Table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
  if (!all) return
  if (item.source === '') {
    const td = Table.renderCell(tr, 0, '(Default exchange binding)')
    td.setAttribute('colspan', 5)
  } else {
    const btn = document.createElement('button')
    btn.classList.add('btn-secondary')
    btn.textContent = 'Unbind'
    btn.onclick = function () {
      const s = encodeURIComponent(item.source)
      const d = encodeURIComponent(item.destination)
      const p = encodeURIComponent(item.properties_key)
      const t = item.destination_type == "exchange" ? "e" : "q"
      const url = 'api/bindings/' + urlEncodedVhost + '/e/' + s + '/' + t + '/' + d + '/' + p
      HTTP.request('DELETE', url)
        .then(() => { tr.parentNode.removeChild(tr) })
        .catch(HTTP.standardErrorHandler)
    }

    const destinationLink = document.createElement('a')
    destinationLink.href = `${item.destination_type}#vhost=${urlEncodedVhost}&name=${encodeURIComponent(item.destination)}`
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
  const d = encodeURIComponent(data.get('destination').trim())
  const t = data.get('dest-type')
  const url = 'api/bindings/' + urlEncodedVhost + '/e/' + urlEncodedExchange + '/' + t + '/' + d
  const args = DOM.parseJSON(data.get('arguments'))
  const body = {
    routing_key: data.get('routing_key').trim(),
    arguments: args
  }
  HTTP.request('POST', url, { body })
    .then(() => {
      bindingsTable.fetchAndUpdate()
      evt.target.reset()
    }).catch(HTTP.alertErrorHandler)
})

document.querySelector('#publishMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = 'api/exchanges/' + urlEncodedVhost + '/' + urlEncodedExchange + '/publish'
  const properties = DOM.parseJSON(data.get('properties'))
  properties.delivery_mode = parseInt(data.get('delivery_mode'))
  properties.headers = DOM.parseJSON(data.get('headers'))
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
    .catch(HTTP.alertErrorHandler)
})

document.querySelector('#deleteExchange').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = 'api/exchanges/' + urlEncodedVhost + '/' + urlEncodedExchange
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = 'exchanges' })
      .catch(HTTP.standardErrorHandler)
  }
})

function updateAutocomplete(val) {
  const type = val === 'q' ? 'queues' : 'exchanges'
  Helpers.autoCompleteDatalist("exchange-dest-list", type, urlEncodedVhost)
}
updateAutocomplete('q')
document.getElementById("dest-type").onchange = (e) => updateAutocomplete(e.target.value)

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON('publishMessage', 'properties', e)
}
