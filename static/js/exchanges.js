import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'

Helpers.addVhostOptions('addExchange')

HTTP.request('GET', 'api/overview').then(function (response) {
  const exchangeTypes = response.exchange_types
  const select = document.forms.addExchange.elements.type
  exchangeTypes.forEach(type => {
    const opt = document.createElement('option')
    opt.text = type.name
    select.add(opt)
  })
})

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/exchanges'
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
}
const tableOptions = {
  url,
  keyColumns: ['vhost', 'name'],
  pagination: true,
  columnSelector: true,
  search: true
}
const exchangeTable = Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    if (item.name === '') {
      item.name = 'amq.default'
    }
    const features = document.createElement('span')
    features.className = 'features'
    if (item.durable) {
      const durable = document.createElement('span')
      durable.textContent = 'D'
      durable.title = 'Durable'
      features.appendChild(durable)
    }
    if (item.auto_delete) {
      const autoDelete = document.createElement('span')
      autoDelete.textContent = ' AD'
      autoDelete.title = 'Auto Delete'
      features.appendChild(autoDelete)
    }
    if (item.internal) {
      const internal = document.createElement('span')
      internal.textContent = ' I'
      internal.title = 'Internal'
      features.appendChild(internal)
    }
    if (item.arguments['x-delayed-exchange']) {
      const delayed = document.createElement('span')
      delayed.textContent = ' d'
      delayed.title = 'Delayed'
      features.appendChild(delayed)
    }
    const exchangeLink = document.createElement('a')
    exchangeLink.href = HTTP.url`exchange#vhost=${item.vhost}&name=${item.name}`
    exchangeLink.textContent = item.name
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 1, exchangeLink)
    Table.renderCell(tr, 2, item.type)
    Table.renderCell(tr, 3, features, 'center')
  }
  let policyLink = ''
  if (item.policy) {
    policyLink = document.createElement('a')
    policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
    policyLink.textContent = item.policy
  }
  Table.renderCell(tr, 4, policyLink, 'center')
  Table.renderCell(tr, 5, item.message_stats.publish_in_details.rate, 'center')
  Table.renderCell(tr, 6, item.message_stats.publish_out_details.rate, 'center')
})

document.querySelector('#addExchange').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = data.get('vhost')
  const exchange = data.get('name').trim()
  const url = HTTP.url`api/exchanges/${vhost}/${exchange}`
  const body = {
    durable: data.get('durable') === '1',
    auto_delete: data.get('auto_delete') === '1',
    internal: data.get('internal') === '1',
    delayed: data.get('delayed') === '1',
    type: data.get('type'),
    arguments: DOM.parseJSON(data.get('arguments'))
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      exchangeTable.reload()
      DOM.toast('Exchange ' + exchange + ' created')
      evt.target.reset()
    })
})

document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelperJSON('addExchange', 'arguments', e)
})
