import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Vhosts from './vhosts.js'
import * as Overview from './overview.js'

Vhosts.addVhostOptions('addExchange')

Overview.get('exchange_types').then(exchangeTypes => {
  const select = document.forms.addExchange.elements.type
  while (select.options.length) {
    select.remove(0)
  }
  for (let i = 0; i < exchangeTypes.length; i++) {
    const opt = document.createElement('option')
    opt.text = exchangeTypes[i].name
    select.add(opt)
  }
})

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/exchanges'
if (vhost && vhost !== '_all') {
  url += '/' + encodeURIComponent(vhost)
}
const tableOptions = {
  url,
  keyColumns: ['vhost', 'name'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: true
}
const exchangeTable = Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    if (item.name === '') {
      item.name = 'amq.default'
    }
    let features = ''
    features += item.durable ? ' D' : ''
    features += item.auto_delete ? ' AD' : ''
    features += item.internal ? ' I' : ''
    features += item.arguments['x-delayed-exchange'] ? ' d' : ''
    const exchangeLink = document.createElement('a')
    exchangeLink.href = `exchange#vhost=${encodeURIComponent(item.vhost)}&name=${encodeURIComponent(item.name)}`
    exchangeLink.textContent = item.name
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 1, exchangeLink)
    Table.renderCell(tr, 2, item.type)
    Table.renderCell(tr, 3, features, 'center')
  }
  let policyLink = ''
  if (item.policy) {
    policyLink = document.createElement('a')
    policyLink.href = 'policies#name=' + encodeURIComponent(item.policy) + '&vhost=' + encodeURIComponent(item.vhost)
    policyLink.textContent = item.policy
  }
  Table.renderCell(tr, 4, policyLink, 'center')
})

document.querySelector('#addExchange').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = encodeURIComponent(data.get('vhost'))
  const exchange = encodeURIComponent(data.get('name').trim())
  const url = 'api/exchanges/' + vhost + '/' + exchange
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
      exchangeTable.fetchAndUpdate()
      DOM.toast('Exchange ' + exchange + ' created')
      evt.target.reset()
    }).catch(HTTP.standardErrorHandler)
})

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON('addExchange', 'arguments', e)
}
