import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'

interface ExchangeType {
  name: string
  human?: string
}

interface OverviewResponse {
  exchange_types: (ExchangeType | string)[]
}

interface MessageStats {
  publish_in_details: { rate: number }
  publish_out_details: { rate: number }
}

interface ExchangeItem {
  name: string
  vhost: string
  type: string
  durable: boolean
  auto_delete: boolean
  internal: boolean
  policy?: string
  arguments: Record<string, unknown>
  message_stats: MessageStats
}

Helpers.addVhostOptions('addExchange')

HTTP.request<OverviewResponse>('GET', 'api/overview').then(function (response) {
  if (!response) return
  const exchangeTypes = response.exchange_types
  const form = document.forms.namedItem('addExchange') as HTMLFormElement | null
  const select = form?.elements.namedItem('type') as HTMLSelectElement | null
  if (!select) return
  exchangeTypes.forEach((item) => {
    const name = item && typeof item === 'object' ? item.name : item
    const human = item && typeof item === 'object' && 'human' in item ? item.human : undefined
    const opt = document.createElement('option')
    opt.text = human || name
    opt.value = name
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
  search: true,
}
const exchangeTable = Table.renderTable<ExchangeItem>('table', tableOptions, function (tr, item, all) {
  if (all) {
    let displayName = item.name
    if (displayName === '') {
      displayName = 'amq.default'
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
    exchangeLink.href = HTTP.url`exchange#vhost=${item.vhost}&name=${displayName}`
    exchangeLink.textContent = displayName
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 1, exchangeLink)
    Table.renderCell(tr, 2, item.type)
    Table.renderCell(tr, 3, features, 'center')
  }
  let policyLink: HTMLAnchorElement | string = ''
  if (item.policy) {
    policyLink = document.createElement('a')
    policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
    policyLink.textContent = item.policy
  }
  Table.renderCell(tr, 4, policyLink, 'center')
  Table.renderCell(tr, 5, item.message_stats.publish_in_details.rate, 'center')
  Table.renderCell(tr, 6, item.message_stats.publish_out_details.rate, 'center')
})

const addForm = document.querySelector('#addExchange')
if (addForm) {
  addForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const data = new FormData(form)
    const formVhost = data.get('vhost') as string
    const exchange = (data.get('name') as string).trim()
    const createUrl = HTTP.url`api/exchanges/${formVhost}/${exchange}`
    const body = {
      durable: data.get('durable') === '1',
      auto_delete: data.get('auto_delete') === '1',
      internal: data.get('internal') === '1',
      delayed: data.get('delayed') === '1',
      type: data.get('type'),
      arguments: DOM.parseJSON(data.get('arguments') as string),
    }
    HTTP.request('PUT', createUrl, { body }).then(() => {
      exchangeTable.reload()
      DOM.toast('Exchange ' + exchange + ' created')
      form.reset()
    })
  })
}

const dataTags = document.querySelector('#dataTags')
if (dataTags) {
  dataTags.addEventListener('click', (e) => {
    Helpers.argumentHelperJSON('addExchange', 'arguments', e)
  })
}
