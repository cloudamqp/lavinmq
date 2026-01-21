import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Chart from './chart.js'
import { UrlDataSource } from './datasource.js'

interface ExchangeResponse {
  type: string
  durable: boolean
  auto_delete: boolean
  internal: boolean
  vhost: string
  policy?: string
  arguments: Record<string, unknown>
  effective_arguments: string[]
  message_stats: Record<string, number>
}

interface BindingItem {
  source: string
  destination: string
  destination_type: string
  routing_key: string
  properties_key: string
  arguments: Record<string, unknown>
}

interface PublishResponse {
  routed: boolean
}

const search = new URLSearchParams(window.location.hash.substring(1))
const exchange = search.get('name') ?? ''
const vhost = search.get('vhost') ?? ''
const chart = Chart.render('chart', 'msgs/s')

document.title = exchange + ' | LavinMQ'

const exchangeUrl = HTTP.url`api/exchanges/${vhost}/${exchange}`
function updateExchange(all: boolean): void {
  HTTP.request<ExchangeResponse>('GET', exchangeUrl).then((item) => {
    if (!item) return
    Chart.update(chart, item.message_stats)
    if (all) {
      const features: string[] = []
      if (item.durable) features.push('Durable')
      if (item.auto_delete) features.push('Auto delete')
      if (item.internal) features.push('Internal')
      if (item.arguments['x-delayed-exchange']) features.push('Delayed')
      const featuresEl = document.getElementById('e-features')
      if (featuresEl) featuresEl.innerText = features.join(', ')
      const typeEl = document.getElementById('e-type')
      if (typeEl) typeEl.textContent = item.type
      const pagenameLabel = document.querySelector('#pagename-label')
      if (pagenameLabel) pagenameLabel.textContent = exchange + ' in virtual host ' + item.vhost
      const argList = document.createElement('div')
      Object.keys(item.arguments).forEach((key) => {
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
      const argsEl = document.getElementById('e-arguments')
      if (argsEl) argsEl.appendChild(argList)
      if (item.policy) {
        const policyLink = document.createElement('a')
        policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
        policyLink.textContent = item.policy
        const policyEl = document.getElementById('e-policy')
        if (policyEl) policyEl.appendChild(policyLink)
      }
    }
  })
}
updateExchange(true)
setInterval(updateExchange, 5000)

const tableOptions = {
  dataSource: new UrlDataSource<BindingItem>(exchangeUrl + '/bindings/source', { useQueryState: false }),
  pagination: true,
  keyColumns: ['destination', 'properties_key'],
  search: true,
  countId: 'bindings-table-count',
}
const bindingsTable = Table.renderTable<BindingItem>('bindings-table', tableOptions, function (tr, item, all) {
  if (!all) return
  if (item.source === '') {
    const td = Table.renderCell(tr, 0, '(Default exchange binding)')
    td?.setAttribute('colspan', '5')
  } else {
    const btn = DOM.button.delete({
      text: 'Unbind',
      click: function () {
        const type = item.destination_type === 'exchange' ? 'e' : 'q'
        const url = HTTP.url`api/bindings/${vhost}/e/${item.source}/${type}/${item.destination}/${item.properties_key}`
        HTTP.request('DELETE', url).then(() => {
          tr.parentNode?.removeChild(tr)
        })
      },
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

const addBindingForm = document.querySelector('#addBinding')
if (addBindingForm) {
  addBindingForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const data = new FormData(form)
    const d = (data.get('destination') as string).trim()
    const t = data.get('dest-type') as string
    const url = HTTP.url`api/bindings/${vhost}/e/${exchange}/${t}/${d}`
    const args = DOM.parseJSON(data.get('arguments') as string)
    const body = {
      routing_key: (data.get('routing_key') as string).trim(),
      arguments: args,
    }
    HTTP.request('POST', url, { body })
      .then(() => {
        bindingsTable.reload()
        form.reset()
      })
      .catch((e: { status?: number }) => {
        if (e.status === 404) {
          const type = t === 'q' ? 'Queue' : 'Exchange'
          DOM.toast.error(`${type} '${d}' does not exist and needs to be created first.`)
        }
      })
  })
}

const publishForm = document.querySelector('#publishMessage')
if (publishForm) {
  publishForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const data = new FormData(form)
    const url = HTTP.url`api/exchanges/${vhost}/${exchange}/publish`
    const properties = DOM.parseJSON<Record<string, unknown>>(data.get('properties') as string)
    properties['delivery_mode'] = parseInt(data.get('delivery_mode') as string, 10)
    const headers = DOM.parseJSON<Record<string, unknown>>(data.get('headers') as string)
    properties['headers'] = { ...(properties['headers'] as Record<string, unknown>), ...headers }
    const body = {
      payload: data.get('payload'),
      payload_encoding: data.get('payload_encoding'),
      routing_key: (data.get('routing_key') as string).trim(),
      properties,
    }
    HTTP.request<PublishResponse>('POST', url, { body }).then((res) => {
      DOM.toast('Published message to ' + exchange + (res?.routed ? '.' : ', but not routed.'))
    })
  })
}

const deleteForm = document.querySelector('#deleteExchange')
if (deleteForm) {
  deleteForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = HTTP.url`api/exchanges/${vhost}/${exchange}`
    if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
      HTTP.request('DELETE', url).then(() => {
        window.location.href = 'exchanges'
      })
    }
  })
}

function updateAutocomplete(val: string): void {
  const type = val === 'q' ? 'queues' : 'exchanges'
  Helpers.autoCompleteDatalist('exchange-dest-list', type, vhost)
}
updateAutocomplete('q')
const destTypeSelect = document.getElementById('dest-type')
if (destTypeSelect) {
  destTypeSelect.addEventListener('change', (e) => {
    const target = e.target as HTMLSelectElement
    updateAutocomplete(target.value)
  })
}

const dataTags = document.querySelector('#dataTags')
if (dataTags) {
  dataTags.addEventListener('click', (e) => {
    Helpers.argumentHelperJSON('publishMessage', 'properties', e)
  })
}

// Handle navigation to another exchange via hash change
window.addEventListener('hashchange', () => {
  window.location.reload()
})
