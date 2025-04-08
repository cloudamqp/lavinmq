import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as HTTP from './http.js'
import * as Chart from './chart.js'
import { DataSource } from './datasource.js'

const channel = new URLSearchParams(window.location.hash.substring(1)).get('name')
const urlEncodedChannel = encodeURIComponent(channel)
const channelUrl = 'api/channels/' + urlEncodedChannel
const chart = Chart.render('chart', 'msgs/s')
let vhost = null
document.title = channel + ' | LavinMQ'

const consumersDataSource = new (class extends DataSource {
  constructor () { super({ autoReloadTimeout: 0, useQueryState: false }) }
  setConsumers (consumers) { this.items = consumers }
  reload () { }
})()

const consumerTableOpts = {
  dataSource: consumersDataSource,
  keyColumns: ['consumer_tag'],
  countId: 'consumer-count'
}
Table.renderTable('table', consumerTableOpts, function (tr, item) {
  Table.renderCell(tr, 0, item.consumer_tag)
  const queueLink = document.createElement('a')
  queueLink.href = `queue#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(item.queue.name)}`
  queueLink.textContent = item.queue.name
  const ack = item.ack_required ? '●' : '○'
  const exclusive = item.exclusive ? '●' : '○'
  Table.renderCell(tr, 1, queueLink)
  Table.renderCell(tr, 2, ack, 'center')
  Table.renderCell(tr, 3, exclusive, 'center')
  Table.renderCell(tr, 4, Helpers.formatNumber(item.prefetch_count), 'right')
})

const prefetchForm = (cb) => {
  const input = document.createElement('input')
  input.type = 'number'

  const save = document.createElement('button')
  save.type = 'submit'
  save.title = 'Save'
  save.classList.add('btn-icon', 'btn-submit')

  const reset = document.createElement('button')
  reset.type = 'reset'
  reset.title = 'Reset'
  reset.classList.add('btn-icon', 'btn-reset')

  const form = document.createElement('form')
  form.classList.add('prefetch-form')
  form.addEventListener('submit', (event) => {
    event.preventDefault()
    const prefetch = parseInt(input.value)
    HTTP.request('PUT', channelUrl, { body: { prefetch } })
      .then((r) => {
        if (!(r && r.is_error)) {
          cb(prefetch)
        }
      })
  })
  form.append(input, save, reset)
  const updateForm = (value) => { input.value = value }
  return { form, updateForm }
}
const prefetchView = (cb) => {
  const text = document.createElement('div')
  text.title = 'Click to edit'
  text.style.cursor = 'pointer'
  text.addEventListener('click', cb)
  const updateView = (value) => { text.textContent = Helpers.formatNumber(value) }
  return { text, updateView }
}

const prefetchHandler = () => {
  const el = document.createElement('div')
  let editing = false
  const rerender = (editMode) => {
    editing = editMode
    if (editMode) {
      el.replaceChild(form, text)
    } else {
      el.replaceChild(text, form)
    }
  }
  const { form, updateForm } = prefetchForm((newValue) => {
    updateView(newValue)
    rerender(false)
  })
  const { text, updateView } = prefetchView(() => rerender(true))
  const update = (value) => {
    if (!editing) updateForm(value)
    updateView(value)
  }
  el.appendChild(text)
  return { el, update }
}

const prefetch = prefetchHandler()
document.getElementById('ch-prefetch').appendChild(prefetch.el)
function updateChannel () {
  HTTP.request('GET', channelUrl).then(item => {
    Chart.update(chart, item.message_stats)
    vhost = item.vhost
    const stateEl = document.getElementById('ch-state')
    if (item.state !== stateEl.textContent) {
      stateEl.textContent = item.state
    }
    document.getElementById('ch-unacked').textContent = item.messages_unacknowledged
    consumersDataSource.setConsumers(item.consumer_details)
    document.getElementById('pagename-label').textContent = `${channel} in virtual host ${item.vhost}`
    document.getElementById('ch-username').textContent = item.user
    const connectionLink = document.querySelector('#ch-connection a')
    connectionLink.href = `connection#name=${encodeURIComponent(item.connection_details.name)}`
    connectionLink.textContent = item.connection_details.name
    prefetch.update(item.prefetch_count)
    document.getElementById('ch-mode').textContent = `${item.confirm ? 'C' : ''}`
    document.getElementById('ch-global-prefetch').textContent = Helpers.formatNumber(item.global_prefetch_count)
  })
}
updateChannel()
setInterval(updateChannel, 5000)
