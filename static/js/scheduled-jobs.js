import * as HTTP from './http.js'
import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Form from './form.js'
import { UrlDataSource } from './datasource.js'

Helpers.addVhostOptions('createScheduledJob')

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/parameters/scheduled-job'
let statusUrl = 'api/scheduled-jobs'
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
  statusUrl += HTTP.url`/${vhost}`
}

class ScheduledJobsDataSource extends UrlDataSource {
  constructor (url, statusUrl) {
    super(url)
    this.statusUrl = statusUrl
  }

  _reload () {
    const p1 = super._reload()
    const p2 = HTTP.request('GET', this.statusUrl)
    return Promise.all([p1, p2]).then(values => {
      const params = values[0].items ?? values[0]
      const statuses = values[1].items ?? values[1]
      return params.map(item => {
        const s = statuses.find(s => s.name === item.name && s.vhost === item.vhost) || {}
        item.state = s.state
        item.runCount = s.run_count
        item.lastRunAt = s.last_run_at
        item.nextRunAt = s.next_run_at
        item.lastError = s.last_error
        return item
      })
    })
  }
}

const dataSource = new ScheduledJobsDataSource(url, statusUrl)
const tableOptions = { keyColumns: ['vhost', 'name'], columnSelector: false, dataSource }

function fmtTime (iso) {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleString()
  } catch (e) {
    return iso
  }
}

function renderState (item) {
  if (item.lastError) {
    const wrap = document.createElement('span')
    wrap.classList.add('arg-tooltip')
    wrap.appendChild(document.createTextNode(item.state || '—'))
    const tt = document.createElement('span')
    tt.classList.add('tooltiptext')
    tt.textContent = item.lastError
    wrap.appendChild(tt)
    return wrap
  }
  return item.state || '—'
}

Table.renderTable('table', tableOptions, (tr, item, _all) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, item.value.exchange || '(default)')
  Table.renderCell(tr, 3, item.value['routing-key'])
  Table.renderCell(tr, 4, item.value.cron)
  Table.renderCell(tr, 5, fmtTime(item.lastRunAt))
  Table.renderCell(tr, 6, fmtTime(item.nextRunAt))
  Table.renderCell(tr, 7, item.runCount ?? 0)
  Table.renderCell(tr, 8, renderState(item))

  const btns = document.createElement('div')
  btns.classList.add('buttons')

  const runBtn = DOM.button.edit({
    click: function () {
      const url = HTTP.url`api/scheduled-jobs/${item.vhost}/${item.name}/run`
      HTTP.request('POST', url)
        .then(() => { dataSource.reload(); DOM.toast(`Job ${item.name} dispatched`) })
        .catch((err) => { console.error(err); DOM.toast.error(`Failed to dispatch ${item.name}`) })
    },
    text: 'Run now'
  })

  const pauseLabel = item.state === 'Scheduled' ? 'Pause' : 'Resume'
  const pauseBtn = DOM.button.edit({
    click: function () {
      const isScheduled = item.state === 'Scheduled'
      const action = isScheduled ? 'pause' : 'resume'
      const url = HTTP.url`api/scheduled-jobs/${item.vhost}/${item.name}/${action}`
      HTTP.request('PUT', url)
        .then(() => { dataSource.reload(); DOM.toast(`Job ${item.name} ${action}d`) })
        .catch((err) => { console.error(err); DOM.toast.error(`Failed to ${action} ${item.name}`) })
    },
    text: pauseLabel
  })

  const editBtn = DOM.button.edit({
    click: function () {
      Form.editItem('#createScheduledJob', item, {
        cron: (item) => item.value.cron,
        exchange: (item) => item.value.exchange,
        'routing-key': (item) => item.value['routing-key'],
        body: (item) => item.value.body,
        'content-type': (item) => item.value['content-type'],
        headers: (item) => item.value.headers ? JSON.stringify(item.value.headers) : ''
      })
    }
  })

  const deleteBtn = DOM.button.delete({
    click: function () {
      const url = HTTP.url`api/parameters/scheduled-job/${item.vhost}/${item.name}`
      if (window.confirm('Are you sure? This scheduled job will be permanently removed.')) {
        HTTP.request('DELETE', url)
          .then(() => { tr.parentNode.removeChild(tr); DOM.toast(`Job ${item.name} deleted`) })
      }
    }
  })

  btns.append(runBtn, pauseBtn, editBtn, deleteBtn)
  Table.renderCell(tr, 9, btns, 'right')
})

document.querySelector('#createScheduledJob').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = data.get('name').trim()
  const vhost = data.get('vhost')
  const url = HTTP.url`api/parameters/scheduled-job/${vhost}/${name}`

  const value = {
    cron: data.get('cron'),
    exchange: data.get('exchange') || '',
    'routing-key': data.get('routing-key'),
    body: data.get('body')
  }
  const ct = data.get('content-type')
  if (ct) value['content-type'] = ct
  const headersRaw = data.get('headers')
  if (headersRaw && headersRaw.trim().length) {
    try {
      const parsed = JSON.parse(headersRaw)
      if (typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('not an object')
      value.headers = parsed
    } catch (e) {
      DOM.toast.error('Headers must be a JSON object')
      return
    }
  }
  HTTP.request('PUT', url, { body: { value } })
    .then(() => {
      dataSource.reload()
      evt.target.reset()
      DOM.toast(`Scheduled job ${name} saved`)
    })
    .catch((err) => {
      console.error(err)
      DOM.toast.error(`Failed to save: ${err.message || err}`)
    })
})
