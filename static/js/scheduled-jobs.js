import * as HTTP from './http.js'
import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Form from './form.js'
import { UrlDataSource } from './datasource.js'

Helpers.addVhostOptions('createScheduledJob')

function renderState (item) {
  if (item.error_message) {
    const state = document.createElement('a')
    state.classList.add('arg-tooltip')
    state.appendChild(document.createTextNode(item.state))
    const tooltip = document.createElement('span')
    tooltip.classList.add('tooltiptext')
    tooltip.textContent = item.error_message
    state.appendChild(tooltip)
    return state
  } else {
    return item.state
  }
}

function formatDate (dateStr) {
  if (!dateStr) return '-'
  const date = new Date(dateStr)
  return date.toLocaleString()
}

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
      let jobs = values[0].items ?? values[0]
      const status = values[1]
      jobs = jobs.map(item => {
        const jobStatus = status.find(s => s.name === item.name && s.vhost === item.vhost)
        item.state = jobStatus.state
        item.error_message = jobStatus.error_message
        item.last_run = jobStatus.last_run
        item.next_run = jobStatus.next_run
        item.enabled = jobStatus.enabled
        return item
      })
      return jobs
    })
  }
}

const dataSource = new ScheduledJobsDataSource(url, statusUrl)
const tableOptions = { keyColumns: ['vhost', 'name'], columnSelector: true, dataSource }
Table.renderTable('table', tableOptions, (tr, item, all) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, item.value.exchange)
  Table.renderCell(tr, 3, item.value.routing_key)
  Table.renderCell(tr, 4, item.value.schedule)
  Table.renderCell(tr, 5, item.enabled ? 'Yes' : 'No')
  Table.renderCell(tr, 6, renderState(item))
  Table.renderCell(tr, 7, formatDate(item.last_run))
  Table.renderCell(tr, 8, formatDate(item.next_run))

  const btns = document.createElement('div')
  btns.classList.add('buttons')

  const deleteBtn = DOM.button.delete({
    click: function () {
      const url = HTTP.url`api/parameters/scheduled-job/${item.vhost}/${item.name}`
      if (window.confirm('Are you sure? This scheduled job cannot be restored after deletion.')) {
        HTTP.request('DELETE', url)
          .then(() => {
            tr.parentNode.removeChild(tr)
            DOM.toast(`Scheduled job ${item.name} deleted`)
          })
      }
    }
  })

  const editBtn = DOM.button.edit({
    click: function () {
      Form.editItem('#createScheduledJob', item, {})
    }
  })

  const enableLabel = item.enabled ? 'Disable' : 'Enable'
  const enableBtn = DOM.button.edit({
    click: function () {
      const action = item.enabled ? 'disable' : 'enable'
      const url = HTTP.url`api/scheduled-jobs/${item.vhost}/${item.name}/${action}`

      if (window.confirm(`Are you sure you want to ${action} this job?`)) {
        HTTP.request('PUT', url)
          .then(() => {
            dataSource.reload()
            DOM.toast(`Scheduled job ${item.name} ${action}d`)
          })
          .catch((err) => {
            console.error(err)
            DOM.toast(`Failed to ${action} scheduled job ${item.name}`, 'error')
          })
      }
    },
    text: enableLabel
  })

  const triggerBtn = DOM.button.edit({
    click: function () {
      const url = HTTP.url`api/scheduled-jobs/${item.vhost}/${item.name}/trigger`

      if (window.confirm(`Trigger job "${item.name}" now?`)) {
        HTTP.request('PUT', url)
          .then(() => {
            dataSource.reload()
            DOM.toast(`Scheduled job ${item.name} triggered`)
          })
          .catch((err) => {
            console.error(err)
            DOM.toast(`Failed to trigger scheduled job ${item.name}`, 'error')
          })
      }
    },
    text: 'Trigger Now'
  })

  btns.append(editBtn, enableBtn, triggerBtn, deleteBtn)
  Table.renderCell(tr, 9, btns, 'right')
})

document.querySelector('#createScheduledJob').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = data.get('name').trim()
  const vhost = data.get('vhost')
  const url = HTTP.url`api/parameters/scheduled-job/${vhost}/${name}`
  const body = {
    value: {
      exchange: data.get('exchange'),
      routing_key: data.get('routing_key'),
      body: data.get('body'),
      schedule: data.get('schedule'),
      enabled: data.get('enabled') === 'on'
    }
  }

  HTTP.request('PUT', url, { body })
    .then(() => {
      dataSource.reload()
      evt.target.reset()
      DOM.toast(`Scheduled job ${name} saved`)
    })
    .catch((err) => {
      console.error(err)
      DOM.toast(`Failed to save scheduled job: ${err.message}`, 'error')
    })
})
