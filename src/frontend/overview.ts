import * as Chart from './chart.js'
import * as HTTP from './http.js'
import * as Helpers from './helpers.js'

interface QueueTotals {
  messages_ready: number
  messages_unacknowledged: number
  messages_ready_log?: number[]
  messages_unacknowledged_log?: number[]
}

interface MessageStats {
  [key: string]: unknown
}

interface ObjectTotals {
  [key: string]: number
}

interface OverviewResponse {
  queue_totals: QueueTotals
  message_stats: MessageStats
  send_oct_details: { rate: number }
  recv_oct_details: { rate: number }
  object_totals: ObjectTotals
  uptime: number
}

const numFormatter = new Intl.NumberFormat()
const msgChart = Chart.render('msgChart', 'msgs', true, true, true)
const dataChart = Chart.render('dataChart', 'bytes/s')
const rateChart = Chart.render('rateChart', 'msgs/s')

function updateCharts(response: OverviewResponse): void {
  const msgStats = {
    messages_ready: response.queue_totals.messages_ready,
    messages_unacked: response.queue_totals.messages_unacknowledged,
    messages_ready_log: response.queue_totals.messages_ready_log,
    messages_unacked_log: response.queue_totals.messages_unacknowledged_log,
  }
  Chart.update(msgChart, msgStats, true)
  Chart.update(rateChart, response.message_stats as Record<string, number>)

  const dataStats = {
    send_details: response.send_oct_details,
    receive_details: response.recv_oct_details,
  }
  Chart.update(dataChart, dataStats)
}

start(updateCharts)
Helpers.addVhostOptions('importDefinitions', { addAll: true })

const importForm = document.querySelector('#importDefinitions')
if (importForm) {
  importForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const body = new FormData(form)
    let url = 'api/definitions/'
    if (body.get('vhost') === '_all') {
      url += 'upload'
    } else {
      url += HTTP.url`${body.get('vhost') as string}/upload`
    }
    HTTP.request('POST', url, { body })
      .then(function () {
        window.location.assign('.')
      })
      .catch(function () {
        window.alert('Upload failed')
      })
  })
}

Helpers.addVhostOptions('exportDefinitions', { addAll: true })

const exportForm = document.querySelector('#exportDefinitions')
if (exportForm) {
  exportForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const body = new FormData(form)
    let url = 'api/definitions'
    if (body.get('vhost') !== '_all') {
      url += HTTP.url`/${body.get('vhost') as string}`
    }
    HTTP.request<Record<string, unknown>>('GET', url).then(function (data) {
      const a = document.createElement('a')
      a.classList.add('hide')
      let name = 'lavinmq-definitions-' + window.location.hostname
      name += '-' + new Date().toISOString() + '.json'
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'text/json' })
      a.download = name
      a.href = URL.createObjectURL(blob)
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
    })
  })
}

const raw = window.sessionStorage.getItem(cacheKey())
if (raw) {
  try {
    const data = JSON.parse(raw) as OverviewResponse
    render(data)
  } catch (e) {
    window.sessionStorage.removeItem(cacheKey())
    console.error('Error parsing data from sessionStorage', e)
  }
}

function cacheKey(): string {
  const vhost = window.sessionStorage.getItem('vhost')
  return 'api/overview/' + vhost
}

function update(cb?: (response: OverviewResponse) => void): void {
  const vhost = window.sessionStorage.getItem('vhost')
  const headers = new Headers()
  if (vhost && vhost !== '_all') {
    headers.append('x-vhost', vhost)
  }
  HTTP.request<OverviewResponse>('GET', 'api/overview', { headers }).then(function (response) {
    if (!response) return
    try {
      window.sessionStorage.setItem(cacheKey(), JSON.stringify(response))
    } catch (e) {
      console.error('Saving sessionStorage', e)
    }
    render(response)
    if (cb) {
      cb(response)
    }
  })
}

function render(data: OverviewResponse): void {
  const table = document.getElementById('overview')
  if (table) {
    Object.keys(data.object_totals).forEach((key) => {
      const el = document.getElementById(key)
      if (el) {
        el.textContent = numFormatter.format(data.object_totals[key] ?? 0)
      }
    })
    const uptimeEl = document.getElementById('uptime')
    if (uptimeEl) {
      uptimeEl.textContent = Helpers.duration(data.uptime)
    }
  }
}

function start(cb: (response: OverviewResponse) => void): void {
  update(cb)
  setInterval(update, 5000, cb)
}
