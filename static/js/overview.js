import * as Chart from './chart.js'
import * as HTTP from './http.js'
import * as Helpers from './helpers.js'

const numFormatter = new Intl.NumberFormat()
let updateTimer = null
let data = null

const msgChart = Chart.render('msgChart', 'msgs', true)
const dataChart = Chart.render('dataChart', 'bytes/s')
const rateChart = Chart.render('rateChart', 'msgs/s')

function updateCharts (response) {
  let msgStats = {
    messages_ready: response.queue_totals.messages_ready,
    messages_unacked: response.queue_totals.messages_unacknowledged,
    messages_ready_log: response.queue_totals.messages_ready_log,
    messages_unacked_log: response.queue_totals.messages_unacknowledged_log,
  }
  Chart.update(msgChart, msgStats, "origin")
  Chart.update(rateChart, response.message_stats)

  const dataStats = {
    send_details: response.send_oct_details,
    receive_details: response.recv_oct_details
  }
  Chart.update(dataChart, dataStats)
}

start(updateCharts)
Helpers.addVhostOptions('importDefinitions', {addAll: true})
document.querySelector('#importDefinitions').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const body = new window.FormData(this)
  let url = 'api/definitions/'
  if(body.get('vhost') === '_all') {
    url += 'upload'
  } else {
    url += encodeURIComponent(body.get('vhost')) + '/upload'
  }
  HTTP.request('POST', url, { body }).then(function () {
    window.location.assign('.')
  }).catch(function () {
    window.alert('Upload failed')
  })
})

Helpers.addVhostOptions('exportDefinitions', {addAll: true})
document.querySelector('#exportDefinitions').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const body = new window.FormData(this)
  let url = 'api/definitions'
  if (body.get('vhost') !== '_all') {
    url += '/' + encodeURIComponent(body.get('vhost'))
  }
  HTTP.request('GET', url).then(function (data) {
    const a = document.createElement('a')
    a.classList.add('hide')
    let name = 'lavinmq-definitions-' + window.location.hostname
    name += '-' + new Date().toISOString() + '.json'
    const blob = new window.Blob([JSON.stringify(data, null, 2)], { type: 'text/json', name })
    a.download = name
    a.href = URL.createObjectURL(blob)
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
  }).catch(HTTP.standardErrorHandler)
})

const raw = window.sessionStorage.getItem(cacheKey())
if (raw) {
  try {
    data = JSON.parse(raw)
  } catch (e) {
    window.sessionStorage.removeItem(cacheKey())
    console.error('Error parsing data from sessionStorage', e)
  }
} else {
  update(render)
}

function cacheKey () {
  const vhost = window.sessionStorage.getItem('vhost')
  return 'api/overview/' + vhost
}

function update (cb) {
  const vhost = window.sessionStorage.getItem('vhost')
  const headers = new window.Headers()
  if (vhost && vhost !== '_all') {
    headers.append('x-vhost', vhost)
  }
  HTTP.request('GET', 'api/overview', { headers }).then(function (response) {
    data = response
    try {
      window.sessionStorage.setItem(cacheKey(), JSON.stringify(response))
    } catch (e) {
      console.error('Saving sessionStorage', e)
    }
    render(response)
    if (cb) {
      cb(response)
    }
  }).catch(HTTP.standardErrorHandler).catch(stop)
}

function render (data) {
  const table = document.getElementById('overview')
  if (table) {
    Object.keys(data.object_totals).forEach((key) => {
      document.getElementById(key).textContent = numFormatter.format(data.object_totals[key])
    })
    document.getElementById('uptime').textContent = Helpers.duration(data.uptime)
  }
}

function start (cb) {
  update(cb)
  updateTimer = setInterval(update, 5000, cb)
}

// Show that we're offline in the UI
function stop () {
  if (updateTimer) {
    clearInterval(updateTimer)
  }
}
