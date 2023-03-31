import * as HTTP from './http.js'
import * as Helpers from './helpers.js'

const numFormatter = new Intl.NumberFormat()
const url = 'api/overview'
let updateTimer = null
let data = null

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
  return url + '/' + vhost
}

function update (cb) {
  const vhost = window.sessionStorage.getItem('vhost')
  const headers = new window.Headers()
  if (vhost && vhost !== '_all') {
    headers.append('x-vhost', vhost)
  }
  HTTP.request('GET', url, { headers }).then(function (response) {
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

function get (key) {
  return new Promise(function (resolve, reject) {
    try {
      if (data) {
        resolve(data[key])
      } else {
        update(data => {
          resolve(data[key])
        })
      }
    } catch (e) {
      reject(e.message)
    }
  })
}

export {
  update, start, stop, render, get
}
