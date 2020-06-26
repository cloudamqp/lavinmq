
/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  const url = '/api/overview'
  const raw = window.sessionStorage.getItem(cacheKey())
  let data = null
  let updateTimer = null

  if (raw) {
    try {
      data = JSON.parse(raw)
      if (data) {
        render(data)
      }
    } catch (e) {
      window.sessionStorage.removeItem(cacheKey())
      console.log('Error parsing data from sessionStorage')
      console.error(e)
    }
  }

  if (data === null) {
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
    avalanchemq.http.request('GET', url, { headers }).then(function (response) {
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
    }).catch(avalanchemq.http.standardErrorHandler).catch(stop)
  }

  function duration (secs) {
    let res = ''
    const days = Math.floor(secs / (24 * 3600))
    if (days > 0) {
      res += days + ' d, '
    }
    const daysRest = secs % (24 * 3600)
    const hours = Math.floor(daysRest / 3600)
    if (hours > 0) {
      res += hours + ' h, '
    }
    const hoursRest = daysRest % 3600
    const minutes = Math.floor(hoursRest / 60)
    res += minutes + ' m '
    const minRest = hoursRest % 60
    const seconds = Math.ceil(minRest)
    if (days === 0) {
      res += seconds + ' s'
    }
    return res
  }

  function render (data) {
    document.querySelector('#version').innerText = data.avalanchemq_version
    const table = document.querySelector('#overview')
    if (table) {
      Object.keys(data.object_totals).forEach(function (key) {
        table.querySelector('.' + key).innerText = data.object_totals[key]
      })
      table.querySelector('.uptime').innerText = duration(data.uptime)
    }
  }

  function start (cb) {
    update(cb)
    updateTimer = setInterval(() => update(cb), 5000)
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

  Object.assign(window.avalanchemq, {
    overview: {
      update, start, stop, render, get
    }
  })
})()
