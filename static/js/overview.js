
/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  const numFormatter = new Intl.NumberFormat()
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

  function render (data) {
    document.querySelector('#version').innerText = data.avalanchemq_version
    document.querySelector('#cluster_name').innerText = data.node
    const table = document.querySelector('#overview')
    if (table) {
      Object.keys(data.object_totals).forEach(function (key) {
        table.querySelector('.' + key).innerText = numFormatter.format(data.object_totals[key])
      })
      table.querySelector('.uptime').innerText = avalanchemq.helpers.duration(data.uptime)
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
