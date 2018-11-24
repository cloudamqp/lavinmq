
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
    })
  }

  function render (data) {
    document.querySelector('#version').innerText = data.avalanchemq_version
    const table = document.querySelector('#overview')
    if (table) {
      Object.keys(data.object_totals).forEach(function (key) {
        table.querySelector('.' + key).innerText = data.object_totals[key]
      })
    }
  }

  function start (cb) {
    update(cb)
    updateTimer = setInterval(() => update(cb), 5000)
  }

  function stop () {
    if (updateTimer) {
      clearInterval(updateTimer)
    }
  }

  Object.assign(window.avalanchemq, {
    overview: {
      update, start, stop, render, data
    }
  })
})()
