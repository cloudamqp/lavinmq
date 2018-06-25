
(function () {
  window.avalanchemq = window.avalanchemq || {}

  const url = '/api/overview'
  const raw = sessionStorage.getItem(cacheKey())
  let data = null
  let updateTimer = null

  if (raw) {
    try {
      data = JSON.parse(raw)
      if (data) {
        render(data)
      }
    } catch (e) {
      sessionStorage.removeItem(cacheKey())
      console.log('Error parsing data from sessionStorage')
      console.error(e)
    }
  }

  function cacheKey () {
    const vhost = sessionStorage.getItem('vhost')
    return url + '/' + vhost
  }

  function update () {
    const vhost = sessionStorage.getItem('vhost')
    const headers = new Headers()
    if (vhost && vhost !== "_all") {
      headers.append('x-vhost', vhost)
    }
    avalanchemq.http.request('GET', url, { headers }).then(function (response) {
      data = response
      try {
        sessionStorage.setItem(cacheKey(), JSON.stringify(response))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      render(response)
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

  function start () {
    update()
    updateTimer = setInterval(update, 5000)
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
