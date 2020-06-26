
/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  const url = '/api/nodes'
  let data = null
  let updateTimer = null

  if (data === null) {
    update(render)
  }

  function update (cb) {
    avalanchemq.http.request('GET', url).then(function (response) {
      data = response
      render(response)
      if (cb) {
        cb(response)
      }
    }).catch(avalanchemq.http.standardErrorHandler).catch(stop)
  }

  function render (data) {
    document.querySelector('#version').innerText = data[0].applications[0].version
    for (const node of data) {
      updateDetails(node)
      updateStats(node)
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

  const updateDetails = (nodeStats) => {
    document.getElementById('tr-name').textContent = nodeStats.name
    document.getElementById('tr-uptime').textContent = duration((nodeStats.uptime / 1000).toFixed(0))
    document.getElementById('tr-vcpu').textContent = nodeStats.processors
    document.getElementById('tr-memory').textContent = (
      nodeStats.mem_used / 10 ** 9
    ).toFixed(2) + ' GB (' + ((nodeStats.mem_used / nodeStats.mem_limit) * 100).toFixed(2) + '%)'
    document.getElementById('tr-cpu').textContent = (
      ((nodeStats.cpu_user_time + nodeStats.cpu_sys_time) / nodeStats.uptime) * 100
    ).toFixed(2) + '%'
    document.getElementById('tr-disk').textContent = (
      (nodeStats.disk_total - nodeStats.disk_free) / 10 ** 9
    ).toFixed(2) + ' GB (' + (
      (nodeStats.disk_total - nodeStats.disk_free) / nodeStats.disk_total
    ).toFixed(2) * 100 + '%)'
  }

  const stats = [
    {
      heading: 'Connection',
      content: [
        {
          heading: 'Created',
          key: 'connections_created'
        },
        {
          heading: 'Closed',
          key: 'connections_closed'
        }
      ]
    },
    {
      heading: 'Channels',
      content: [
        {
          heading: 'Created',
          key: 'channels_created'
        },
        {
          heading: 'Closed',
          key: 'channels_closed'
        }
      ]
    },
    {
      heading: 'Queues',
      content: [
        {
          heading: 'Declared',
          key: 'queues_declared'
        },
        {
          heading: 'Deleted',
          key: 'queues_deleted'
        }
      ]
    },
    {
      heading: 'File descriptors',
      content: [
        {
          heading: 'Used',
          key: 'fd_used'
        },
        {
          heading: 'Total',
          key: 'fd_total'
        }
      ]
    }
  ]

  const numFormatter = new Intl.NumberFormat()
  const updateStats = (nodeStats) => {
    const table = document.getElementById('stats-table')
    while (table.firstChild) {
      table.firstChild.remove()
    }

    for (const rowStats of stats) {
      const row = document.createElement('tr')
      const th = document.createElement('th')
      th.textContent = rowStats.heading
      row.append(th)
      for (const items of rowStats.content) {
        const td = document.createElement('td')
        td.textContent = items.heading + ': ' + numFormatter.format(nodeStats[items.key])
        row.append(td)
      }
      table.append(row)
    }
  }

  Object.assign(window.avalanchemq, {
    nodes: {
      update, start, stop, render, get
    }
  })
})()
