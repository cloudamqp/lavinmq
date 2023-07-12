import * as HTTP from './http.js'
import * as Chart from './chart.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import { DataSource } from './datasource.js'

const numFormatter = new Intl.NumberFormat()
let url = 'api/nodes'
const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += '?vhost=' + encodeURIComponent(vhost)
}
let data = null

if (data === null) {
  update(render)
}

function update (cb) {
  HTTP.request('GET', url).then((response) => {
    data = response
    render(response)
    if (cb) {
      cb(response)
    }
  }).catch(HTTP.standardErrorHandler).catch(stop)
}

function render (data) {
  document.querySelector('#version').textContent = data[0].applications[0].version
  for (const node of data) {
    updateDetails(node)
    updateStats(node)
  }
}

function start (cb) {
  update(cb)
  setInterval(() => update(cb), 5000)
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

const updateDetails = (nodeStats) => {
  document.getElementById('tr-name').textContent = nodeStats.name
  document.getElementById('tr-uptime').textContent = Helpers.duration((nodeStats.uptime / 1000).toFixed(0))
  document.getElementById('tr-vcpu').textContent = nodeStats.processors || "N/A"
  let memUsage, cpuUsage, diskUsage

  if(nodeStats.mem_used !== undefined) {
    const mem_used_gb = (nodeStats.mem_used / 1024 ** 3).toFixed(3)
    const mem_limit_gb = (nodeStats.mem_limit / 1024 ** 3).toFixed(3)
    const mem_pcnt = (nodeStats.mem_used / nodeStats.mem_limit * 100).toFixed(2)
    memUsage = `${mem_used_gb}/${mem_limit_gb} GiB (${mem_pcnt}%)`
  } else {
    memUsage = "N/A"
  }
  document.getElementById('tr-memory').textContent = memUsage
  if(nodeStats.cpu_user_time !== undefined) {
    const cpu_pcnt = (((nodeStats.cpu_user_time + nodeStats.cpu_sys_time) / nodeStats.uptime) * 100).toFixed(2)
    cpuUsage = `${cpu_pcnt}%`
  } else {
    cpuUsage = "N/A"
  }
  document.getElementById('tr-cpu').textContent = cpuUsage
  if(nodeStats.disk_total !== undefined) {
    const disk_usage_gb = ((nodeStats.disk_total - nodeStats.disk_free) / 1024 ** 3).toFixed(3)
    const disk_total_gb = (nodeStats.disk_total / 1024 ** 3).toFixed(0)
    const disk_pcnt = ((nodeStats.disk_total - nodeStats.disk_free) / nodeStats.disk_total * 100).toFixed(2)
    diskUsage = `${disk_usage_gb}/${disk_total_gb} GiB (${disk_pcnt}%)`
  } else {
    diskUsage = "N/A"
  }
  document.getElementById('tr-disk').textContent = diskUsage
}

const stats = [
  {
    heading: 'Connection',
    content: [
      {
        heading: 'Created',
        key: 'connection_created'
      },
      {
        heading: 'Closed',
        key: 'connection_closed'
      }
    ]
  },
  {
    heading: 'Channels',
    content: [
      {
        heading: 'Created',
        key: 'channel_created'
      },
      {
        heading: 'Closed',
        key: 'channel_closed'
      }
    ]
  },
  {
    heading: 'Queues',
    content: [
      {
        heading: 'Declared',
        key: 'queue_declared'
      },
      {
        heading: 'Deleted',
        key: 'queue_deleted'
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
  },
  {
    heading: 'Messages',
    content: [
      {
        heading: 'Ready',
        key: 'messages_ready'
      },
      {
        heading: 'Unacked',
        key: 'messages_unacknowledged'
      }
    ]
  }
]

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
    let metrics = 0
    for (const items of rowStats.content) {
      if(nodeStats[items.key] !== undefined) {
        const td = document.createElement('td')
        td.textContent = items.heading + ': ' + numFormatter.format(nodeStats[items.key])
        row.append(td)
        metrics += 1
      }
    }
    if(metrics > 0) {
      table.append(row)
    }
  }
}
const memoryChart = Chart.render('memoryChart', 'MB')
const ioChart = Chart.render('ioChart', 'ops')
const cpuChart = Chart.render('cpuChart', '%', true)
const connectionChurnChart = Chart.render('connectionChurnChart', '/s')
const channelChurnChart = Chart.render('channelChurnChart', '/s')
const queueChurnChart = Chart.render('queueChurnChart', '/s')

const toMegaBytes = (dataPointInBytes) => (dataPointInBytes / 1024 ** 2).toFixed(2)

const followersDataSource = new (class extends DataSource {
  constructor() { super({autoReloadTimeout: 0, useQueryState: false}) }
  update(items) { this.items = items }
  reload() { }
})
const followersTableOpts = {
  dataSource: followersDataSource,
  keyColumns: ['remote_address'],
  countId: 'followers-count'
}
const followersTable = Table.renderTable('followers', followersTableOpts, (tr, item, firstRender) => {
  if (firstRender) {
    Table.renderCell(tr, 0, item.remote_address)
  }
  Table.renderCell(tr, 1, humanizeBytes(item.sent_bytes), 'right')
  Table.renderCell(tr, 2, humanizeBytes(item.acked_bytes), 'right')
  Table.renderCell(tr, 3, humanizeBytes(item.sent_bytes - item.acked_bytes), 'right')
})

function updateCharts (response) {
  if(response[0].mem_used !== undefined) {
    const memoryStats = {
      mem_used_details: toMegaBytes(response[0].mem_used),
      mem_used_details_log: response[0].mem_used_details.log.map(toMegaBytes)
    }
    Chart.update(memoryChart, memoryStats)
  }
  if(response[0].io_write_details !== undefined) {
    const ioStats = {
      io_write_details: response[0].io_write_details.log.slice(-1)[0],
      io_write_details_log: response[0].io_write_details.log,
      io_read_details: response[0].io_read_details.log.slice(-1)[0],
      io_read_details_log: response[0].io_read_details.log
    }
    Chart.update(ioChart, ioStats)
  }

  if(response[0].cpu_user_details !== undefined) {
    const cpuStats = {
      user_time_details: response[0].cpu_user_details.log.slice(-1)[0] * 100,
      system_time_details: response[0].cpu_sys_details.log.slice(-1)[0] * 100,
      user_time_details_log: response[0].cpu_user_details.log.map(x => x * 100),
      system_time_details_log: response[0].cpu_sys_details.log.map(x => x * 100)
    }
    Chart.update(cpuChart, cpuStats, "origin")
  }

  if(response[0].connection_created_details !== undefined) {
    const connectionChurnStats = {
      connection_created_details: response[0].connection_created_details.rate,
      connection_closed_details: response[0].connection_closed_details.rate,
      connection_created_details_log: response[0].connection_created_details.log,
      connection_closed_details_log: response[0].connection_closed_details.log
    }
    Chart.update(connectionChurnChart, connectionChurnStats)
  }
  if(response[0].channel_created_details !== undefined) {
    const channelChurnStats = {
      channel_created_details: response[0].channel_created_details.rate,
      channel_closed_details: response[0].channel_closed_details.rate,
      channel_created_details_log: response[0].channel_created_details.log,
      channel_closed_details_log: response[0].channel_closed_details.log
    }
    Chart.update(channelChurnChart, channelChurnStats)
  }
  if(response[0].queue_declared_details !== undefined) {
    const queueChurnStats = {
      queue_declared_details: response[0].queue_declared_details.rate,
      queue_deleted_details: response[0].queue_deleted_details.rate,
      queue_declared_details_log: response[0].queue_declared_details.log,
      queue_deleted_details_log: response[0].queue_deleted_details.log
    }
    Chart.update(queueChurnChart, queueChurnStats)
  }
  followersDataSource.update(response[0].followers)
}

function humanizeBytes(bytes, si=false, dp=1) {
  const thresh = si ? 1000 : 1024;

  if (Math.abs(bytes) < thresh) {
    return bytes + ' B';
  }

  const units = si 
    ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] 
    : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  let u = -1;
  const r = 10**dp;

  do {
    bytes /= thresh;
    ++u;
  } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);


  return bytes.toFixed(dp) + ' ' + units[u];
}

start(updateCharts)
