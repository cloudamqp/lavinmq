import * as HTTP from './http.js'
import * as Chart from './chart.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
import { DataSource } from './datasource.js'

const numFormatter = new Intl.NumberFormat()
let url = 'api/nodes'
const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += HTTP.url`?vhost=${vhost}`
}

function update (cb) {
  HTTP.request('GET', url).then((response) => {
    render(response)
    if (cb) {
      cb(response)
    }
  })
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
  setInterval(update, 5000, cb)
}

const gcStatsFields = [
  { heading: 'GC cycles', key: 'gc_no', info: 'Garbage collection cycle number. The value may wrap.' },
  { heading: 'Heap size', key: 'heap_size', bytes: true, info: 'Heap size in bytes (including the area unmapped to OS).' },
  { heading: 'Free bytes', key: 'free_bytes', bytes: true, info: 'Total bytes contained in free and unmapped blocks.' },
  { heading: 'Unmapped bytes', key: 'unmapped_bytes', bytes: true, info: 'Amount of memory unmapped to the OS.' },
  { heading: 'Allocated since last GC', key: 'bytes_since_gc', bytes: true, info: 'Number of bytes allocated since the recent collection.' },
  { heading: 'Allocated before last GC', key: 'bytes_before_gc', bytes: true, info: 'Number of bytes allocated before the recent garbage collection. The value may wrap.' },
  { heading: 'Non GC bytes', key: 'non_gc_bytes', bytes: true, info: 'Number of bytes not considered candidates for garbage collection.' },
  { heading: 'Marker threads', key: 'markers_m1', info: 'Number of marker threads (excluding the initiating one), or 0 if single-threaded.' },
  { heading: 'Reclaimed since last GC', key: 'bytes_reclaimed_since_gc', bytes: true, info: 'Approximate number of reclaimed bytes after the recent garbage collection.' },
  { heading: 'Reclaimed before last GC', key: 'reclaimed_bytes_before_gc', bytes: true, info: 'Approximate number of bytes reclaimed before the recent garbage collection. The value may wrap.' },
  { heading: 'Explicitly freed since last GC', key: 'expl_freed_bytes_since_gc', bytes: true, info: 'Number of bytes freed explicitly since the recent garbage collection.' },
  { heading: 'Obtained from OS', key: 'obtained_from_os_bytes', bytes: true, info: 'Total amount of memory obtained from the OS, in bytes.' }
]

const renderGCStats = (gc) => {
  const table = document.getElementById('gc-stats-table')
  if (!table || gc === undefined) return
  while (table.firstChild) {
    table.firstChild.remove()
  }
  for (const field of gcStatsFields) {
    const value = gc[field.key]
    if (value === undefined) continue
    const row = document.createElement('tr')
    const th = document.createElement('th')
    const tooltip = document.createElement('a')
    tooltip.className = 'prop-tooltip'
    tooltip.append(document.createTextNode(field.heading))
    const icon = document.createElement('span')
    icon.className = 'tooltip-icon'
    icon.textContent = '?'
    const text = document.createElement('span')
    text.className = 'prop-tooltiptext'
    text.textContent = field.info
    tooltip.append(icon, text)
    th.append(tooltip)
    const td = document.createElement('td')
    td.textContent = field.bytes ? humanizeBytes(value) : numFormatter.format(value)
    row.append(th, td)
    table.append(row)
  }
}

const refreshGCStats = () => {
  return HTTP.request('GET', 'api/nodes/gc_stats').then(renderGCStats)
}

const gcRefreshBtn = document.getElementById('gc-refresh-btn')
if (gcRefreshBtn) {
  gcRefreshBtn.addEventListener('click', () => {
    gcRefreshBtn.disabled = true
    refreshGCStats().finally(() => { gcRefreshBtn.disabled = false })
  })
}

const gcBtn = document.getElementById('gc-btn')
if (gcBtn) {
  gcBtn.addEventListener('click', () => {
    gcBtn.disabled = true
    HTTP.request('POST', 'api/nodes/gc_collect')
      .then(() => {
        DOM.toast('Garbage collection triggered')
        return refreshGCStats()
      })
      .finally(() => { gcBtn.disabled = false })
  })
  // Only admins may read GC stats; the gc-btn element is in the DOM for all
  // users (require-administrator only hides it via CSS), so fetching here
  // unconditionally would 403 for non-admins and pop up an error. Gate the
  // initial load on the (persisted, synchronously applied) admin role class;
  // afterwards fetch only on demand.
  if (Helpers.stateClasses.has('user-is-administrator')) {
    refreshGCStats()
  }
}

const updateDetails = (nodeStats) => {
  document.getElementById('tr-name').textContent = nodeStats.name
  document.getElementById('tr-uptime').textContent = Helpers.duration((nodeStats.uptime / 1000).toFixed(0))
  document.getElementById('tr-vcpu').textContent = nodeStats.processors || 'N/A'
  let memUsage, cpuUsage, diskUsage

  if (nodeStats.mem_used !== undefined) {
    const memUsedGb = (nodeStats.mem_used / 1024 ** 3).toFixed(3)
    const memLimitGb = (nodeStats.mem_limit / 1024 ** 3).toFixed(3)
    const memPcnt = (nodeStats.mem_used / nodeStats.mem_limit * 100).toFixed(2)
    memUsage = `${memUsedGb}/${memLimitGb} GiB (${memPcnt}%)`
  } else {
    memUsage = 'N/A'
  }
  document.getElementById('tr-memory').textContent = memUsage
  if (nodeStats.cpu_user_time !== undefined) {
    const cpuPcnt = (((nodeStats.cpu_user_time + nodeStats.cpu_sys_time) / nodeStats.uptime) * 100).toFixed(2)
    cpuUsage = `${cpuPcnt}%`
  } else {
    cpuUsage = 'N/A'
  }
  document.getElementById('tr-cpu').textContent = cpuUsage
  if (nodeStats.disk_total !== undefined) {
    const diskUsageGb = ((nodeStats.disk_total - nodeStats.disk_free) / 1024 ** 3).toFixed(3)
    const diskTotalGb = (nodeStats.disk_total / 1024 ** 3).toFixed(0)
    const diskPcnt = ((nodeStats.disk_total - nodeStats.disk_free) / nodeStats.disk_total * 100).toFixed(2)
    diskUsage = `${diskUsageGb}/${diskTotalGb} GiB (${diskPcnt}%)`
  } else {
    diskUsage = 'N/A'
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
        heading: 'Unacknowledged',
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
      if (nodeStats[items.key] !== undefined) {
        const td = document.createElement('td')
        td.textContent = items.heading + ': ' + numFormatter.format(nodeStats[items.key])
        row.append(td)
        metrics += 1
      }
    }
    if (metrics > 0) {
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
  constructor () { super({ autoReloadTimeout: 0, useQueryState: false }) }
  update (items) { this.items = items }
  reload () { }
})()
const followersTableOpts = {
  dataSource: followersDataSource,
  keyColumns: ['id'],
  countId: 'followers-count'
}
Table.renderTable('followers', followersTableOpts, (tr, item, firstRender) => {
  if (firstRender) {
    Table.renderCell(tr, 0, item.id)
  }
  Table.renderCell(tr, 1, item.remote_address)
  Table.renderCell(tr, 2, humanizeBytes(item.sent_bytes), 'right')
  Table.renderCell(tr, 3, humanizeBytes(item.acked_bytes), 'right')
  Table.renderCell(tr, 4, humanizeBytes(item.sent_bytes - item.acked_bytes), 'right')
})

function updateCharts (response) {
  if (response[0].mem_used !== undefined) {
    const memoryStats = {
      mem_used_details: toMegaBytes(response[0].mem_used),
      mem_used_details_log: response[0].mem_used_details.log.map(toMegaBytes)
    }
    Chart.update(memoryChart, memoryStats)
  }
  if (response[0].io_write_details !== undefined) {
    const ioStats = {
      io_write_details: response[0].io_write_details.log.slice(-1)[0],
      io_write_details_log: response[0].io_write_details.log,
      io_read_details: response[0].io_read_details.log.slice(-1)[0],
      io_read_details_log: response[0].io_read_details.log
    }
    Chart.update(ioChart, ioStats)
  }

  if (response[0].cpu_user_details !== undefined) {
    const cpuStats = {
      user_time_details: response[0].cpu_user_details.log.slice(-1)[0] * 100,
      system_time_details: response[0].cpu_sys_details.log.slice(-1)[0] * 100,
      user_time_details_log: response[0].cpu_user_details.log.map(x => x * 100),
      system_time_details_log: response[0].cpu_sys_details.log.map(x => x * 100)
    }
    Chart.update(cpuChart, cpuStats, 'origin')
  }

  if (response[0].connection_created_details !== undefined) {
    const connectionChurnStats = {
      connection_created_details: response[0].connection_created_details.rate,
      connection_closed_details: response[0].connection_closed_details.rate,
      connection_created_details_log: response[0].connection_created_details.log,
      connection_closed_details_log: response[0].connection_closed_details.log
    }
    Chart.update(connectionChurnChart, connectionChurnStats)
  }
  if (response[0].channel_created_details !== undefined) {
    const channelChurnStats = {
      channel_created_details: response[0].channel_created_details.rate,
      channel_closed_details: response[0].channel_closed_details.rate,
      channel_created_details_log: response[0].channel_created_details.log,
      channel_closed_details_log: response[0].channel_closed_details.log
    }
    Chart.update(channelChurnChart, channelChurnStats)
  }
  if (response[0].queue_declared_details !== undefined) {
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

function humanizeBytes (bytes, si = false, dp = 1) {
  const thresh = si ? 1000 : 1024

  if (Math.abs(bytes) < thresh) {
    return bytes + ' B'
  }

  const units = si
    ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
  let u = -1
  const r = 10 ** dp

  do {
    bytes /= thresh
    ++u
  } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1)

  return bytes.toFixed(dp) + ' ' + units[u]
}

start(updateCharts)
