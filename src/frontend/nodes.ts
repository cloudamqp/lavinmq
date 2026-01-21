import * as HTTP from './http.js'
import * as Chart from './chart.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import { DataSource, type PaginatedResponse } from './datasource.js'

interface Application {
  version: string
}

interface DetailsLog {
  rate: number
  log: number[]
}

interface FollowerItem {
  id: string
  remote_address: string
  sent_bytes: number
  acked_bytes: number
}

interface NodeStats {
  name: string
  uptime: number
  processors?: number
  mem_used?: number
  mem_limit?: number
  cpu_user_time?: number
  cpu_sys_time?: number
  disk_total?: number
  disk_free?: number
  applications: Application[]
  connection_created?: number
  connection_closed?: number
  channel_created?: number
  channel_closed?: number
  queue_declared?: number
  queue_deleted?: number
  fd_used?: number
  fd_total?: number
  messages_ready?: number
  messages_unacknowledged?: number
  mem_used_details?: DetailsLog
  io_write_details?: DetailsLog
  io_read_details?: DetailsLog
  cpu_user_details?: DetailsLog
  cpu_sys_details?: DetailsLog
  connection_created_details?: DetailsLog
  connection_closed_details?: DetailsLog
  channel_created_details?: DetailsLog
  channel_closed_details?: DetailsLog
  queue_declared_details?: DetailsLog
  queue_deleted_details?: DetailsLog
  followers: FollowerItem[]
}

const numFormatter = new Intl.NumberFormat()
let url = 'api/nodes'
const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += HTTP.url`?vhost=${vhost}`
}

function update(cb?: (response: NodeStats[]) => void): void {
  HTTP.request<NodeStats[]>('GET', url).then((response) => {
    if (!response) return
    render(response)
    if (cb) {
      cb(response)
    }
  })
}

function render(data: NodeStats[]): void {
  const versionEl = document.querySelector('#version')
  if (versionEl && data[0]) versionEl.textContent = data[0].applications[0]?.version ?? ''
  for (const node of data) {
    updateDetails(node)
    updateStats(node)
  }
}

function start(cb: (response: NodeStats[]) => void): void {
  update(cb)
  setInterval(update, 5000, cb)
}

const updateDetails = (nodeStats: NodeStats): void => {
  const setTextById = (id: string, text: string): void => {
    const el = document.getElementById(id)
    if (el) el.textContent = text
  }

  setTextById('tr-name', nodeStats.name)
  setTextById('tr-uptime', Helpers.duration(parseInt((nodeStats.uptime / 1000).toFixed(0), 10)))
  setTextById('tr-vcpu', nodeStats.processors !== undefined ? String(nodeStats.processors) : 'N/A')
  let memUsage: string, cpuUsage: string, diskUsage: string

  if (nodeStats.mem_used !== undefined && nodeStats.mem_limit !== undefined) {
    const memUsedGb = (nodeStats.mem_used / 1024 ** 3).toFixed(3)
    const memLimitGb = (nodeStats.mem_limit / 1024 ** 3).toFixed(3)
    const memPcnt = ((nodeStats.mem_used / nodeStats.mem_limit) * 100).toFixed(2)
    memUsage = `${memUsedGb}/${memLimitGb} GiB (${memPcnt}%)`
  } else {
    memUsage = 'N/A'
  }
  setTextById('tr-memory', memUsage)
  if (nodeStats.cpu_user_time !== undefined && nodeStats.cpu_sys_time !== undefined) {
    const cpuPcnt = (((nodeStats.cpu_user_time + nodeStats.cpu_sys_time) / nodeStats.uptime) * 100).toFixed(2)
    cpuUsage = `${cpuPcnt}%`
  } else {
    cpuUsage = 'N/A'
  }
  setTextById('tr-cpu', cpuUsage)
  if (nodeStats.disk_total !== undefined && nodeStats.disk_free !== undefined) {
    const diskUsageGb = ((nodeStats.disk_total - nodeStats.disk_free) / 1024 ** 3).toFixed(3)
    const diskTotalGb = (nodeStats.disk_total / 1024 ** 3).toFixed(0)
    const diskPcnt = (((nodeStats.disk_total - nodeStats.disk_free) / nodeStats.disk_total) * 100).toFixed(2)
    diskUsage = `${diskUsageGb}/${diskTotalGb} GiB (${diskPcnt}%)`
  } else {
    diskUsage = 'N/A'
  }
  setTextById('tr-disk', diskUsage)
}

interface StatsRow {
  heading: string
  content: { heading: string; key: keyof NodeStats }[]
}

const stats: StatsRow[] = [
  {
    heading: 'Connection',
    content: [
      { heading: 'Created', key: 'connection_created' },
      { heading: 'Closed', key: 'connection_closed' },
    ],
  },
  {
    heading: 'Channels',
    content: [
      { heading: 'Created', key: 'channel_created' },
      { heading: 'Closed', key: 'channel_closed' },
    ],
  },
  {
    heading: 'Queues',
    content: [
      { heading: 'Declared', key: 'queue_declared' },
      { heading: 'Deleted', key: 'queue_deleted' },
    ],
  },
  {
    heading: 'File descriptors',
    content: [
      { heading: 'Used', key: 'fd_used' },
      { heading: 'Total', key: 'fd_total' },
    ],
  },
  {
    heading: 'Messages',
    content: [
      { heading: 'Ready', key: 'messages_ready' },
      { heading: 'Unacknowledged', key: 'messages_unacknowledged' },
    ],
  },
]

const updateStats = (nodeStats: NodeStats): void => {
  const table = document.getElementById('stats-table')
  if (!table) return
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
      const val = nodeStats[items.key]
      if (val !== undefined && typeof val === 'number') {
        const td = document.createElement('td')
        td.textContent = items.heading + ': ' + numFormatter.format(val)
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

const toMegaBytes = (dataPointInBytes: number): string => (dataPointInBytes / 1024 ** 2).toFixed(2)

class FollowersDataSource extends DataSource<FollowerItem> {
  constructor() {
    super({ autoReloadTimeout: 0, useQueryState: false })
  }
  update(items: FollowerItem[]): void {
    this.items = items
  }
  override reload(): Promise<PaginatedResponse<FollowerItem> | FollowerItem[] | void> {
    return Promise.resolve()
  }
}

const followersDataSource = new FollowersDataSource()
const followersTableOpts = {
  dataSource: followersDataSource,
  keyColumns: ['id'],
  countId: 'followers-count',
}
Table.renderTable<FollowerItem>('followers', followersTableOpts, (tr, item, firstRender) => {
  if (firstRender) {
    Table.renderCell(tr, 0, item.id)
  }
  Table.renderCell(tr, 1, item.remote_address)
  Table.renderCell(tr, 2, humanizeBytes(item.sent_bytes), 'right')
  Table.renderCell(tr, 3, humanizeBytes(item.acked_bytes), 'right')
  Table.renderCell(tr, 4, humanizeBytes(item.sent_bytes - item.acked_bytes), 'right')
})

function updateCharts(response: NodeStats[]): void {
  const node = response[0]
  if (!node) return
  if (node.mem_used !== undefined && node.mem_used_details) {
    const memoryStats = {
      mem_used_details: parseFloat(toMegaBytes(node.mem_used)),
      mem_used_details_log: node.mem_used_details.log.map((v) => parseFloat(toMegaBytes(v))),
    }
    Chart.update(memoryChart, memoryStats)
  }
  if (node.io_write_details !== undefined && node.io_read_details !== undefined) {
    const ioStats = {
      io_write_details: node.io_write_details.log.slice(-1)[0] ?? 0,
      io_write_details_log: node.io_write_details.log,
      io_read_details: node.io_read_details.log.slice(-1)[0] ?? 0,
      io_read_details_log: node.io_read_details.log,
    }
    Chart.update(ioChart, ioStats)
  }

  if (node.cpu_user_details !== undefined && node.cpu_sys_details !== undefined) {
    const cpuStats = {
      user_time_details: (node.cpu_user_details.log.slice(-1)[0] ?? 0) * 100,
      system_time_details: (node.cpu_sys_details.log.slice(-1)[0] ?? 0) * 100,
      user_time_details_log: node.cpu_user_details.log.map((x) => x * 100),
      system_time_details_log: node.cpu_sys_details.log.map((x) => x * 100),
    }
    Chart.update(cpuChart, cpuStats)
  }

  if (node.connection_created_details !== undefined && node.connection_closed_details !== undefined) {
    const connectionChurnStats = {
      connection_created_details: node.connection_created_details.rate,
      connection_closed_details: node.connection_closed_details.rate,
      connection_created_details_log: node.connection_created_details.log,
      connection_closed_details_log: node.connection_closed_details.log,
    }
    Chart.update(connectionChurnChart, connectionChurnStats)
  }
  if (node.channel_created_details !== undefined && node.channel_closed_details !== undefined) {
    const channelChurnStats = {
      channel_created_details: node.channel_created_details.rate,
      channel_closed_details: node.channel_closed_details.rate,
      channel_created_details_log: node.channel_created_details.log,
      channel_closed_details_log: node.channel_closed_details.log,
    }
    Chart.update(channelChurnChart, channelChurnStats)
  }
  if (node.queue_declared_details !== undefined && node.queue_deleted_details !== undefined) {
    const queueChurnStats = {
      queue_declared_details: node.queue_declared_details.rate,
      queue_deleted_details: node.queue_deleted_details.rate,
      queue_declared_details_log: node.queue_declared_details.log,
      queue_deleted_details_log: node.queue_deleted_details.log,
    }
    Chart.update(queueChurnChart, queueChurnStats)
  }
  followersDataSource.update(node.followers)
}

function humanizeBytes(bytes: number, si = false, dp = 1): string {
  const thresh = si ? 1000 : 1024

  if (Math.abs(bytes) < thresh) {
    return bytes + ' B'
  }

  const units = si
    ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
  let u = -1
  const r = 10 ** dp

  let b = bytes
  do {
    b /= thresh
    ++u
  } while (Math.round(Math.abs(b) * r) / r >= thresh && u < units.length - 1)

  return b.toFixed(dp) + ' ' + units[u]
}

start(updateCharts)
