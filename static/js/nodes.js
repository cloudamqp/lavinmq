import * as HTTP from './http.js'
import * as Chart from './chart.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
import { DataSource } from './datasource.js'
import * as Poller from './poller.js'

const numFormatter = new Intl.NumberFormat()
let url = 'api/nodes'
const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += HTTP.url`?vhost=${vhost}`
}

function update (cb, signal) {
  return HTTP.request('GET', url, { signal }).then((response) => {
    render(response)
    if (cb) {
      cb(response)
    }
  }).catch(() => {})
}

function render (data) {
  document.querySelector('#version').textContent = data[0].applications[0].version
  for (const node of data) {
    updateDetails(node)
    updateStats(node)
  }
}

function start (cb) {
  Poller.start(signal => update(cb, signal))
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
  DOM.wireTooltipAnchors(table)
}

const refreshGCStats = () => {
  return HTTP.request('GET', 'api/nodes/gc_stats')
    .then(renderGCStats)
    .catch(() => {})
}

const gcRefreshBtn = document.getElementById('gc-refresh-btn')
if (gcRefreshBtn) {
  gcRefreshBtn.addEventListener('click', () => {
    gcRefreshBtn.disabled = true
    refreshGCStats()
      .finally(() => { gcRefreshBtn.disabled = false })
      .catch(() => {})
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
      .catch(() => {})
  })
  // Only admins may read GC stats; the gc-btn element is in the DOM for all
  // users (require-administrator only hides it via CSS), so fetching here
  // unconditionally would 403 for non-admins and pop up an error. Gate the
  // initial load on the (persisted, synchronously applied) admin role class;
  // afterwards fetch only on demand.
  if (Helpers.stateClasses.has('user-is-administrator')) {
    refreshGCStats().catch(() => {})
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
  constructor () { super({ autoReload: false, useQueryState: false }) }
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
  Table.renderCell(tr, 4, memberLag(item), 'right')
})

let followersById = {}
// Sequence number of the leader's latest replicated change, from /api/nodes
let journalLsn = null

// A member's replication lag, in journal entries when both sides report
// their position and in bytes otherwise
function memberLag (follower) {
  if (!follower) return ''
  const bytes = humanizeBytes(follower.sent_bytes - follower.acked_bytes)
  if (journalLsn == null || follower.applied_lsn == null) return bytes
  const entries = Math.max(0, journalLsn - follower.applied_lsn)
  return `${numFormatter.format(entries)} entries, ${bytes}`
}

// The reason a member can't be promoted or made leader yet, or null when it can
function notReadyReason (item) {
  if (!item.in_isr) return 'Not in the in-sync replica set yet'
  if (!item.caught_up) return 'Not caught up with the leader yet'
  return null
}

function readinessButton (item, text, click) {
  const btn = DOM.button.edit({ text, click })
  const reason = notReadyReason(item)
  if (reason) {
    btn.disabled = true
    btn.title = reason
  }
  return btn
}

// Cluster members, only with the raft clustering backend. The cluster API
// answers 400/404 otherwise, so use fetch directly instead of HTTP.request
// to not alert about that.
const clusterDataSource = new (class extends DataSource {
  constructor () { super({ autoReloadTimeout: 0, useQueryState: false }) }
  update (items) { this.items = items }
  reload () { }
})()
const clusterTableOpts = {
  dataSource: clusterDataSource,
  keyColumns: ['node_id'],
  countId: 'cluster-members-count'
}
const clusterRequest = (method, path, body) => {
  return HTTP.request(method, path, body ? { body } : {}).then((r) => { updateCluster(); return r })
}
Table.renderTable('cluster-members', clusterTableOpts, (tr, item, firstRender) => {
  Table.renderCell(tr, 0, item.leader ? item.address + ' (leader)' : item.address)
  Table.renderCell(tr, 1, item.node_id || '')
  Table.renderCell(tr, 2, item.role)
  Table.renderCell(tr, 3, item.in_isr ? '●' : '○')
  Table.renderCell(tr, 4, item.match_index == null ? '' : numFormatter.format(item.match_index), 'right')
  Table.renderCell(tr, 5, item.caught_up ? '●' : '○')
  Table.renderCell(tr, 6, memberLag(followersById[item.node_id]), 'right')
  const buttons = document.createElement('div')
  buttons.classList.add('buttons', 'require-administrator')
  if (item.role === 'learner') {
    // The leader refuses to promote a learner that isn't in sync and caught
    // up, so the button says why instead of failing
    buttons.append(readinessButton(item, 'Promote', () => {
      clusterRequest('POST', HTTP.url`api/cluster/members/${item.node_id}/promote`)
        .then(() => DOM.toast(`Promoted ${item.address}`)).catch(() => {})
    }))
  } else if (!item.leader) {
    buttons.append(readinessButton(item, 'Make leader', () => {
      if (!window.confirm(`Hand over leadership to ${item.address}? The current leader stops serving and continues as a follower.`)) return
      clusterRequest('POST', 'api/cluster/transfer-leadership', { target: item.node_id })
        .then(() => DOM.toast(`Handing over leadership to ${item.address}`)).catch(() => {})
    }))
  }
  if (!item.leader) {
    buttons.append(DOM.button.delete({
      text: 'Remove',
      click: () => {
        if (!window.confirm(`Remove ${item.address} from the cluster? Shut the node down afterwards.`)) return
        clusterRequest('DELETE', HTTP.url`api/cluster/members/${item.node_id}`)
          .then(() => DOM.toast(`Removed ${item.address}`)).catch(() => {})
      }
    }))
  }
  Table.renderCell(tr, 7, buttons, 'right')
})

// The cluster members table already covers the followers with the raft backend
function updateFollowersVisibility () {
  const hasFollowers = (followersDataSource.items || []).length > 0
  document.getElementById('followers-section').hidden = !hasFollowers || !document.getElementById('cluster-section').hidden
}

const CLUSTER_POLL_MS = 5000
// Without a raft cluster the API answers 400/404; check again now and then
// in case the node is restarted with one, rather than needing a reload
const CLUSTER_RECHECK_MS = 60000
let clusterPoll = null
let clusterPollMs = 0

function pollCluster (ms) {
  if (clusterPollMs === ms) return
  clearInterval(clusterPoll)
  clusterPollMs = ms
  clusterPoll = ms ? setInterval(updateCluster, ms) : null
}

function clusterSummary (cluster) {
  const isr = (cluster.isr || []).join(', ')
  if (!cluster.leader) return `No leader elected (term ${cluster.term}). In-sync replicas: ${isr}`
  return `Leader ${cluster.leader}, term ${cluster.term}. In-sync replicas: ${isr}`
}

function updateCluster () {
  return window.fetch('api/cluster').then((response) => {
    const section = document.getElementById('cluster-section')
    if (!response.ok) {
      if ([400, 404].includes(response.status)) {
        // No raft cluster
        section.hidden = true
        updateFollowersVisibility()
        pollCluster(CLUSTER_RECHECK_MS)
      } else if (response.status === 403) {
        // Not an administrator, which won't change
        section.hidden = true
        updateFollowersVisibility()
        pollCluster(0)
      }
      // Otherwise (409 not the leader, 503 no answer) it is transient, e.g.
      // during a leadership change, so keep showing the last known members
      return
    }
    return response.json().then((cluster) => {
      if (!Array.isArray(cluster.members)) {
        section.hidden = true
        updateFollowersVisibility()
        return
      }
      section.hidden = false
      updateFollowersVisibility()
      pollCluster(CLUSTER_POLL_MS)
      document.getElementById('cluster-summary').textContent = clusterSummary(cluster)
      clusterDataSource.update(cluster.members)
    })
  }).catch(() => {})
}

document.getElementById('transferLeadership').addEventListener('click', () => {
  if (!window.confirm('Hand over leadership to another in-sync voter? The current leader stops serving and continues as a follower.')) return
  clusterRequest('POST', 'api/cluster/transfer-leadership', {})
    .then((r) => DOM.toast(r && r.target ? `Handing over leadership to ${r.target}` : 'Handing over leadership'))
    .catch(() => {})
})

document.getElementById('addClusterMember').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const address = new window.FormData(this).get('address').trim()
  clusterRequest('POST', 'api/cluster/members', { address })
    .then(() => {
      DOM.toast(`Added ${address} as learner`)
      this.reset()
    }).catch(() => {})
})

updateCluster()
pollCluster(CLUSTER_POLL_MS)

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
  const followers = response[0].followers || []
  followersById = Object.fromEntries(followers.map((f) => [f.id, f]))
  journalLsn = response[0].journal_lsn == null ? null : response[0].journal_lsn
  followersDataSource.update(followers)
  updateFollowersVisibility()
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
