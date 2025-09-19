import * as Table from './table.js'

class LiveLogDataSource {
  constructor (streamUrl) {
    this.items = []
    this.totalCount = 0
    this.page = 1
    this.searchTerm = ''
    this.sortKey = null
    this.reverseOrder = false
    this._debounceTimer = null
    this._pendingCount = 0
    this._lastRender = 0
    this._DEBOUNCE_MS = 100
    this._FORCE_BATCH = 200
    this._MAX_LATENCY_MS = 500

    // Internal state:
    this.allLogs = []
    this._event = new EventTarget()

    // Event Source
    this._evtSource = new window.EventSource(streamUrl)
    this._evtSource.onmessage = (event) => {
    const timestamp = new Date(parseInt(event.lastEventId, 10))
    const [severity, source, message] = JSON.parse(event.data)
    this.pushLog({ timestamp, severity, source, message })
  }

    this._evtSource.onerror = () => {
      window.fetch('api/whoami')
        .then(response => response.json())
        .then(whoami => {
          if (!whoami.tags.includes('administrator')) {
            forbidden()
          }
        })
    }

    function forbidden () {
      const tblError = document.getElementById('table-error')
      tblError.textContent = 'Access denied, administator access required'
      tblError.style.display = 'block'
    }
}

  on (eventName, handler) {
    this._event.addEventListener(eventName, handler)
  }

  close () {
    this._evtSource?.close()
  }

  reload () {
    let regex = null
    if (this.searchTerm) {
      try {
        regex = new RegExp(this.searchTerm, 'i')
      } catch (e) {
        regex = null
      }
    }
    const visible = regex
      ? this.allLogs.filter(log => regex.test(joinFieldsForSearch(log)))
      : this.allLogs.slice()

    if (this.sortKey) {
      const direction = this.reverseOrder ? -1 : 1
      visible.sort((a, b) => compareValues(a[this.sortKey], b[this.sortKey], direction))
    }

    this.items = visible
    this.totalCount = visible.length
    this._event.dispatchEvent(new CustomEvent('update'))
  }

  pushLog (log) {
    this.allLogs.push(log)
    this._pendingCount++

    const now = Date.now()
    const tooLong = (now - this._lastRender) > this._MAX_LATENCY_MS

    if (this._pendingCount >= this._FORCE_BATCH || tooLong) {
      clearTimeout(this._debounceTimer)
      this._pendingCount = 0
      this._lastRender = now
      this.reload()
      return
    }

    clearTimeout(this._debounceTimer)
    this._debounceTimer = setTimeout(() => {
      this._pendingCount = 0
      this.reload()
    }, this._DEBOUNCE_MS)
  }
}

// Let the filter regex match anywhere in the row.
function joinFieldsForSearch (log) {
  const isoTime = (log.timestamp).toISOString()
  return `${isoTime} ${log.severity} ${log.source} ${log.message ?? log.msg ?? ''}`
}

function compareValues (a, b, direction) {
  const toNumber = (v) =>
    v instanceof Date
      ? v.getTime()
      : (typeof v === 'number'
          ? v
          : (typeof v === 'string' && !Number.isNaN(+v)
              ? +v
              : null))

  const aNum = toNumber(a)
  const bNum = toNumber(b)

  if (aNum !== null && bNum !== null) {
    return (aNum - bNum) * direction
  }

  const aStr = String(a ?? '').toLowerCase()
  const bStr = String(b ?? '').toLowerCase()
  return aStr.localeCompare(bStr) * direction
}

// Build table dynamically using table.js
const logsDataSource = new LiveLogDataSource('api/livelog')

const tableOptions = {
  dataSource: logsDataSource,
  keyColumns: ['timestamp'],
  pagination: false,
  columnSelector: true, // TODO: improve position/placement
  search: true,
  countId: 'pagename-label'
}

const logsTable = Table.renderTable('table', tableOptions, (tr, item) => {
  Table.renderCell(tr, 0, (item.timestamp).toLocaleString())
  Table.renderCell(tr, 1, item.severity)
  Table.renderCell(tr, 2, item.source)
  const pre = document.createElement('pre')
  pre.textContent = item.message
  Table.renderCell(tr, 3, pre)
})

// TODO: improve scroll fx but works for now
let shouldAutoScroll = true
const livelog = document.getElementById('livelog')
livelog.addEventListener('scroll', () => {
  const nearBottom = (livelog.scrollHeight - livelog.clientHeight - livelog.scrollTop < 3)
  shouldAutoScroll = nearBottom
})

logsTable.on('updated', () => {
  if (!shouldAutoScroll) return
  livelog.scrollTop = livelog.scrollHeight
})

window.addEventListener('beforeunload', () => logsDataSource.close())
