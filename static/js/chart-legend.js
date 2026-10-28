import * as helpers from './helpers.js'
import * as Poller from './poller.js'

function statsKey (key) {
  return key.replace(/_details$|_log$/, '')
}

function formatLabel (handle, key) {
  if (handle.labels[key]) return handle.labels[key]
  const label = statsKey(key).replace(/_/g, ' ').replace(/(rate|messages)/ig, '').trim()
    .replace(/^\w/, c => c.toUpperCase())
  return label || 'Total'
}

const SERIES_DESCRIPTIONS = {
  publish: 'Messages published into exchanges by clients',
  confirm: 'Publisher confirms, broker acks that publishes were accepted',
  deliver: 'Messages pushed to consumers (ack required)',
  deliver_no_ack: 'Messages pushed to consumers (no-ack mode)',
  get: 'Messages pulled via basic.get (ack required)',
  get_no_ack: 'Messages pulled via basic.get (no-ack mode)',
  deliver_get: 'Total outbound, all deliver and get variants including no-ack',
  ack: 'Consumer acknowledgments of delivered messages',
  redeliver: 'Messages redelivered (consumer disconnect or requeue)',
  reject: 'Messages rejected or nacked by consumers',
  return_unroutable: 'Publishes with no matching queue, returned to publisher',
  dedup: 'Duplicate publishes dropped by dedup',
  send: 'Outbound bytes to clients',
  receive: 'Inbound bytes from clients'
}

function describeSeries (handle, key) {
  return handle.descs[key] || SERIES_DESCRIPTIONS[statsKey(key)] || ''
}

const timeFormatter = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' })

function fmtTimestamp (v) {
  if (v == null) return '--'
  return timeFormatter.format(new Date(v * 1000))
}

const preciseFormatter = new Intl.NumberFormat('en', { minimumFractionDigits: 1, maximumFractionDigits: 1, useGrouping: true })

const exactFormatter = new Intl.NumberFormat('en', { maximumFractionDigits: 20, useGrouping: true })

function isByteUnit (unit) {
  return unit === 'bytes' || unit === 'bytes/s'
}

function formatDigits (v) {
  return Number.isInteger(v) ? exactFormatter.format(v) : preciseFormatter.format(v)
}

function formatLegendValue (v, unit, byteIdx) {
  if (v == null) return '--'
  if (isByteUnit(unit)) {
    const { value, prefix } = helpers.scaleBytes(v, byteIdx)
    const suffix = unit === 'bytes/s' ? '/s' : ''
    const digits = value !== 0 && Math.abs(value) < 1 ? value.toFixed(2) : formatDigits(value)
    return digits + ' ' + prefix + suffix
  }
  const digits = formatDigits(v)
  if (!unit) return digits
  const sep = (unit === '%' || unit.startsWith('/')) ? '' : ' '
  return digits + sep + unit
}

function formatLegendTitle (v, unit) {
  if (v == null) return ''
  const digits = isByteUnit(unit)
    ? Math.round(v).toLocaleString('en')
    : exactFormatter.format(v)
  return unit ? digits + ' ' + unit : digits
}

function buildLegend (handle) {
  const el = document.createElement('div')
  el.className = 'u-legend-custom'

  const timeSpan = document.createElement('span')
  timeSpan.className = 'u-legend-time'
  timeSpan.textContent = '\u00a0'

  const card = handle.el.closest('.card') || handle.el.parentElement
  const heading = card && card.querySelector('h3')
  if (heading) {
    heading.classList.add('chart-heading')
    heading.append(timeSpan)
  } else {
    el.prepend(timeSpan)
  }

  const table = document.createElement('table')
  table.className = 'u-legend-table'
  const container = document.createElement('tbody')
  table.append(container)
  el.append(table)

  handle.legendEl = el
  handle.legendGrid = container
  handle.legendTime = timeSpan
  handle.legendItems = []
}

function addLegendItem (handle, seriesIdx, color) {
  const label = formatLabel(handle, handle.seriesKeys[seriesIdx - 1])

  const item = document.createElement('tr')
  item.className = 'u-legend-item'
  item.style.cursor = 'pointer'
  const desc = describeSeries(handle, handle.seriesKeys[seriesIdx - 1])
  if (desc) item.title = desc

  const marker = document.createElement('span')
  marker.className = 'u-legend-marker'
  marker.style.background = color

  const labelNode = document.createElement('td')
  labelNode.className = 'u-legend-label'
  labelNode.textContent = label

  const valueNode = document.createElement('td')
  valueNode.className = 'u-legend-value'
  const valueText = document.createElement('span')
  valueText.textContent = '--'
  valueNode.append(valueText)

  const markerCell = document.createElement('td')
  markerCell.className = 'u-legend-marker-cell'
  markerCell.append(marker)
  item.append(markerCell, labelNode, valueNode)

  item.addEventListener('click', (e) => {
    if (!handle.uplot) return
    const series = handle.uplot.series
    if (e.shiftKey || e.metaKey || e.ctrlKey) {
      const show = !series[seriesIdx].show
      handle.uplot.setSeries(seriesIdx, { show })
      item.classList.toggle('u-legend-hidden', !show)
      return
    }
    const othersHidden = series.every((s, i) => i === 0 || i === seriesIdx || !s.show)
    const isolated = series[seriesIdx].show && othersHidden
    for (let i = 1; i < series.length; i++) {
      const show = isolated ? true : i === seriesIdx
      handle.uplot.setSeries(i, { show })
      const li = handle.legendItems[i]
      if (li) li.item.classList.toggle('u-legend-hidden', !show)
    }
  })
  item.addEventListener('mouseenter', () => {
    if (handle.uplot) handle.uplot.setSeries(seriesIdx, { focus: true })
  })
  item.addEventListener('mouseleave', () => {
    if (handle.uplot) handle.uplot.setSeries(null, { focus: true })
  })

  handle.legendGrid.append(item)
  handle.legendItems[seriesIdx] = { valueSpan: valueText, item }
}

function updateLegend (handle, idx) {
  if (!handle.legendEl) return
  const data = handle.data
  const resolveIdx = idx != null ? idx : (data[0].length > 0 ? data[0].length - 1 : null)

  if (resolveIdx == null) {
    handle.legendTime.textContent = '\u00a0'
  } else if (idx == null && Poller.isPaused()) {
    handle.legendTime.textContent = 'Paused at ' + fmtTimestamp(data[0][resolveIdx])
  } else {
    handle.legendTime.textContent = fmtTimestamp(data[0][resolveIdx])
  }

  let byteIdx = null
  if (isByteUnit(handle.config.unit) && resolveIdx != null) {
    let maxV = 0
    for (let i = 1; i <= handle.seriesKeys.length; i++) {
      const v = data[i] ? data[i][resolveIdx] : null
      if (v != null) maxV = Math.max(maxV, Math.abs(v))
    }
    byteIdx = helpers.byteUnitIndex(maxV)
  }

  let longest = 0
  for (let i = 1; i <= handle.seriesKeys.length; i++) {
    const li = handle.legendItems[i]
    if (!li) continue
    const v = resolveIdx != null && data[i] ? data[i][resolveIdx] : null
    const text = formatLegendValue(v, handle.config.unit, byteIdx)
    li.valueSpan.textContent = text
    li.valueSpan.title = formatLegendTitle(v, handle.config.unit)
    longest = Math.max(longest, text.length)
  }
  holdValueWidth(handle, longest)
}

// The value column sizes to its values but never shrinks, so hovering or a
// quieter poll doesn't move the labels. Only a value longer than any so far
// costs a layout read.
function holdValueWidth (handle, longest) {
  if (!handle.legendEl.isConnected || longest <= (handle.valueChars || 0)) return
  handle.valueChars = longest
  let width = handle.valueWidth || 0
  for (const li of handle.legendItems) {
    if (li) width = Math.max(width, li.valueSpan.getBoundingClientRect().width)
  }
  handle.valueWidth = width
  handle.legendEl.style.setProperty('--legend-value-width', width + 'px')
}

// Drop the held width so the next update measures afresh, e.g. once the web
// font has replaced the fallback font the first measurement used.
function resetValueWidth (handle) {
  handle.valueChars = 0
  handle.valueWidth = 0
  if (handle.legendEl) handle.legendEl.style.removeProperty('--legend-value-width')
}

function alignLegend (handle) {
  if (!handle.uplot || !handle.legendEl) return
  const plotLeft = Math.round(handle.uplot.bbox.left / window.devicePixelRatio)
  if (plotLeft !== handle.legendPad) {
    handle.legendPad = plotLeft
    handle.legendEl.style.paddingLeft = plotLeft + 'px'
  }
}

export {
  buildLegend, addLegendItem, updateLegend, alignLegend, resetValueWidth,
  formatLabel, formatDigits, isByteUnit, timeFormatter
}
