/* global ResizeObserver MutationObserver */
import UPlot from './lib/uplot.esm.js'
import * as helpers from './helpers.js'
import * as Poller from './poller.js'
import { statsInterval } from './http.js'
import {
  buildLegend, addLegendItem, updateLegend, alignLegend,
  formatLabel, formatDigits, isByteUnit, timeFormatter
} from './chart-legend.js'

const chartColors = ['#54be7e', '#4589ff', '#d12771', '#d2a106', '#08bdba', '#bae6ff', '#ba4e00',
  '#d4bbff', '#8a3ffc', '#33b1ff', '#007d79', '#770f1c']

function seriesColor (seriesIdx) {
  return chartColors[(seriesIdx - 1) % chartColors.length]
}

const handles = new Set()
const cursorSync = UPlot.sync('lavinmq')

function refreshLegend (handle) {
  updateLegend(handle, handle.uplot ? handle.uplot.legend.idx : null)
}

Poller.events.addEventListener('change', () => {
  for (const h of handles) refreshLegend(h)
})

function toPoint (v) {
  if (v == null) return null
  if (typeof v === 'object') v = v.rate
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function makeSeriesDef (handle, key, color, filled) {
  return {
    label: formatLabel(handle, key),
    stroke: color,
    width: 1.5,
    points: { show: false },
    fill: filled ? color + '40' : undefined
  }
}

// Below this a narrow card leaves too little plot for more than one y tick.
const MIN_CHART_HEIGHT = 180

function chartHeight (width) {
  return Math.max(MIN_CHART_HEIGHT, Math.round(width / 2.5))
}

const SCALE_STEPS = [1, 1.5, 2, 3, 5, 7.5, 10]
const SHRINK_RATIO = 0.6
const SHRINK_POLLS = 5

function niceCeil (v) {
  const mag = 10 ** Math.floor(Math.log10(v))
  return SCALE_STEPS.find(s => v <= s * mag) * mag
}

function targetMax (handle, max) {
  if (handle.fixedMax > 0) {
    return max > handle.fixedMax ? niceCeil(max * 1.05) : handle.fixedMax
  }
  if (handle.scaleCap > 0 && max > handle.scaleCap / 2) return niceCeil(handle.scaleCap)
  return max > 0 ? niceCeil(max * 1.05) : 0
}

function stableMax (handle, target) {
  let scaleMax = handle.scaleMax || 0
  if (target > scaleMax) {
    scaleMax = target
    handle.shrinkCount = 0
  } else if (target >= scaleMax * SHRINK_RATIO) {
    handle.shrinkCount = 0
  } else if (handle.countedPoll !== handle.poll) {
    handle.countedPoll = handle.poll
    if (++handle.shrinkCount >= SHRINK_POLLS) {
      scaleMax = target
      handle.shrinkCount = 0
    }
  }
  handle.scaleMax = scaleMax
  return scaleMax
}

function stableMaxRange (handle) {
  return (u, min, max) => {
    const scaleMax = stableMax(handle, targetMax(handle, max))
    return [0, scaleMax > 0 ? scaleMax : 10]
  }
}

const darkSchemeMql = window.matchMedia('(prefers-color-scheme: dark)')

function prefersDark () {
  const theme = document.documentElement.classList
  if (theme.contains('theme-light')) return false
  return theme.contains('theme-dark') || darkSchemeMql.matches
}

let dark = prefersDark()

function redrawOnThemeChange () {
  if (prefersDark() === dark) return
  dark = !dark
  for (const h of handles) h.uplot && h.uplot.redraw()
}
darkSchemeMql.addEventListener('change', redrawOnThemeChange)
new MutationObserver(redrawOnThemeChange).observe(
  document.documentElement, { attributes: true, attributeFilter: ['class'] }
)

function setScale (handle, { fixedMax = null, scaleCap = null, refLines = [] } = {}) {
  handle.fixedMax = fixedMax
  handle.scaleCap = scaleCap
  handle.refLines = refLines
}

const REF_TONES = {
  neutral: () => prefersDark() ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.4)',
  warn: () => '#d2a106',
  alarm: () => prefersDark() ? 'rgba(255,120,120,0.85)' : 'rgba(200,40,40,0.7)'
}

const LABEL_ROOM = 16

function drawRefLines (u, handle) {
  const lines = handle.refLines
  if (!lines || !lines.length) return
  const ctx = u.ctx
  const dpr = window.devicePixelRatio
  const left = u.bbox.left
  const right = u.bbox.left + u.bbox.width
  const top = u.bbox.top
  for (const line of lines) {
    if (!(line.value > 0)) continue
    const offTop = line.value > u.scales.y.max
    if (offTop && !line.pinAbove) continue
    const color = (REF_TONES[line.tone] || REF_TONES.neutral)()
    const alignLeft = line.align === 'left'
    const labelX = alignLeft ? left + 4 * dpr : right - 4 * dpr
    const text = line.valueText ? `${line.label} ${line.valueText}` : line.label
    ctx.save()
    ctx.fillStyle = color
    ctx.font = `${10 * dpr}px sans-serif`
    ctx.textAlign = alignLeft ? 'left' : 'right'
    ctx.textBaseline = 'bottom'
    if (offTop) {
      ctx.fillText(`↑ ${text}`, labelX, top - 2 * dpr)
      ctx.restore()
      continue
    }
    const y = Math.round(u.valToPos(line.value, 'y', true)) + 0.5
    ctx.strokeStyle = color
    ctx.lineWidth = dpr
    ctx.setLineDash([4 * dpr, 4 * dpr])
    ctx.beginPath()
    ctx.moveTo(left, y)
    ctx.lineTo(right, y)
    ctx.stroke()
    ctx.setLineDash([])
    if (line.label) ctx.fillText(text, labelX, y - 2 * dpr)
    ctx.restore()
  }
}

function yAxisSize (u, values, axisIdx, cycleNum) {
  const axis = u.axes[axisIdx]
  if (cycleNum > 1) return axis._size
  let textWidth = 0
  if (values) {
    u.ctx.font = axis.font[0]
    for (const v of values) {
      textWidth = Math.max(textWidth, u.ctx.measureText(v).width)
    }
  }
  const size = axis.ticks.size + axis.gap + textWidth / window.devicePixelRatio
  return Math.max(24, Math.ceil(size / 8) * 8)
}

const BYTE_INCRS = [0, 1, 2, 3, 4].flatMap(p =>
  [1, 2, 4, 8, 16, 32, 64, 128, 256, 512].map(m => m * 1024 ** p))

function fmtAxisBytes (v, perSec) {
  if (v === 0) return '0'
  const { value, prefix } = helpers.scaleBytes(v)
  const digits = formatDigits(value)
  return digits + ' ' + prefix + (perSec ? '/s' : '')
}

function xAxisValues (u, vals) {
  const dpr = window.devicePixelRatio
  const plotWidth = u.bbox.width / dpr
  const leftRoom = u.bbox.left / dpr
  u.ctx.font = u.axes[0].font[0]
  return vals.map(v => {
    const label = timeFormatter.format(new Date(v * 1000))
    const half = u.ctx.measureText(label).width / dpr / 2
    const pos = u.valToPos(v, 'x')
    if (pos - half < -leftRoom || pos + half > plotWidth) return null
    return label
  })
}

function initChart (handle) {
  const { el, config, seriesKeys, data } = handle
  const width = el.clientWidth || 400
  const height = chartHeight(width)

  const series = [{}]
  seriesKeys.forEach((key, i) => {
    series.push(makeSeriesDef(handle, key, seriesColor(i + 1), config.fill))
  })

  const gridColor = () => prefersDark() ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.08)'
  const axisColor = () => prefersDark() ? '#777' : '#666'
  const bytes = isByteUnit(config.unit)

  const opts = {
    width,
    height,
    padding: [LABEL_ROOM, 0, null, 0],
    cursor: {
      show: true,
      drag: { x: false, y: false },
      focus: { prox: 16 },
      sync: { key: cursorSync.key, setSeries: false, scales: ['x', null] }
    },
    focus: { alpha: 0.25 },
    legend: { show: false },
    hooks: {
      setLegend: [(u) => {
        updateLegend(handle, u.legend.idx)
      }],
      draw: [() => alignLegend(handle), (u) => drawRefLines(u, handle)]
    },
    series,
    axes: [
      {
        stroke: axisColor,
        grid: { stroke: gridColor, dash: [2, 4] },
        ticks: { stroke: gridColor },
        space: 80,
        incrs: [1, 5, 10, 15, 30, 60, 120, 300, 600],
        values: xAxisValues
      },
      {
        stroke: axisColor,
        grid: { stroke: gridColor, dash: [2, 4] },
        ticks: { stroke: gridColor, size: 3 },
        ...(bytes && { incrs: BYTE_INCRS }),
        values: bytes
          ? (u, vals) => vals.map(v => fmtAxisBytes(v, config.unit === 'bytes/s'))
          : (u, vals) => vals.map(helpers.nFormatter),
        size: yAxisSize,
        gap: 2
      }
    ],
    scales: {
      y: {
        range: stableMaxRange(handle)
      }
    }
  }

  buildLegend(handle)
  for (let i = 1; i <= seriesKeys.length; i++) {
    addLegendItem(handle, i, seriesColor(i))
  }

  handle.uplot = new UPlot(opts, data, el)
  el.append(handle.legendEl)
  // uPlot's first legend update ran before the legend was in the page,
  // too early to measure the value column.
  updateLegend(handle, null)
  alignLegend(handle)
  initChartObservers(handle)
}

function initChartObservers (handle) {
  const el = handle.el
  let lastWidth = el.clientWidth
  const ro = new ResizeObserver(() => {
    const newWidth = el.clientWidth
    if (newWidth > 0 && newWidth !== lastWidth && handle.uplot) {
      lastWidth = newWidth
      handle.uplot.setSize({ width: newWidth, height: chartHeight(newWidth) })
      alignLegend(handle)
    }
  })
  ro.observe(el)
}

function render (id, unit, fill = false) {
  const el = document.getElementById(id)
  const graphContainer = document.createElement('div')
  graphContainer.classList.add('graph')
  el.append(graphContainer)

  const handle = {
    el: graphContainer,
    uplot: null,
    legendEl: null,
    seriesKeys: [],
    labels: {},
    descs: {},
    data: [[]],
    scaleMax: 0,
    shrinkCount: 0,
    poll: 0,
    config: { unit, fill }
  }
  handles.add(handle)
  return handle
}

function toSpecs (data) {
  if (Array.isArray(data)) return data
  return Object.keys(data)
    .filter(key => key.endsWith('_details'))
    .map(key => ({ key, value: data[key], log: data[key + '_log'] || (data[key] && data[key].log) }))
}

function rebuildFromLogs (handle, specs) {
  const now = Date.now() / 1000
  const byKey = new Map(specs.map(spec => [spec.key, spec]))
  const logs = handle.seriesKeys.map(key => (byKey.get(key) || {}).log || [])
  const maxLen = Math.max(1, ...logs.map(log => log.length))

  const timestamps = []
  for (let i = 0; i < maxLen; i++) {
    timestamps.push(now - (statsInterval() / 1000) * (maxLen - 1 - i))
  }
  handle.data = [timestamps]

  handle.seriesKeys.forEach((key, i) => {
    const series = logs[i].map(toPoint)
    if (series.length === 0) series.push(toPoint((byKey.get(key) || {}).value))
    const pad = new Array(maxLen - series.length).fill(null)
    handle.data.push(pad.concat(series))
  })
}

function update (handle, data) {
  const specs = toSpecs(data)
  handle.poll++

  const newKeys = []
  for (const { key, label, desc } of specs) {
    if (label) handle.labels[key] = label
    if (desc) handle.descs[key] = desc
    if (handle.seriesKeys.includes(key)) continue
    newKeys.push(key)
    handle.seriesKeys.push(key)
    if (handle.legendEl) addLegendItem(handle, handle.seriesKeys.length, seriesColor(handle.seriesKeys.length))
  }

  rebuildFromLogs(handle, specs)

  if (!handle.uplot) {
    initChart(handle)
  } else {
    for (const key of newKeys) {
      const idx = handle.seriesKeys.indexOf(key)
      handle.uplot.addSeries(makeSeriesDef(handle, key, seriesColor(idx + 1), handle.config.fill), idx + 1)
    }
  }
  handle.uplot.setData(handle.data)
  refreshLegend(handle)
}

export {
  render, update, setScale, stableMax
}
