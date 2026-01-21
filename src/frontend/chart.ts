import * as helpers from './helpers.js'

// Chart.js types - the library is loaded from lib/chart.js at runtime
// @ts-expect-error - External library with minimal types
import { Chart, TimeScale, LinearScale, LineController, PointElement, LineElement, Legend, Tooltip, Title, Filler } from './lib/chart.js'
import './lib/chartjs-adapter-luxon.esm.js'

Chart.register(TimeScale)
Chart.register(LinearScale)
Chart.register(LineController)
Chart.register(PointElement)
Chart.register(LineElement)
Chart.register(Legend)
Chart.register(Tooltip)
Chart.register(Title)
Chart.register(Filler)

const chartColors = [
  '#54be7e',
  '#4589ff',
  '#d12771',
  '#d2a106',
  '#08bdba',
  '#bae6ff',
  '#ba4e00',
  '#d4bbff',
  '#8a3ffc',
  '#33b1ff',
  '#007d79',
]

const POLLING_RATE = 5000
const X_AXIS_LENGTH = 600000 // 10 min
const MAX_TICKS = X_AXIS_LENGTH / POLLING_RATE

interface DataPoint {
  x: Date
  y: number | null
}

interface CustomDataset {
  key: string
  label: string
  fill: boolean
  order: number
  type: string
  tension: number
  pointRadius: number
  pointStyle: string
  data: DataPoint[]
  backgroundColor: string
  borderColor: string
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ChartInstance = any

export interface CustomChart {
  data: {
    datasets: CustomDataset[]
    labels: unknown[]
  }
  reverseStack?: boolean
  update(): void
}

interface ChartData {
  rate?: number
  log?: number[]
}

type ChartDataRecord = Record<string, ChartData | number | number[] | undefined>

export function render(
  id: string,
  unit: string,
  fill: boolean | Record<string, unknown> = false,
  stacked = false,
  reverseStack = false
): CustomChart {
  const el = document.getElementById(id)
  if (!el) throw new Error(`Element with id "${id}" not found`)

  const graphContainer = document.createElement('div')
  graphContainer.classList.add('graph')
  const ctx = document.createElement('canvas')
  graphContainer.append(ctx)
  el.append(graphContainer)

  const chart: ChartInstance = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [],
      labels: [],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      aspectRatio: 1.3,
      plugins: {
        legend: {
          labels: {
            generateLabels: function (chart: ChartInstance) {
              return Chart.defaults.plugins.legend.labels.generateLabels(chart).map(function (label: { strokeStyle?: string }) {
                return { ...label, fillStyle: label.strokeStyle }
              })
            },
          },
        },
        tooltip: {
          mode: 'index',
          intersect: false,
          position: 'nearest',
          callbacks: {
            label: function (tooltipItem: { dataset: { label?: string }; parsed: { y: number } }) {
              let label = tooltipItem.dataset.label || ''
              label += ': ' + helpers.formatNumber(tooltipItem.parsed.y)
              return label
            },
          },
        },
      },
      hover: {
        mode: 'index',
        intersect: false,
      },
      scales: {
        x: {
          type: 'time',
          grid: {
            color: '#2D2C2C',
          },
          border: {
            dash: [2, 4],
          },
          time: {
            unit: 'second',
            displayFormats: {
              second: 'HH:mm:ss',
            },
          },
        },
        y: {
          title: {
            display: true,
            text: unit,
          },
          grid: {
            color: '#2D2C2C',
          },
          border: {
            dash: [2, 4],
          },
          ticks: {
            beginAtZero: true,
            min: 0,
            suggestedMax: 10,
            callback: (value: number) => helpers.nFormatter(value),
          },
          stacked,
          beginAtZero: true,
        },
      },
      ...(typeof fill === 'object' ? fill : {}),
    },
  })
  chart.reverseStack = reverseStack

  return chart as CustomChart
}

function formatLabel(key: string): string {
  const label = key
    .replace(/_/g, ' ')
    .replace(/(rate|details|messages)/gi, '')
    .trim()
    .replace(/^\w/, (c) => c.toUpperCase())
  return label || 'Total'
}

function value(data: ChartData | number): number {
  if (typeof data === 'number') return data
  return data.rate !== undefined ? data.rate : 0
}

function createDataset(key: string, color: string, fill: boolean, order = 0): CustomDataset {
  const label = formatLabel(key)
  const backgroundColor = fill ? color + '66' : color

  return {
    key,
    label,
    fill,
    order,
    type: 'line',
    tension: 0.3,
    pointRadius: 0,
    pointStyle: 'line',
    data: [],
    backgroundColor,
    borderColor: color,
  }
}

function addToDataset(dataset: CustomDataset, data: ChartData | number, date: Date): void {
  const point: DataPoint = {
    x: date,
    y: value(data),
  }
  if (dataset.data.length >= MAX_TICKS) {
    dataset.data.shift()
  }
  dataset.data.push(point)
  fillDatasetVoids(dataset)
  fixDatasetLength(dataset)
}

function fillDatasetVoids(dataset: CustomDataset): void {
  const firstPoint = dataset.data[0]
  if (!firstPoint) return
  let prevPoint: DataPoint = firstPoint
  let moreIter = false
  dataset.data.forEach((point, i) => {
    const timeDiff = point.x.getTime() - prevPoint.x.getTime()
    if (timeDiff >= POLLING_RATE * 2) {
      dataset.data.splice(i, 0, { x: new Date(point.x.getTime() - POLLING_RATE), y: null })
      moreIter = timeDiff >= POLLING_RATE * 3
    }
    prevPoint = point
  })
  if (moreIter) fillDatasetVoids(dataset)
}

function fixDatasetLength(dataset: CustomDataset): void {
  const now = new Date()
  dataset.data.forEach((point) => {
    if (now.getTime() > point.x.getTime() + X_AXIS_LENGTH) {
      dataset.data.shift()
    }
  })
}

export function update(chart: CustomChart, data: ChartDataRecord, filled = false): void {
  const date = new Date()
  let keys = Object.keys(data)
  const hasDetails = keys.find((key) => key.match(/_details$/))
  if (hasDetails) {
    keys = keys.filter((key) => key.match(/_details$/))
  }
  for (const key in data) {
    if (key.match(/_log$/)) continue
    if (hasDetails && !key.match(/_details$/)) continue
    let dataset = chart.data.datasets.find((ds) => ds.key === key)
    const i = keys.indexOf(key)
    if (dataset === undefined) {
      const color = chartColors[i % chartColors.length] ?? chartColors[0]!
      const order = chart.reverseStack ? keys.length - i : i
      dataset = createDataset(key, color, filled, order)
      chart.data.datasets.push(dataset)
      const dataValue = data[key]
      const log: number[] =
        (data[`${key}_log`] as number[] | undefined) ||
        (typeof dataValue === 'object' && dataValue !== null && !Array.isArray(dataValue) ? (dataValue as ChartData).log : undefined) ||
        []
      log.forEach((p, logIndex) => {
        const pDate = new Date(date.getTime() - POLLING_RATE * (log.length - logIndex))
        addToDataset(dataset!, p, pDate)
      })
    }
    const dataVal = data[key]
    if (dataVal !== undefined && !Array.isArray(dataVal)) {
      addToDataset(dataset, dataVal, date)
    }
  }
  chart.update()
}
