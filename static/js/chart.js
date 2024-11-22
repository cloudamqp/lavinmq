import * as helpers from './helpers.js'
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

const chartColors = ['#54be7e', '#4589ff', '#d12771', '#d2a106', '#08bdba', '#bae6ff', '#ba4e00',
  '#d4bbff', '#8a3ffc', '#33b1ff', '#007d79']

const POLLING_RATE = 5000
const X_AXIS_LENGTH = 600000 // 10 min
const MAX_TICKS = X_AXIS_LENGTH / POLLING_RATE

function render (id, unit, options = {}, stacked = false) {
  const el = document.getElementById(id)
  const graphContainer = document.createElement('div')
  graphContainer.classList.add('graph')
  const ctx = document.createElement('canvas')
  graphContainer.append(ctx)
  el.append(graphContainer)

  const chart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [],
      labels: []
    },
    options: Object.assign({
      responsive: true,
      maintainAspectRatio: false,
      aspectRatio: 1.3,
      tooltips: {
        mode: 'x',
        intersect: false,
        position: 'nearest',
        callbacks: {
          label: function (tooltipItem, data) {
            let label = data.datasets[tooltipItem.datasetIndex].label || ''
            label += ': ' + helpers.formatNumber(tooltipItem.yLabel)
            return label
          }
        }
      },
      hover: {
        mode: 'x',
        intersect: false
      },
      scales: {
        x: {
          type: 'time',
          distribution: 'series',
          gridLines: {
            display: false
          },
          grid: {
            color: '#2D2C2C'
          },
          border: {
            dash: [2, 4],
          },
          time: {
            unit: 'second',
            displayFormats: {
              second: 'HH:mm:ss'
            }
          }
        },
        y: {
          title: {
            display: true,
            text: unit,
            fontsize: 14
          },
          grid: {
            color: '#2D2C2C'
          },
          border: {
            dash: [2, 4],
          },
          ticks: {
            beginAtZero: true,
            min: 0,
            suggestedMax: 10,
            callback: helpers.nFormatter
          },
          stacked: false,
          beginAtZero: true
        }
      }
    }, options)
  })
  return chart
}
function formatLabel (key) {
  const label = key.replace(/_/g, ' ').replace(/(rate|details|messages)/ig, '').trim()
    .replace(/^\w/, c => c.toUpperCase())
  return label || 'Total'
}

function value (data) {
  return (data.rate === undefined) ? data : data.rate
}

function createDataset (key, color, fill) {
  const label = formatLabel(key)
  return {
    key,
    label,
    fill,
    type: 'line',
    steppedLine: false,
    lineTension: 0.3,
    pointRadius: 0,
    pointStyle: 'line',
    data: [],
    backgroundColor: color,
    borderColor: color
  }
}

function addToDataset (dataset, data, date) {
  const point = {
    x: date,
    y: value(data)
  }
  if (dataset.data.length >= MAX_TICKS) {
    dataset.data.shift()
  }
  dataset.data.push(point)
  fillDatasetVoids(dataset)
  fixDatasetLength(dataset)
}

function fillDatasetVoids (dataset) {
  let prevPoint = dataset.data[0]
  let moreIter = false
  dataset.data.forEach((point, i) => {
    const timeDiff = point.x.getTime() - prevPoint.x.getTime()
    if (timeDiff >= POLLING_RATE * 2) {
      dataset.data.splice(i, 0, { x: new Date(point.x.getTime() - POLLING_RATE), y: null })
      moreIter = timeDiff >= POLLING_RATE * 3
    }
    prevPoint = point
  })
  moreIter && fillDatasetVoids(dataset)
}

function fixDatasetLength (dataset) {
  const now = new Date()
  dataset.data.forEach((point) => {
    now > point.x.getTime() + X_AXIS_LENGTH && dataset.data.shift()
  })
}

function update (chart, data, filled = false) {
  const date = new Date()
  let keys = Object.keys(data)
  const hasDetails = keys.find(key => key.match(/_details$/))
  if (hasDetails) { keys = keys.filter(key => key.match(/_details$/)) }
  for (const key in data) {
    if (key.match(/_log$/)) continue
    if (hasDetails && !key.match(/_details$/)) continue
    let dataset = chart.data.datasets.find(dataset => dataset.key === key)
    const i = keys.indexOf(key)
    if (dataset === undefined) {
      const color = chartColors[i % chartColors.length]
      dataset = createDataset(key, color, filled)
      chart.data.datasets.push(dataset)
      const log = data[`${key}_log`] || data[key].log || []
      log.forEach((p, i) => {
        const pDate = new Date(date.getTime() - POLLING_RATE * (log.length - i))
        addToDataset(dataset, p, pDate)
      })
    }
    addToDataset(dataset, data[key], date)
  }
  chart.update()
}

export {
  render, update
}
