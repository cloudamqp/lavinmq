import * as helpers from './helpers.js'

const chartColors = ['#003f5c', '#ffa600', '#665191', '#a05195', '#d45087', '#f95d6a', '#ff7c43', '#2f4b7c',
  '#EE6868', '#2F6497', '#6C8893']

const POLLING_RATE = 5000
const X_AXIS_LENGTH = 600000 //10 min
const MAX_TICKS = X_AXIS_LENGTH/POLLING_RATE

function render (id, unit, options = {}, stacked = false) {
  const el = document.getElementById(id)
  const graphContainer = document.createElement('div')
  graphContainer.classList.add('graph')
  const ctx = document.createElement('canvas')
  graphContainer.append(ctx)
  el.append(graphContainer)
  const legendEl = document.createElement('div')
  legendEl.classList.add('legend')
  el.append(legendEl)
  const chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: []
    },
    options: Object.assign({
      responsive: true,
      aspectRatio: 4,
      legend: {
        display: false
      },
      tooltips: {
        mode: 'x',
        intersect: false,
        position: 'nearest',
        callbacks: {
          label: function (tooltipItem, data) {
            var label = data.datasets[tooltipItem.datasetIndex].label || ''
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
        xAxes: [{
          type: 'time',
          distribution: 'series',
          gridLines: {
            display: false
          },
          time: {
            unit: 'second',
            displayFormats: {
              second: 'HH:mm:ss'
            }
          }
        }],
        yAxes: [{
          scaleLabel: {
            display: true,
            labelString: unit,
            fontSize: 14
          },
          ticks: {
            beginAtZero: true,
            min: 0,
            suggestedMax: 10,
            callback: helpers.nFormatter
          },
          stacked: stacked
        }]
      },
      legendCallback: function (chart) {
        const text = []
        let total = 0
        for (let i = 0; i < chart.data.datasets.length; i++) {
          const dataSet = chart.data.datasets[i]
          const value = dataSet.data[-1] ? dataSet.data[-1].y : ''
          total += value
          text.push(`<div class="legend-item checked">
              <div class="toggle"></div>
              <div class="color-ref" style="background-color:${dataSet.backgroundColor}"></div>
              <div>
                <div class="legend-label">${dataSet.label}</div>
                <div class="legend-value">${helpers.formatNumber(value)}</div>
              </div>
            </div>`)
        }
        if (stacked) {
          text.push(`<div class="legend-item checked">
              <div class="toggle"></div>
              <div class="color-ref" style="background-color:${chartColors.slice(-1)[0]}"></div>
              <div>
                <div class="legend-label">Total</div>
                <div class="legend-value">${helpers.formatNumber(total)}</div>
              </div>
            </div>`)
        }
        return text.join('')
      }
    }, options)
  })
  legendEl.classList.add(chart.id + '-legend')

  legendEl.innerHTML = chart.generateLegend()
  addLegendClickHandler(legendEl)
  return chart
}

function addLegendClickHandler (legendEl) {
  legendEl.addEventListener('click', e => {
    let target = e.target
    while (!target.classList.contains('legend-item')) {
      target = target.parentElement
    }
    const parent = target.parentElement
    const chartId = parseInt(parent.classList[1].split('-')[0], 10)
    const chart = Chart.instances[chartId]
    const index = Array.prototype.slice.call(parent.children).indexOf(target)

    chart.legend.options.onClick.call(chart, e, chart.legend.legendItems[index])
    if (chart.isDatasetVisible(index)) {
      target.classList.add('checked')
    } else {
      target.classList.remove('checked')
    }
  })
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
    fill: fill,
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

function fillDatasetVoids(dataset) {
  let prevPoint = dataset.data[0]
  let moreIter = false
  dataset.data.forEach((point,i) => {
    const timeDiff = point.x.getTime() - prevPoint.x.getTime()
    if (timeDiff >= POLLING_RATE*2) {
      dataset.data.splice(i ,0 ,{"x": new Date(point.x.getTime() - POLLING_RATE), y: null})
      moreIter = timeDiff >= POLLING_RATE*3
    }
    prevPoint = point
  })
  moreIter && fillDatasetVoids(dataset)
}

function fixDatasetLength(dataset) {
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
  const legend = chart.ctx.canvas.closest('.chart-container').querySelector('.legend')
  for (const key in data) {
    if (key.match(/_log$/)) continue
    if (hasDetails && !key.match(/_details$/)) continue
    let dataset = chart.data.datasets.find(dataset => dataset.key === key)
    const i = keys.indexOf(key)
    if (dataset === undefined) {
      const color = chartColors[i % chartColors.length]
      dataset = createDataset(key, color, filled)
      chart.data.datasets.push(dataset)
      legend.innerHTML = chart.generateLegend()
      const log = data[`${key}_log`] || data[key].log || []
      log.forEach((p, i) => {
        const pDate = new Date(date.getTime() - POLLING_RATE * (log.length - i))
        addToDataset(dataset, p, pDate)
      })
    }
    addToDataset(dataset, data[key], date)
    setTimeout(() => {
      legend.children[i].querySelector('.legend-value').innerHTML = helpers.formatNumber(dataset.data.slice(-1)[0].y)
    }, 50)
  }
  if (chart.config.options.scales.yAxes[0].stacked) {
    setTimeout(() => {
      const value = chart.data.datasets.reduce((accumulator, dataset) => {
        return accumulator + dataset.data.slice(-1)[0].y
      }, 0)
      legend.children[legend.children.length - 1].querySelector('.legend-value').innerHTML = helpers.formatNumber(value)
    }, 50)
  }
  chart.update()
}

export {
  render, update
}
