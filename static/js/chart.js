/* globals Chart */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  const chartColors = ['#5899DA', '#E8743B', '#19A979', '#ED4A7B', '#945ECF', '#13A4B4',
    '#525DF4', '#BF399E', '#6C8893', '#EE6868', '#2F6497']

  function ticks (ctx) {
    return ctx.clientWidth / 10
  }

  function render (id, unit, options = {}) {
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
            label: function(tooltipItem, data) {
              var label = data.datasets[tooltipItem.datasetIndex].label || ''
              label += ': ' + avalanchemq.helpers.formatNumber(tooltipItem.yLabel)
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
            },
            ticks: {
              min: 0,
              max: ticks(ctx),
              source: 'auto'
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
              callback: window.avalanchemq.helpers.nFormatter
            }
          }]
        },
        legendCallback: function (chart) {
          let text = []
          for (let i = 0; i < chart.data.datasets.length; i++) {
            let dataSet = chart.data.datasets[i]
            let value = dataSet.data[-1] ? dataSet.data[-1].y : ''
            text.push(`<div class="legend-item checked">
              <div class="toggle"></div>
              <div class="color-ref" style="background-color:${dataSet.backgroundColor}"></div>
              <div>
                <div class="legend-label">${dataSet.label}</div>
                <div class="legend-value">${avalanchemq.helpers.formatNumber(value)}</div>
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
      let parent = target.parentElement
      let chartId = parseInt(parent.classList[1].split('-')[0], 10)
      let chart = Chart.instances[chartId]
      let index = Array.prototype.slice.call(parent.children).indexOf(target)

      chart.legend.options.onClick.call(chart, e, chart.legend.legendItems[index])
      if (chart.isDatasetVisible(index)) {
        target.classList.add('checked')
      } else {
        target.classList.remove('checked')
      }
    })
  }

  function formatLabel (key) {
    let label = key.replace(/_/g, ' ').replace(/(rate|details|unroutable|messages)/ig, '').trim()
      .replace(/^\w/, c => c.toUpperCase())
    return label || 'Total'
  }

  function value (data) {
    return (data.rate === undefined) ? data : data.rate
  }

  function createDataset (key, color) {
    let label = formatLabel(key)
    return {
      key,
      label,
      fill: false,
      type: 'line',
      steppedLine: true,
      pointRadius: 0,
      pointStyle: 'line',
      data: [],
      backgroundColor: color,
      borderColor: color
    }
  }

  function addToDataset (dataset, data, date, maxY) {
    let point = {
      x: date,
      y: value(data)
    }
    if (dataset.data.length >= maxY) {
      dataset.data.shift()
    }
    dataset.data.push(point)
  }

  function update (chart, data) {
    const date = new Date()
    const maxY = ticks(chart.ctx.canvas)
    let keys = Object.keys(data)
    const has_details = keys.find(key => key.match(/_details$/))
    if (has_details) { keys = keys.filter(key => key.match(/_details$/)) }
    const legend = chart.ctx.canvas.closest('.chart-container').querySelector('.legend')
    for (let key in data) {
      if (key.match(/_log$/)) continue
      if (has_details && !key.match(/_details$/)) continue
      let dataset = chart.data.datasets.find(dataset => dataset.key === key)
      let i = keys.indexOf(key)
      if (dataset === undefined) {
        let color = chartColors[Math.floor((i / keys.length) * chartColors.length)]
        dataset = createDataset(key, color)
        chart.data.datasets.push(dataset)
        legend.innerHTML = chart.generateLegend()
        let log = data[`${key}_log`] || data[key].log || []
        log.forEach((p, i) => {
          let pDate = new Date(date.getTime() - 5000 * (log.length - i))
          addToDataset(dataset, p, pDate, maxY)
        })
      }
      addToDataset(dataset, data[key], date, maxY)
      setTimeout(() => {
        legend.children[i].querySelector('.legend-value').innerHTML = avalanchemq.helpers.formatNumber(dataset.data.slice(-1)[0].y)
      }, 50)
    }
    chart.update()
  }

  Object.assign(window.avalanchemq, {
    chart: {
      render, update
    }
  })
})()
