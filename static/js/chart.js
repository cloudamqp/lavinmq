/* globals Chart */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  const chartColors = ['#5899DA', '#E8743B', '#19A979', '#ED4A7B', '#945ECF', '#13A4B4',
    '#525DF4', '#BF399E', '#6C8893', '#EE6868', '#2F6497']

  function ticks (ctx) {
    return ctx.clientWidth / 80
  }

  function render (selector, unit, options = {}) {
    const ctx = document.getElementById(selector)
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
          labels: {
            boxWidth: 10
          }
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
              unitStepSize: 5,
              displayFormats: {
                second: 'HH:mm:ss'
              }
            },
            ticks: {
              min: 0,
              max: ticks(ctx),
              source: 'data'
            }
          }],
          yAxes: [{
            scaleLabel: {
              display: true,
              labelString: unit
            },
            ticks: {
              beginAtZero: true,
              min: 0,
              callback: nFormatter
            }
          }]
        }
      }, options)
    })
    return chart
  }

  function nFormatter (num) {
    if (num >= 1000000000) {
      return (num / 1000000000).toFixed(1).replace(/\.0$/, '') + 'G'
    }
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M'
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'K'
    }
    return num
  }

  function formatLabel (key) {
    return key.replace(/_/, ' ').replace(/(rate|details)/, '').replace(/^\w/, c => c.toUpperCase())
  }

  function value (data) {
    if (data.rate !== undefined) return data.rate
    return data
  }

  function update (chart, data) {
    let date = new Date()
    let keys = Object.keys(data)
    for (let key in data) {
      let label = formatLabel(key)
      let dataset = chart.data.datasets.find(dataset => dataset.key === key)
      if (dataset === undefined) {
        let i = keys.indexOf(key)
        let color = chartColors[Math.floor((i / keys.length) * chartColors.length)]
        dataset = {
          key,
          label,
          fill: false,
          type: 'line',
          data: [],
          backgroundColor: color,
          borderColor: color
        }
        chart.data.datasets.push(dataset)
      }
      let point = {
        x: date,
        y: value(data[key])
      }
      if (dataset.data.length >= ticks(chart.ctx.canvas)) {
        dataset.data.shift()
      }
      dataset.data.push(point)
    }
    chart.update()
  }

  Object.assign(window.avalanchemq, {
    chart: {
      render, update
    }
  })
})()
