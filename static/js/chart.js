import * as helpers from './helpers.js'
import './lib/d3.v7.min.js'

// D3 is now available as a global variable
const d3 = window.d3

const chartColors = ['#54be7e', '#4589ff', '#d12771', '#d2a106', '#08bdba', '#bae6ff', '#ba4e00',
  '#d4bbff', '#8a3ffc', '#33b1ff', '#007d79']

const POLLING_RATE = 5000
const X_AXIS_LENGTH = 600000 // 10 min
const MAX_TICKS = X_AXIS_LENGTH / POLLING_RATE

function render (id, unit, options = {}, stacked = false) {
  const el = document.getElementById(id)
  const graphContainer = document.createElement('div')
  graphContainer.classList.add('graph')
  el.append(graphContainer)

  // Set up dimensions and margins
  const margin = { top: 20, right: 120, bottom: 50, left: 60 }
  const minWidth = 400
  const minHeight = 250

  // Get initial container dimensions
  const containerWidth = Math.max(minWidth, graphContainer.offsetWidth || 800)
  const containerHeight = Math.max(minHeight, Math.min(containerWidth * 0.5, 400))

  const width = containerWidth - margin.left - margin.right
  const height = containerHeight - margin.top - margin.bottom

  // Create responsive SVG
  const svg = d3.select(graphContainer)
    .append('svg')
    .attr('width', '100%')
    .attr('height', '100%')
    .attr('viewBox', `0 0 ${containerWidth} ${containerHeight}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('background', 'transparent')
    .style('min-width', minWidth + 'px')
    .style('min-height', minHeight + 'px')

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)

  // Create scales
  const xScale = d3.scaleTime()
    .range([0, width])

  const yScale = d3.scaleLinear()
    .range([height, 0])
    .domain([0, 10])

  // Create line generator
  const line = d3.line()
    .defined(d => d.y !== null)
    .x(d => xScale(d.x))
    .y(d => yScale(d.y))
    .curve(d3.curveMonotoneX)

  // Create axes
  const xAxis = d3.axisBottom(xScale)
    .tickFormat(d3.timeFormat('%H:%M:%S'))

  const yAxis = d3.axisLeft(yScale)
    .tickFormat(helpers.nFormatter)

  // Add axes to chart
  const xAxisGroup = g.append('g')
    .attr('class', 'x-axis')
    .attr('transform', `translate(0,${height})`)
    .call(xAxis)

  const yAxisGroup = g.append('g')
    .attr('class', 'y-axis')
    .call(yAxis)

  // Remove axis domain lines (the outer border)
  xAxisGroup.select('.domain').remove()
  yAxisGroup.select('.domain').remove()

  // Add grid lines
  const xGrid = g.append('g')
    .attr('class', 'grid x-grid')
    .attr('transform', `translate(0,${height})`)
    .call(xAxis.tickSize(-height).tickFormat(''))

  xGrid.selectAll('line')
    .style('stroke', '#404040')
    .style('stroke-width', 0.5)
    .style('opacity', 0.7)

  xGrid.select('.domain').remove()

  const yGrid = g.append('g')
    .attr('class', 'grid y-grid')
    .call(yAxis.tickSize(-width).tickFormat(''))

  yGrid.selectAll('line')
    .style('stroke', '#404040')
    .style('stroke-width', 0.5)
    .style('opacity', 0.7)

  yGrid.select('.domain').remove()

  // Add Y axis label
  g.append('text')
    .attr('class', 'y-label')
    .attr('transform', 'rotate(-90)')
    .attr('y', 0 - margin.left)
    .attr('x', 0 - (height / 2))
    .attr('dy', '1em')
    .style('text-anchor', 'middle')
    .style('font-size', '14px')
    .style('fill', '#fff')
    .text(unit)

  // Create tooltip
  const tooltip = d3.select('body').append('div')
    .attr('class', 'chart-tooltip')
    .style('position', 'absolute')
    .style('visibility', 'hidden')
    .style('background', 'rgba(0, 0, 0, 0.8)')
    .style('color', 'white')
    .style('padding', '8px')
    .style('border-radius', '4px')
    .style('font-size', '12px')
    .style('z-index', '1000')

  // Create legend container
  const legend = svg.append('g')
    .attr('class', 'legend')
    .attr('transform', `translate(${width + margin.left + 10}, ${margin.top + 20})`)

  // Chart object to return
  const chart = {
    svg,
    g,
    xScale,
    yScale,
    line,
    xAxisGroup,
    yAxisGroup,
    tooltip,
    legend,
    width,
    height,
    margin,
    minWidth,
    minHeight,
    graphContainer,
    data: { datasets: [] },
    options: Object.assign({}, options)
  }

  // Add resize functionality with debounce
  let resizeTimeout
  const resizeObserver = new ResizeObserver(() => {
    clearTimeout(resizeTimeout)
    resizeTimeout = setTimeout(() => {
      resizeChart(chart)
    }, 100)
  })
  resizeObserver.observe(graphContainer)

  // Store the observer for cleanup
  chart.resizeObserver = resizeObserver

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
    data: [],
    color,
    hidden: false
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

  // Update D3 chart
  updateD3Chart(chart)
}

function updateD3Chart (chart) {
  if (chart.data.datasets.length === 0) return

  // Calculate time domain
  const now = new Date()
  const timeWindow = X_AXIS_LENGTH
  const xDomain = [new Date(now.getTime() - timeWindow), now]

  // Update x scale domain
  chart.xScale.domain(xDomain)

  // Calculate y domain from visible datasets only
  const visibleData = chart.data.datasets.filter(d => !d.hidden).flatMap(d => d.data)
  const yMax = visibleData.length > 0 ? d3.max(visibleData, d => d.y) || 10 : 10
  const yMaxWithPadding = Math.max(yMax * 1.1, 10) // Add 10% padding to the top
  chart.yScale.domain([0, yMaxWithPadding])

  // Update axes
  chart.xAxisGroup.call(d3.axisBottom(chart.xScale).tickFormat(d3.timeFormat('%H:%M:%S')))
  chart.yAxisGroup.call(d3.axisLeft(chart.yScale).tickFormat(helpers.nFormatter))

  // Remove all axis domain lines (borders)
  chart.xAxisGroup.select('.domain').remove()
  chart.yAxisGroup.select('.domain').remove()

  // Update grid lines with current dimensions
  const xGrid = chart.g.select('.x-grid')
    .call(d3.axisBottom(chart.xScale).tickSize(-chart.height).tickFormat(''))

  xGrid.selectAll('line')
    .style('stroke', '#404040')
    .style('stroke-width', 0.5)
    .style('opacity', 0.7)

  xGrid.select('.domain').remove()

  const yGrid = chart.g.select('.y-grid')
    .call(d3.axisLeft(chart.yScale).tickSize(-chart.width).tickFormat(''))

  yGrid.selectAll('line')
    .style('stroke', '#404040')
    .style('stroke-width', 0.5)
    .style('opacity', 0.7)

  yGrid.select('.domain').remove()

  // Bind data to lines (only visible datasets)
  const lines = chart.g.selectAll('.line')
    .data(chart.data.datasets, d => d.key)

  // Remove old lines
  lines.exit().remove()

  // Add new lines
  const newLines = lines.enter()
    .append('g')
    .attr('class', 'line')

  newLines.append('path')
    .attr('class', 'line-path')
    .style('fill', 'none')
    .style('stroke-width', 2)

  // Update all lines
  const allLines = newLines.merge(lines)

  allLines.select('.line-path')
    .datum(d => d.data)
    .style('stroke', (d, i) => chart.data.datasets[i].color)
    .style('opacity', (d, i) => chart.data.datasets[i].hidden ? 0 : 1)
    .attr('d', chart.line)

  // Add chart-wide hover interaction
  chart.g.selectAll('.hover-overlay').remove()

  // Create invisible overlay for mouse tracking
  const overlay = chart.g.append('rect')
    .attr('class', 'hover-overlay')
    .attr('width', chart.width)
    .attr('height', chart.height)
    .style('fill', 'none')
    .style('pointer-events', 'all')
    .on('mousemove', function (event) {
      const [mouseX] = d3.pointer(event, this)
      const mouseTime = chart.xScale.invert(mouseX)

      // Find closest data points for all visible datasets with wider search radius
      const tooltipData = []
      const maxDistance = POLLING_RATE * 2 // Allow 2 polling intervals distance

      chart.data.datasets.forEach(dataset => {
        if (dataset.hidden || dataset.data.length === 0) return

        // Find closest point by time
        const bisect = d3.bisector(d => d.x).left
        const index = bisect(dataset.data, mouseTime)

        let closestPoint = null
        let minDistance = Infinity

        // Check points around the bisection index
        for (let i = Math.max(0, index - 2); i < Math.min(dataset.data.length, index + 3); i++) {
          const point = dataset.data[i]
          if (point && point.y !== null) {
            const distance = Math.abs(mouseTime.getTime() - point.x.getTime())
            if (distance < minDistance && distance <= maxDistance) {
              minDistance = distance
              closestPoint = point
            }
          }
        }

        if (closestPoint) {
          tooltipData.push({
            label: dataset.label,
            value: closestPoint.y,
            color: dataset.color,
            time: closestPoint.x
          })
        }
      })

      // Always show tooltip and crosshair when hovering over chart
      // Remove previous hover elements
      chart.g.selectAll('.hover-line').remove()
      chart.g.selectAll('.hover-dot').remove()

      // Show vertical line at mouse position
      chart.g.append('line')
        .attr('class', 'hover-line')
        .attr('x1', mouseX)
        .attr('x2', mouseX)
        .attr('y1', 0)
        .attr('y2', chart.height)
        .style('stroke', '#fff')
        .style('stroke-width', 1)
        .style('stroke-dasharray', '3,3')
        .style('opacity', 0.7)

      if (tooltipData.length > 0) {
        // Highlight closest data points
        tooltipData.forEach(item => {
          const pointX = chart.xScale(item.time)
          const pointY = chart.yScale(item.value)

          // Add outer ring
          chart.g.append('circle')
            .attr('class', 'hover-dot')
            .attr('cx', pointX)
            .attr('cy', pointY)
            .attr('r', 6)
            .style('fill', 'none')
            .style('stroke', '#fff')
            .style('stroke-width', 2)
            .style('opacity', 0.8)

          // Add inner dot
          chart.g.append('circle')
            .attr('class', 'hover-dot')
            .attr('cx', pointX)
            .attr('cy', pointY)
            .attr('r', 3)
            .style('fill', item.color)
            .style('stroke', '#fff')
            .style('stroke-width', 1)
        })

        // Create tooltip content
        const timeStr = d3.timeFormat('%H:%M:%S')(tooltipData[0].time)
        let tooltipHTML = `<div class="tooltip-time">${timeStr}</div>`

        tooltipData.forEach((item, index) => {
          tooltipHTML += `<div class="tooltip-line" data-index="${index}">
            <span class="color-indicator" data-index="${index}"></span>
            <span class="line-text" data-index="${index}">${item.label}: <strong class="line-value" data-index="${index}">${helpers.formatNumber(item.value)}</strong></span>
          </div>`
        })

        // Update tooltip content
        chart.tooltip.html(tooltipHTML)

        // Style the tooltip using D3
        const tooltipNode = d3.select(chart.tooltip.node())

        tooltipNode.select('.tooltip-time')
          .style('font-weight', 'bold')
          .style('margin-bottom', '5px')

        // Style each tooltip line using D3
        tooltipData.forEach((item, index) => {
          tooltipNode.select(`.tooltip-line[data-index="${index}"]`)
            .style('margin', '2px 0')
            .style('display', 'flex')
            .style('align-items', 'center')

          tooltipNode.select(`.color-indicator[data-index="${index}"]`)
            .style('display', 'inline-block')
            .style('width', '12px')
            .style('height', '12px')
            .style('margin-right', '8px')
            .style('border-radius', '2px')
            .style('border', '1px solid rgba(255,255,255,0.3)')
            .style('background-color', item.color)

          tooltipNode.select(`.line-text[data-index="${index}"]`)
            .style('color', item.color)

          tooltipNode.select(`.line-value[data-index="${index}"]`)
            .style('color', item.color)
        })

        // Show tooltip
        chart.tooltip
          .style('visibility', 'visible')
          .style('top', (event.pageY - 10) + 'px')
          .style('left', (event.pageX + 10) + 'px')
      } else {
        // Show time even when no data points are found
        const timeStr = d3.timeFormat('%H:%M:%S')(mouseTime)
        chart.tooltip
          .html(`<div class="tooltip-time">${timeStr}</div><div>No data at this time</div>`)

        // Style the fallback tooltip using D3
        d3.select(chart.tooltip.node())
          .select('.tooltip-time')
          .style('font-weight', 'bold')
          .style('margin-bottom', '5px')

        chart.tooltip
          .style('visibility', 'visible')
          .style('top', (event.pageY - 10) + 'px')
          .style('left', (event.pageX + 10) + 'px')
      }
    })
    .on('mouseout', function () {
      chart.tooltip.style('visibility', 'hidden')
      chart.g.selectAll('.hover-line').remove()
      chart.g.selectAll('.hover-dot').remove()
    })

  // Update legend
  updateLegend(chart)
}

function resizeChart (chart) {
  // Get new container dimensions
  const containerWidth = Math.max(chart.minWidth, chart.graphContainer.offsetWidth || 800)
  const containerHeight = Math.max(chart.minHeight, Math.min(containerWidth * 0.5, 400))

  const newWidth = containerWidth - chart.margin.left - chart.margin.right
  const newHeight = containerHeight - chart.margin.top - chart.margin.bottom

  // Update chart dimensions
  chart.width = newWidth
  chart.height = newHeight

  // Update SVG viewBox
  chart.svg.attr('viewBox', `0 0 ${containerWidth} ${containerHeight}`)

  // Update scales ranges
  chart.xScale.range([0, newWidth])
  chart.yScale.range([newHeight, 0])

  // Update line generator
  chart.line = d3.line()
    .defined(d => d.y !== null)
    .x(d => chart.xScale(d.x))
    .y(d => chart.yScale(d.y))
    .curve(d3.curveMonotoneX)

  // Update axis positions
  chart.xAxisGroup.attr('transform', `translate(0,${newHeight})`)

  // Remove old grid lines
  chart.g.selectAll('.grid').remove()

  // Re-create grid lines with new dimensions
  const xGrid = chart.g.append('g')
    .attr('class', 'grid x-grid')
    .attr('transform', `translate(0,${newHeight})`)
    .call(d3.axisBottom(chart.xScale).tickSize(-newHeight).tickFormat(''))

  xGrid.selectAll('line')
    .style('stroke', '#404040')
    .style('stroke-width', 0.5)
    .style('opacity', 0.7)

  xGrid.select('.domain').remove()

  const yGrid = chart.g.append('g')
    .attr('class', 'grid y-grid')
    .call(d3.axisLeft(chart.yScale).tickSize(-newWidth).tickFormat(''))

  yGrid.selectAll('line')
    .style('stroke', '#404040')
    .style('stroke-width', 0.5)
    .style('opacity', 0.7)

  yGrid.select('.domain').remove()

  // Update legend position
  chart.legend.attr('transform', `translate(${newWidth + chart.margin.left + 10}, ${chart.margin.top + 20})`)

  // Re-render the chart with new dimensions
  updateD3Chart(chart)
}

function updateLegend (chart) {
  const legendItems = chart.legend.selectAll('.legend-item')
    .data(chart.data.datasets, d => d.key)

  // Remove old legend items
  legendItems.exit().remove()

  // Add new legend items
  const newLegendItems = legendItems.enter()
    .append('g')
    .attr('class', 'legend-item')
    .style('cursor', 'pointer')

  // Add legend rectangles
  newLegendItems.append('rect')
    .attr('width', 12)
    .attr('height', 12)
    .attr('rx', 2)

  // Add legend text
  newLegendItems.append('text')
    .attr('x', 18)
    .attr('y', 6)
    .attr('dy', '0.35em')
    .style('font-size', '12px')
    .style('font-family', 'sans-serif')

  // Update all legend items
  const allLegendItems = newLegendItems.merge(legendItems)

  allLegendItems
    .attr('transform', (d, i) => `translate(0, ${i * 20})`)
    .on('click', function (event, d) {
      d.hidden = !d.hidden
      updateD3Chart(chart)
    })

  allLegendItems.select('rect')
    .style('fill', d => d.hidden ? '#666' : d.color)
    .style('stroke', d => d.color)
    .style('stroke-width', 1)

  allLegendItems.select('text')
    .text(d => d.label)
    .style('fill', d => d.hidden ? '#666' : '#fff')
    .style('text-decoration', d => d.hidden ? 'line-through' : 'none')
}

export {
  render, update
}
