import './lib/d3.v7.min.js'

// D3 is now available as a global variable
const d3 = window.d3

class Chart {
  constructor (elementId, metricsName = 'msgs/s', stacked = false) {
    this.elementId = elementId
    this.metricsName = metricsName
    this.stacked = stacked
    this.maxDataPoints = 600 // Fixed 600 seconds on x-axis
    this.metrics = new Map() // Store metric data and metadata
    this.colors = ['#54be7e', '#4589ff', '#d12771', '#d2a106', '#08bdba', '#bae6ff', '#ba4e00', '#d4bbff', '#8a3ffc', '#33b1ff', '#007d79']

    // Chart dimensions and margins (extra bottom margin for legend and rotated labels)
    this.margin = { top: 20, right: 20, bottom: 120, left: 60 }
    this.width = 600 - this.margin.left - this.margin.right
    this.height = 400 - this.margin.top - this.margin.bottom

    this.initChart()
  }

  initChart () {
    const container = d3.select(`#${this.elementId}`)

    // Clear any existing content
    container.selectAll('*').remove()

    // Create SVG with fixed dimensions
    this.svg = container
      .append('svg')
      .attr('width', this.width + this.margin.left + this.margin.right)
      .attr('height', this.height + this.margin.top + this.margin.bottom)

    // Create main group with clipping path
    this.svg.append('defs').append('clipPath')
      .attr('id', `clip-${this.elementId}`)
      .append('rect')
      .attr('width', this.width)
      .attr('height', this.height)

    this.g = this.svg
      .append('g')
      .attr('transform', `translate(${this.margin.left},${this.margin.top})`)
      .attr('clip-path', `url(#clip-${this.elementId})`)

    // Create scales
    this.xScale = d3.scaleLinear()
      .domain([0, this.maxDataPoints])
      .range([0, this.width])

    this.yScale = d3.scaleLinear()
      .domain([0, 10]) // Initial domain, will be updated
      .range([this.height, 0])

    // Create line generator
    this.line = d3.line()
      .x((d, i) => this.xScale(i))
      .y(d => this.yScale(d))
      .defined(d => d !== null)
      .curve(d3.curveMonotoneX)

    // Create area generator for stacked charts
    this.area = d3.area()
      .x((d, i) => this.xScale(i))
      .y0(d => this.yScale(d[0]))
      .y1(d => this.yScale(d[1]))
      .defined(d => d[0] !== null && d[1] !== null)
      .curve(d3.curveMonotoneX)

    // Create axes
    this.xAxis = d3.axisBottom(this.xScale)
      .ticks(10)
      .tickFormat(d => {
        const secondsAgo = Math.max(0, this.maxDataPoints - d)
        if (secondsAgo === 0) return 'now'
        if (secondsAgo < 60) return `${secondsAgo}s ago`
        const minutesAgo = Math.floor(secondsAgo / 60)
        if (minutesAgo < 60) return `${minutesAgo} min ago`
        const hoursAgo = Math.floor(minutesAgo / 60)
        return `${hoursAgo}h ago`
      })

    this.yAxis = d3.axisLeft(this.yScale)

    // Add axes to chart
    this.g.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0,${this.height})`)
      .style('font-family', 'inherit')
      .call(this.xAxis)
      .selectAll('text')
      .style('text-anchor', 'end')
      .attr('dx', '-.8em')
      .attr('dy', '.15em')
      .attr('transform', 'rotate(-45)')

    this.g.append('g')
      .attr('class', 'y-axis')
      .style('font-family', 'inherit')
      .call(this.yAxis)

    // Add axis labels (x-axis label hidden)

    this.svg.append('text')
      .attr('class', 'y-label')
      .attr('text-anchor', 'middle')
      .attr('transform', 'rotate(-90)')
      .attr('x', -this.height / 2 - this.margin.top)
      .attr('y', 15)
      .text(this.metricsName)
      .style('fill', 'var(--color-text-primary)')

    // Create legend container below the chart
    this.legend = this.svg.append('g')
      .attr('class', 'legend')
      .attr('transform', `translate(${this.margin.left}, ${this.height + this.margin.top + 80})`)

    // Add grid lines
    this.g.append('g')
      .attr('class', 'grid-x')
      .attr('transform', `translate(0,${this.height})`)
      .call(d3.axisBottom(this.xScale)
        .ticks(10)
        .tickSize(-this.height)
        .tickFormat('')
      )
      .style('stroke-dasharray', '3,3')
      .style('opacity', 0.3)

    // Create tooltip
    this.tooltip = d3.select('body').append('div')
      .attr('class', 'chart-tooltip')
      .style('position', 'absolute')
      .style('visibility', 'hidden')
      .style('background', 'rgba(0, 0, 0, 0.8)')
      .style('color', 'white')
      .style('padding', '8px')
      .style('border-radius', '4px')
      .style('font-size', '12px')
      .style('pointer-events', 'none')
      .style('z-index', '1000')

    // Add invisible overlay for mouse tracking (only over chart area)
    this.overlay = this.g.append('rect')
      .attr('class', 'overlay')
      .attr('width', this.width)
      .attr('height', this.height)
      .style('fill', 'none')
      .style('pointer-events', 'all')
      .on('mouseover', () => this.tooltip.style('visibility', 'visible'))
      .on('mouseout', () => this.tooltip.style('visibility', 'hidden'))
      .on('mousemove', (event) => this.showTooltip(event))

    this.g.append('g')
      .attr('class', 'grid-y')
      .call(d3.axisLeft(this.yScale)
        .tickSize(-this.width)
        .tickFormat('')
      )
      .style('stroke-dasharray', '3,3')
      .style('opacity', 0.3)
  }

  getMetricName (key) {
    // Convert "ack_details" to "Ack", "deliver_details" to "Deliver", etc.
    return key.replace(/_details$/, '')
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ')
  }

  addMetric (metricKey) {
    if (this.metrics.has(metricKey)) return

    const colorIndex = this.metrics.size % this.colors.length
    const color = this.colors[colorIndex]

    const metricData = {
      name: this.getMetricName(metricKey),
      color,
      data: [],
      path: null,
      legendLine: null,
      legendText: null
    }

    // Create path for this metric
    if (this.stacked) {
      metricData.path = this.g.append('path')
        .attr('class', `metric-area-${metricKey}`)
        .attr('fill', color)
        .attr('fill-opacity', 0.7)
        .attr('stroke', color)
        .attr('stroke-width', 1)
        .style('pointer-events', 'all')
        .on('mouseover', () => this.tooltip.style('visibility', 'visible'))
        .on('mouseout', () => this.tooltip.style('visibility', 'hidden'))
        .on('mousemove', (event) => this.showTooltip(event))
    } else {
      metricData.path = this.g.append('path')
        .attr('class', `metric-line-${metricKey}`)
        .attr('fill', 'none')
        .attr('stroke', color)
        .attr('stroke-width', 2)
        .on('mouseover', () => this.tooltip.style('visibility', 'visible'))
        .on('mouseout', () => this.tooltip.style('visibility', 'hidden'))
        .on('mousemove', (event) => this.showTooltip(event))
    }

    this.metrics.set(metricKey, metricData)
    this.updateLegend()
  }

  updateLegend () {
    const itemWidth = 150
    const itemsPerRow = Math.floor(this.width / itemWidth) || 1

    // Convert Map to array for D3 data binding
    const metricsArray = Array.from(this.metrics.entries()).map(([key, metric]) => ({
      key,
      ...metric
    }))

    // Bind data to legend item groups
    const legendItems = this.legend.selectAll('.legend-item')
      .data(metricsArray, d => d.key)

    // Enter: create new legend items
    const legendEnter = legendItems.enter()
      .append('g')
      .attr('class', 'legend-item')

    legendEnter.append('line')
      .attr('class', 'legend-line')
      .attr('x1', 0)
      .attr('x2', 20)
      .attr('stroke-width', 4)

    legendEnter.append('text')
      .attr('class', 'legend-text')
      .attr('x', 25)
      .attr('y', 4)
      .style('font-size', '12px')
      .style('alignment-baseline', 'middle')
      .style('fill', 'var(--color-text-primary)')

    // Update: merge enter and existing selections
    const legendMerge = legendEnter.merge(legendItems)

    // Position all legend items
    legendMerge.attr('transform', (d, i) => {
      const row = Math.floor(i / itemsPerRow)
      const col = i % itemsPerRow
      const xOffset = col * itemWidth
      const yOffset = row * 25
      return `translate(${xOffset}, ${yOffset})`
    })

    // Update line colors
    legendMerge.select('.legend-line')
      .attr('stroke', d => d.color)

    // Update text content
    legendMerge.select('.legend-text')
      .text(d => `${d.name}: --`)

    // Exit: remove old legend items
    legendItems.exit().remove()

    // Store references back to metrics Map for value updates
    legendMerge.each((d, i, nodes) => {
      const metric = this.metrics.get(d.key)
      if (metric) {
        metric.legendLine = d3.select(nodes[i]).select('.legend-line')
        metric.legendText = d3.select(nodes[i]).select('.legend-text')
      }
    })
  }

  showTooltip (event) {
    const [mouseX] = d3.pointer(event, this.g.node())
    const dataIndex = Math.round(this.xScale.invert(mouseX))

    if (dataIndex < 0 || dataIndex >= this.maxDataPoints) return

    const secondsAgo = Math.max(0, this.maxDataPoints - dataIndex)
    const now = new Date()
    const timestamp = new Date(now.getTime() - secondsAgo * 1000)
    const timeLabel = timestamp.toLocaleString()

    let tooltipContent = `<b>${timeLabel}</b><br>`

    this.metrics.forEach((metric, key) => {
      const value = metric.data[dataIndex]
      const displayValue = value !== null ? Math.round(value).toLocaleString() : '--'
      tooltipContent += `${metric.name}: ${displayValue}<br>`
    })

    this.tooltip
      .html(tooltipContent)
      .style('left', (event.pageX + 10) + 'px')
      .style('top', (event.pageY - 10) + 'px')
  }

  update (newData) {
    // Process only metrics that end with "_details"
    Object.keys(newData).forEach(metricKey => {
      if (!metricKey.endsWith('_details')) return // Skip non-detail properties
      if (!newData[metricKey] || !newData[metricKey].log) return

      // Add metric if it doesn't exist
      if (!this.metrics.has(metricKey)) {
        this.addMetric(metricKey)
      }

      // Replace the entire data array with the new log values, front-filling with nulls
      const logData = newData[metricKey].log
      const nullsNeeded = this.maxDataPoints - logData.length
      const frontFilledData = new Array(nullsNeeded).fill(null).concat(logData)
      this.metrics.get(metricKey).data = frontFilledData
    })

    this.render()
  }

  createStackedData () {
    const metricsArray = Array.from(this.metrics.values())
    const stackedData = []

    for (let metricIndex = 0; metricIndex < metricsArray.length; metricIndex++) {
      const stackedMetric = []
      for (let i = 0; i < this.maxDataPoints; i++) {
        let cumulativeValue = 0
        let baseValue = 0

        // Calculate base value (sum of all previous metrics)
        for (let j = 0; j < metricIndex; j++) {
          const value = metricsArray[j].data[i]
          if (value !== null) {
            baseValue += value
          }
        }

        // Calculate top value (base + current metric)
        const currentValue = metricsArray[metricIndex].data[i]
        if (currentValue !== null) {
          cumulativeValue = baseValue + currentValue
        } else {
          cumulativeValue = null
          baseValue = null
        }

        stackedMetric.push([baseValue, cumulativeValue])
      }
      stackedData.push(stackedMetric)
    }

    return stackedData
  }

  render () {
    if (this.stacked) {
      // Stacked area chart
      const stackedData = this.createStackedData()

      // Get all values for scaling (use the top values of stacked data)
      let maxValue = 0
      stackedData.forEach(metricStack => {
        metricStack.forEach(([base, top]) => {
          if (top !== null && top > maxValue) {
            maxValue = top
          }
        })
      })

      // Update y-scale domain
      const padding = maxValue * 0.1 || 1
      this.yScale.domain([0, maxValue + padding])

      // Update areas for each metric
      let metricIndex = 0
      this.metrics.forEach((metric, key) => {
        metric.path
          .datum(stackedData[metricIndex])
          .attr('d', this.area)
        metricIndex++
      })
    } else {
      // Regular line chart
      let maxValue = 0
      this.metrics.forEach(metric => {
        metric.data.forEach(value => {
          if (value !== null && value > maxValue) {
            maxValue = value
          }
        })
      })

      // Update y-scale domain
      const padding = maxValue * 0.1 || 1
      this.yScale.domain([0, maxValue + padding])

      // Update lines for each metric
      this.metrics.forEach((metric, key) => {
        metric.path
          .datum(metric.data)
          .attr('d', this.line)
      })
    }

    // Update axes (common for both types)
    this.g.select('.y-axis')
      .call(this.yAxis)

    this.g.select('.x-axis')
      .call(this.xAxis)
      .selectAll('text')
      .style('text-anchor', 'end')
      .attr('dx', '-.8em')
      .attr('dy', '.15em')
      .attr('transform', 'rotate(-45)')

    // Update grid (common for both types)
    this.g.select('.grid-y')
      .transition()
      .duration(300)
      .call(d3.axisLeft(this.yScale)
        .tickSize(-this.width)
        .tickFormat('')
      )

    this.g.select('.grid-x')
      .transition()
      .duration(300)
      .call(d3.axisBottom(this.xScale)
        .ticks(10)
        .tickSize(-this.height)
        .tickFormat('')
      )

    // Update legend with current values (common for both types)
    this.metrics.forEach((metric, key) => {
      const currentValue = metric.data.length > 0 ? metric.data[metric.data.length - 1] : null
      const formattedValue = currentValue !== null ? Math.round(currentValue).toLocaleString() : '--'
      metric.legendText.text(`${metric.name}: ${formattedValue}`)
    })
  }

  destroy () {
    d3.select(`#${this.elementId}`).selectAll('*').remove()
    if (this.tooltip) {
      this.tooltip.remove()
    }
  }
}

export default Chart
