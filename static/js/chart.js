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

    // Chart dimensions and margins (extra bottom margin for legend)
    this.margin = { top: 20, right: 20, bottom: 100, left: 60 }
    this.width = 600 - this.margin.left - this.margin.right
    this.height = 400 - this.margin.top - this.margin.bottom

    this.initChart()
  }

  initChart () {
    const container = d3.select(`#${this.elementId}`)

    // Clear any existing content
    container.selectAll('*').remove()

    // Create SVG with viewBox for scaling
    this.svg = container
      .append('svg')
      .attr('viewBox', `0 0 ${this.width + this.margin.left + this.margin.right} ${this.height + this.margin.top + this.margin.bottom}`)

    // Create main group
    this.g = this.svg
      .append('g')
      .attr('transform', `translate(${this.margin.left},${this.margin.top})`)

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
      .tickFormat(d => {
        const secondsAgo = Math.max(0, this.maxDataPoints - d)
        const now = new Date()
        const timePoint = new Date(now.getTime() - secondsAgo * 1000)
        return timePoint.toLocaleTimeString('en-US', { hour12: false })
      })

    this.yAxis = d3.axisLeft(this.yScale)

    // Add axes to chart
    this.g.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0,${this.height})`)
      .style('font-family', 'inherit')
      .call(this.xAxis)

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
      .attr('transform', `translate(${this.margin.left}, ${this.height + this.margin.top + 40})`)

    // Add grid lines
    this.g.append('g')
      .attr('class', 'grid-x')
      .attr('transform', `translate(0,${this.height})`)
      .call(d3.axisBottom(this.xScale)
        .tickSize(-this.height)
        .tickFormat('')
      )
      .style('stroke-dasharray', '3,3')
      .style('opacity', 0.3)

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

    const color = this.colors[this.metrics.size % this.colors.length]

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
    } else {
      metricData.path = this.g.append('path')
        .attr('class', `metric-line-${metricKey}`)
        .attr('fill', 'none')
        .attr('stroke', color)
        .attr('stroke-width', 2)
    }

    this.metrics.set(metricKey, metricData)
    this.updateLegend()
  }

  updateLegend () {
    // Clear existing legend
    this.legend.selectAll('*').remove()

    let xOffset = 0
    let yOffset = 0
    const itemWidth = 150
    const itemsPerRow = Math.floor(this.width / itemWidth) || 1

    let itemCount = 0
    this.metrics.forEach((metric, key) => {
      // Calculate position with wrapping
      if (itemCount > 0 && itemCount % itemsPerRow === 0) {
        xOffset = 0
        yOffset += 25
      }

      // Add legend line
      metric.legendLine = this.legend.append('line')
        .attr('x1', xOffset)
        .attr('x2', xOffset + 20)
        .attr('y1', yOffset)
        .attr('y2', yOffset)
        .attr('stroke', metric.color)
        .attr('stroke-width', 4)

      // Add legend text
      metric.legendText = this.legend.append('text')
        .attr('x', xOffset + 25)
        .attr('y', yOffset + 4)
        .text(`${metric.name}: --`)
        .style('font-size', '12px')
        .style('alignment-baseline', 'middle')
        .style('fill', 'var(--color-text-primary)')

      xOffset += itemWidth
      itemCount++
    })
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
        .tickSize(-this.height)
        .tickFormat('')
      )

    // Update legend with current values (common for both types)
    this.metrics.forEach((metric, key) => {
      const currentValue = metric.data.length > 0 ? metric.data[metric.data.length - 1] : null
      metric.legendText.text(`${metric.name}: ${currentValue !== null ? Math.round(currentValue) : '--'}`)
    })
  }

  destroy () {
    d3.select(`#${this.elementId}`).selectAll('*').remove()
  }
}

export default Chart
