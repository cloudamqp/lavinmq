import './lib/d3.v7.min.js'

// D3 is now available as a global variable
const d3 = window.d3

class Chart {
  constructor (elementId, metricsName = 'msgs/s') {
    this.elementId = elementId
    this.metricsName = metricsName
    this.maxDataPoints = 600 // Fixed 600 seconds on x-axis
    this.metrics = new Map() // Store metric data and metadata
    this.colors = ['#54be7e', '#4589ff', '#d12771', '#d2a106', '#08bdba', '#bae6ff', '#ba4e00', '#d4bbff', '#8a3ffc', '#33b1ff', '#007d79']

    // Chart dimensions and margins
    this.margin = { top: 20, right: 150, bottom: 40, left: 60 }
    this.width = 800 - this.margin.left - this.margin.right
    this.height = 400 - this.margin.top - this.margin.bottom

    this.initChart()
  }

  initChart () {
    const container = d3.select(`#${this.elementId}`)

    // Clear any existing content
    container.selectAll('*').remove()

    // Create SVG
    this.svg = container
      .append('svg')
      .attr('width', this.width + this.margin.left + this.margin.right)
      .attr('height', this.height + this.margin.top + this.margin.bottom)

    // Create main group
    this.g = this.svg
      .append('g')
      .attr('transform', `translate(${this.margin.left},${this.margin.top})`)

    // Create scales
    this.xScale = d3.scaleLinear()
      .domain([0, this.maxDataPoints - 1])
      .range([0, this.width])

    this.yScale = d3.scaleLinear()
      .domain([0, 10]) // Initial domain, will be updated
      .range([this.height, 0])

    // Create line generator
    this.line = d3.line()
      .x((d, i) => this.xScale(i))
      .y(d => this.yScale(d))
      .curve(d3.curveMonotoneX)

    // Create axes
    this.xAxis = d3.axisBottom(this.xScale)
      .tickFormat(d => {
        const secondsAgo = Math.max(0, this.maxDataPoints - d - 1)
        const now = new Date()
        const timePoint = new Date(now.getTime() - secondsAgo * 1000)
        return timePoint.toLocaleTimeString('en-US', { hour12: false })
      })

    this.yAxis = d3.axisLeft(this.yScale)

    // Add axes to chart
    this.g.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0,${this.height})`)
      .call(this.xAxis)

    this.g.append('g')
      .attr('class', 'y-axis')
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

    // Create legend container
    this.legend = this.svg.append('g')
      .attr('class', 'legend')
      .attr('transform', `translate(${this.width + this.margin.left + 10}, ${this.margin.top + 20})`)

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
    metricData.path = this.g.append('path')
      .attr('class', `metric-line-${metricKey}`)
      .attr('fill', 'none')
      .attr('stroke', color)
      .attr('stroke-width', 2)

    this.metrics.set(metricKey, metricData)
    this.updateLegend()
  }

  updateLegend () {
    // Clear existing legend
    this.legend.selectAll('*').remove()

    let yOffset = 0
    this.metrics.forEach((metric, key) => {
      // Add legend line
      metric.legendLine = this.legend.append('line')
        .attr('x1', 0)
        .attr('x2', 20)
        .attr('y1', yOffset)
        .attr('y2', yOffset)
        .attr('stroke', metric.color)
        .attr('stroke-width', 4)

      // Add legend text
      metric.legendText = this.legend.append('text')
        .attr('x', 25)
        .attr('y', yOffset + 3)
        .text(`${metric.name}: --`)
        .style('font-size', '12px')
        .style('alignment-baseline', 'middle')
        .style('fill', 'var(--color-text-primary)')

      yOffset += 25
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

      // Replace the entire data array with the new log values
      this.metrics.get(metricKey).data = [...newData[metricKey].log]
    })

    this.render()
  }

  render () {
    // Get all values for scaling
    let allValues = []
    this.metrics.forEach(metric => {
      allValues = allValues.concat(metric.data)
    })

    if (allValues.length === 0) return

    // Update y-scale domain
    const maxValue = d3.max(allValues) || 10
    const minValue = d3.min(allValues) || 0
    const padding = (maxValue - minValue) * 0.1 || 1

    this.yScale.domain([
      0,
      maxValue + padding
    ])

    // Update x-scale domain based on actual data length
    const maxDataLength = Math.max(...Array.from(this.metrics.values()).map(m => m.data.length))
    const actualXDomain = Math.min(maxDataLength - 1, this.maxDataPoints - 1)

    this.xScale.domain([0, actualXDomain])

    // Update axes
    this.g.select('.y-axis')
      .call(this.yAxis)

    this.g.select('.x-axis')
      .call(this.xAxis)

    // Update grid
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

    // Update lines for each metric
    this.metrics.forEach((metric, key) => {
      metric.path
        .datum(metric.data)
        .transition()
        .duration(300)
        .attr('d', this.line)

      // Update legend with current value (last value in array)
      const currentValue = metric.data.length > 0 ? metric.data[metric.data.length - 1] : null
      metric.legendText.text(`${metric.name}: ${currentValue !== null ? Math.round(currentValue) : '--'}`)
    })
  }

  destroy () {
    d3.select(`#${this.elementId}`).selectAll('*').remove()
  }
}

export default Chart
