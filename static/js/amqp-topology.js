import './lib/d3.v7.min.js'

const d3 = window.d3

class AMQPTopologyGraph {
  constructor(elementId) {
    this.elementId = elementId
    const container = document.getElementById(elementId)
    this.width = container ? container.clientWidth : 1200
    this.height = container ? container.clientHeight : 600
    // Colors are defined in CSS custom properties

    this.nodes = []
    this.links = []
    this.highlightedNodeId = null
    this.animationId = null
    this.isHighlighting = false
    this.highlightTransitions = new Set()
    this.originalPositions = null
    this.filters = {
      showEmptyQueues: true,
      showInactiveLinks: true,
      showExchanges: true,
      searchTerm: ''
    }

    this.initGraph()
    this.initControls()
    this.initResizeHandler()
  }

  truncateText(text, maxLength = 20) {
    if (text.length <= maxLength) return text
    return text.substring(0, maxLength - 3) + '...'
  }

  initResizeHandler() {
    window.addEventListener('resize', () => {
      const container = document.getElementById(this.elementId)
      if (container) {
        this.width = container.clientWidth
        this.height = container.clientHeight
        this.svg.attr('viewBox', `0 0 ${this.width} ${this.height}`)
        setTimeout(() => this.fitToView(), 100)
      }
    })
  }

  async initGraph() {
    const container = d3.select(`#${this.elementId}`)
    container.selectAll('*').remove()

    this.svg = container.append('svg')
      .attr('width', '100%').attr('height', '100%')
      .attr('viewBox', `0 0 ${this.width} ${this.height}`)
      .attr('preserveAspectRatio', 'xMidYMid meet')

    this.zoom = d3.zoom().scaleExtent([0.1, 4])
      .on('zoom', (event) => this.g.attr('transform', event.transform))

    this.svg.call(this.zoom)
    this.g = this.svg.append('g')

    await this.loadData()
  }

  initControls() {
    const searchInput = document.getElementById('topology-search')

    if (searchInput) {
      searchInput.addEventListener('input', (e) => {
        this.filters.searchTerm = e.target.value.toLowerCase()
        this.applyFilters()
      })
    }

    const filters = ['show-empty-queues', 'show-inactive-links', 'show-exchanges']
    filters.forEach(filterId => {
      const checkbox = document.getElementById(filterId)
      if (checkbox) {
        checkbox.addEventListener('change', (e) => {
          const filterKey = filterId.replace(/-([a-z])/g, (g) => g[1].toUpperCase()).replace('show', 'show')
          this.filters[filterKey] = e.target.checked
          this.applyFilters()
        })
      }
    })

    const fitView = document.getElementById('fit-view')

    if (fitView) {
      fitView.addEventListener('click', () => this.fitToView())
    }

  }

  applyFilters() {
    if (!this.node || !this.link) return

    this.node.style('opacity', d => {
        if (this.filters.searchTerm && !d.name.toLowerCase().includes(this.filters.searchTerm)) {
        return 0.2
      }

      if (!this.filters.showExchanges && d.type === 'exchange') {
        return 0.2
      }

      if (!this.filters.showEmptyQueues && d.type === 'queue' && d.messages === 0) {
        return 0.2
      }

      return 1
    })

    this.link.style('opacity', d => {
      if (!this.filters.showInactiveLinks && (d.flowRate || 0) === 0) {
        return 0.2
      }
      return d._opacity || 0.8
    })

    this.g.selectAll('.flow-particle').style('opacity', d => {
      const link = this.links[d.linkIndex]
      if (!link) return 0

      if (!this.filters.showInactiveLinks && (link.flowRate || 0) === 0) {
        return 0
      }

      return d.visible ? 0.6 : 0
    })
  }


  async loadData() {
    try {
      const vhost = window.sessionStorage.getItem('vhost')
      const baseUrls = ['api/exchanges', 'api/queues', 'api/bindings']
      const urls = baseUrls.map(url => vhost && vhost !== '_all' ? `${url}/${encodeURIComponent(vhost)}` : url)

      const [exchanges, queues, bindings] = await Promise.all(
        urls.map(url => fetch(url).then(res => res.ok ? res.json() : Promise.reject()))
      )

      this.processData(exchanges, queues, bindings)
      this.renderGraph()
    } catch (error) {
      this.showError('Failed to load AMQP topology data.')
    }
  }

  processData(exchanges, queues, bindings) {
    this.nodes = [
      ...exchanges.filter(e => e.name).map(e => ({
        id: `exchange:${e.vhost}:${e.name}`, name: e.name, type: 'exchange', subtype: e.type,
        vhost: e.vhost, durable: e.durable, auto_delete: e.auto_delete, policy: e.policy
      })),
      ...queues.map(q => ({
        id: `queue:${q.vhost}:${q.name}`, name: q.name, type: 'queue', vhost: q.vhost,
        durable: q.durable, auto_delete: q.auto_delete, messages: q.messages || 0, 
        consumers: q.consumers || 0, policy: q.policy
      }))
    ]

    this.links = bindings.filter(b => b.source &&
      this.nodes.find(n => n.id === `exchange:${b.vhost}:${b.source}`) &&
      this.nodes.find(n => n.id === `${b.destination_type}:${b.vhost}:${b.destination}`)
    ).map(b => {
      let flowRate = 0

      if (b.destination_type === 'queue') {
        const queue = queues.find(q => q.vhost === b.vhost && q.name === b.destination)
        const sourceExchange = exchanges.find(e => e.vhost === b.vhost && e.name === b.source)

        if (queue?.message_stats?.deliver_get_details?.rate) {
          flowRate = queue.message_stats.deliver_get_details.rate
        }
        else if (sourceExchange?.message_stats?.publish_out_details?.rate) {
          flowRate = sourceExchange.message_stats.publish_out_details.rate
        }
        else if (sourceExchange?.message_stats?.publish_details?.rate) {
          flowRate = sourceExchange.message_stats.publish_details.rate
        }
      }

      return {
        source: `exchange:${b.vhost}:${b.source}`,
        target: `${b.destination_type}:${b.vhost}:${b.destination}`,
        routing_key: b.routing_key,
        arguments: b.arguments,
        vhost: b.vhost,
        flowRate
      }
    })

    this.calculateLayout()
    this.calculateLinkStyles()
  }

  calculateLayout() {
    const vhosts = [...new Set(this.nodes.map(n => n.vhost))].sort()
    const nodesByVhost = Object.fromEntries(vhosts.map(v => [v, {
      exchanges: this.nodes.filter(n => n.vhost === v && n.type === 'exchange'),
      queues: this.nodes.filter(n => n.vhost === v && n.type === 'queue')
    }]))

    const nodeSpacing = 80
    const boxGap = 120
    const standardBoxWidth = 500

    const vhostSpacing = standardBoxWidth + boxGap
    const totalWidth = vhosts.length * standardBoxWidth + (vhosts.length - 1) * boxGap
    const startX = (this.width - totalWidth) / 2

    this.vhostLabels = vhosts.map((vhost, i) => {
      const { exchanges, queues } = nodesByVhost[vhost]
      const boxCenterX = startX + (standardBoxWidth / 2) + i * vhostSpacing
      const vhostX = boxCenterX - 100

      const sortedExchanges = exchanges.sort((a, b) => a.name.localeCompare(b.name))
      const sortedQueues = queues.sort((a, b) => a.name.localeCompare(b.name))

      sortedExchanges.forEach((node, j) => Object.assign(node, {
        x: vhostX,
        y: 240 + j * nodeSpacing,
        fx: vhostX,
        fy: 240 + j * nodeSpacing
      }))

      sortedQueues.forEach((node, j) => Object.assign(node, {
        x: vhostX + 200,
        y: 240 + j * nodeSpacing,
        fx: vhostX + 200,
        fy: 240 + j * nodeSpacing
      }))

      return { vhost, x: boxCenterX, y: 160 }
    })
  }

  calculateLinkStyles() {
    const maxFlow = Math.max(...this.links.map(l => l.flowRate || 0))
    this.links.forEach(link => {
      const rate = link.flowRate || 0
      const intensity = rate / maxFlow
      link._color = rate === 0 ? 'var(--color-topology-flow-inactive)' : intensity < 0.5 ? 'var(--color-topology-flow-medium)' : 'var(--color-topology-flow-active)'
      link._dash = rate === 0 ? '5,5' : null
      link._opacity = rate === 0 ? 0.4 : 0.8
    })
  }

  renderGraph() {
    this.stopParticleAnimation()
    this.g.selectAll('*').remove()
    this.svg.selectAll('.topology-legend-fixed').remove()

    if (this.nodes.length > 200) {
      this.showLargeGraphWarning()
      return
    }

    this.nodeMap = new Map(this.nodes.map(n => [n.id, n]))

    this.link = this.g.selectAll('.topology-link').data(this.links).enter().append('line')
      .attr('class', 'topology-link').attr('stroke-width', 2)
      .attr('stroke', d => d._color).attr('stroke-dasharray', d => d._dash).attr('opacity', d => d._opacity)

    this.link.append('title').text(d => `${d.source.split(':').pop()} → ${d.target.split(':').pop()}\nFlow: ${d.flowRate.toFixed(2)}/sec`)

    if (this.vhostLabels.length > 1) {
      this.g.selectAll('.topology-vhost-box').data(this.vhostLabels).enter().append('rect')
        .attr('class', 'topology-vhost-box')
        .attr('x', d => {
          const vhostNodes = this.nodes.filter(n => n.vhost === d.vhost)
          const minX = Math.min(...vhostNodes.map(n => n.x))
          const maxX = Math.max(...vhostNodes.map(n => n.x))
          const contentWidth = (maxX - minX) + 180 // Actual content width including text
          const minBoxWidth = Math.max(400, contentWidth + 80) // Ensure box is wide enough
          const padding = 40
          return minX - padding
        })
        .attr('y', d => {
          const vhostNodes = this.nodes.filter(n => n.vhost === d.vhost)
          const minY = Math.min(...vhostNodes.map(n => n.y))
          const labelY = d.y + 10 // Label position
          return labelY - 30 // Start box 30px above the label for top padding
        })
        .attr('width', d => {
          const vhostNodes = this.nodes.filter(n => n.vhost === d.vhost)
          const minX = Math.min(...vhostNodes.map(n => n.x))
          const maxX = Math.max(...vhostNodes.map(n => n.x))
          const contentWidth = (maxX - minX) + 180
          return Math.max(400, contentWidth + 80)
        })
        .attr('height', d => {
          const vhostNodes = this.nodes.filter(n => n.vhost === d.vhost)
          const minY = Math.min(...vhostNodes.map(n => n.y))
          const maxY = Math.max(...vhostNodes.map(n => n.y))
          const labelY = d.y + 10
          const boxStartY = labelY - 30
          return (maxY - boxStartY) + 60 // From box start to bottom of nodes + padding
        })
        .attr('rx', 8)
        .attr('ry', 8)
    }

    this.node = this.g.selectAll('.node').data(this.nodes).enter().append('g')
      .attr('class', 'node').attr('transform', d => `translate(${d.x},${d.y})`)
      .style('cursor', 'pointer').on('click', (e, d) => {
        e.stopPropagation()
        if (this.highlightedNodeId === d.id) {
          this.clearHighlight()
        } else {
          this.highlightConnected(d)
          this.showNodeDetails(d)
        }
      })

    this.node.append('circle').attr('class', 'topology-node-circle')
      .attr('r', d => d.type === 'exchange' ? 20 : 15)
      .attr('fill', d => `var(--color-topology-${d.type})`)

    this.node.append('text').attr('dx', 25).attr('dy', '.35em')
      .attr('class', 'topology-node-text')
      .style('font-size', '11px').style('font-family', 'Inter, sans-serif')
      .style('font-weight', '500')
      .text(d => this.truncateText(d.name, 20))

    this.node.append('title').text(d => d.type === 'exchange' ?
      `Exchange: ${d.name}\nType: ${d.subtype}\nVHost: ${d.vhost}\nDurable: ${d.durable}` :
      `Queue: ${d.name}\nVHost: ${d.vhost}\nMessages: ${d.messages}\nConsumers: ${d.consumers}\nDurable: ${d.durable}`)

    this.g.selectAll('.topology-vhost-label').data(this.vhostLabels).enter().append('text')
      .attr('class', 'topology-vhost-label')
      .attr('x', d => {
        const vhostNodes = this.nodes.filter(n => n.vhost === d.vhost)
        const minX = Math.min(...vhostNodes.map(n => n.x))
        const maxX = Math.max(...vhostNodes.map(n => n.x))
        const boxWidth = Math.max(400, (maxX - minX) + 180 + 80)
        const boxStartX = minX - 40
        return boxStartX + (boxWidth / 2)
      })
      .attr('y', d => d.y + 10)
      .attr('text-anchor', 'middle').style('font-size', '16px').style('font-weight', 'bold')
      .style('font-family', 'Inter, sans-serif').text(d => d.vhost)

    this.addLegend()
    this.updateLinkPositions()

    this.startParticleAnimation()

    // Fit to view with a small delay to ensure everything is rendered
    setTimeout(() => this.fitToView(), 100)
  }

  updateLinkPositions() {
    this.link.attr('x1', d => this.nodeMap.get(d.source)?.x || 0)
      .attr('y1', d => this.nodeMap.get(d.source)?.y || 0)
      .attr('x2', d => this.nodeMap.get(d.target)?.x || 0)
      .attr('y2', d => this.nodeMap.get(d.target)?.y || 0)
  }

  highlightConnected(selectedNode) {
    // Clear any existing highlight transitions
    this.clearHighlightTransitions()

    this.highlightedNodeId = selectedNode.id
    this.isHighlighting = true

    if (!this.originalPositions) {
      this.originalPositions = Object.fromEntries(this.nodes.map(n => [n.id, { x: n.x, y: n.y }]))
    }

    const connectedIds = new Set([selectedNode.id])
    const connectedNodes = [selectedNode]

    this.links.forEach(link => {
      if (link.source === selectedNode.id) {
        connectedIds.add(link.target)
        connectedNodes.push(this.nodes.find(n => n.id === link.target))
      }
      if (link.target === selectedNode.id) {
        connectedIds.add(link.source)
        connectedNodes.push(this.nodes.find(n => n.id === link.source))
      }
    })

    this.arrangeConnectedNodes(connectedNodes.filter(Boolean))
    this.applyHighlightOpacity(connectedIds, selectedNode)
    this.svg.on('click', (e) => {
      // Clear highlight on any background click
      this.clearHighlight()
      this.closePopup()
    })
  }

  arrangeConnectedNodes(nodes) {
    const [exchanges, queues] = [nodes.filter(n => n.type === 'exchange'), nodes.filter(n => n.type === 'queue')]
    const [centerX, centerY] = [this.width / 2, this.height / 2]

    exchanges.forEach((node, i) => Object.assign(node, { x: centerX - 150, y: centerY - (exchanges.length - 1) * 40 + i * 80 }))
    queues.forEach((node, i) => Object.assign(node, { x: centerX + 150, y: centerY - (queues.length - 1) * 40 + i * 80 }))

    this.animateToPositions()
  }

  applyHighlightOpacity(connectedIds, selectedNode) {
    const duration = 250 // Standardized duration

    // Track all transitions to manage them properly
    const nodeTransition = this.node.transition().duration(duration).style('opacity', d => connectedIds.has(d.id) ? 1 : 0)
    const linkTransition = this.link.transition().duration(duration).style('opacity', d => connectedIds.has(d.source) && connectedIds.has(d.target) ? 1 : 0)
    const particleTransition = this.g.selectAll('.flow-particle').transition().duration(duration)
      .style('opacity', d => {
        const link = this.links[d.linkIndex]
        return link && connectedIds.has(link.source) && connectedIds.has(link.target) && d.visible ? 0.6 : 0
      })

    // Hide all vhost boxes and labels except those containing connected nodes
    const selectedVhost = selectedNode.vhost
    const boxTransition = this.g.selectAll('.topology-vhost-box').transition().duration(duration)
      .style('opacity', d => d.vhost === selectedVhost ? 1 : 0.1)
    const labelTransition = this.g.selectAll('.topology-vhost-label').transition().duration(duration)
      .style('opacity', d => d.vhost === selectedVhost ? 1 : 0.1)

    // Store transitions for cleanup
    this.highlightTransitions.add(nodeTransition)
    this.highlightTransitions.add(linkTransition)
    this.highlightTransitions.add(particleTransition)
    this.highlightTransitions.add(boxTransition)
    this.highlightTransitions.add(labelTransition)

    // Remove from set when completed
    ;[nodeTransition, linkTransition, particleTransition, boxTransition, labelTransition].forEach(t => {
      t.on('end interrupt', () => this.highlightTransitions.delete(t))
    })
  }

  clearHighlight() {
    // Clear any ongoing highlight transitions first
    this.clearHighlightTransitions()

    this.highlightedNodeId = null
    this.isHighlighting = false

    if (this.originalPositions) {
      this.nodes.forEach(node => Object.assign(node, this.originalPositions[node.id]))
      this.animateToPositions()
      this.originalPositions = null // Clean up memory
    }

    const duration = 250 // Standardized duration
    this.node.transition().duration(duration).style('opacity', 1)
    this.link.transition().duration(duration).style('opacity', d => d._opacity || 0.8).attr('stroke', d => d._color)
    this.g.selectAll('.flow-particle').transition().duration(duration).style('opacity', d => d.visible ? 0.6 : 0)

    // Restore vhost boxes and labels when clearing highlight
    this.g.selectAll('.topology-vhost-box').transition().duration(duration).style('opacity', 1)
    this.g.selectAll('.topology-vhost-label').transition().duration(duration).style('opacity', 1)

    this.svg.on('click', null)

    // Hide the legend when nothing is highlighted
    this.closePopup()
  }

  animateToPositions() {
    const duration = 400 // Standardized duration for positioning
    let transitionsComplete = 0
    const totalTransitions = 2

    const onComplete = () => {
      transitionsComplete++
      if (transitionsComplete >= totalTransitions) {
        this.updateLinkPositions()
      }
    }

    this.node.transition().duration(duration).attr('transform', d => `translate(${d.x},${d.y})`)
      .on('end', onComplete)
    this.link.transition().duration(duration)
      .attr('x1', d => this.nodeMap.get(d.source)?.x || 0).attr('y1', d => this.nodeMap.get(d.source)?.y || 0)
      .attr('x2', d => this.nodeMap.get(d.target)?.x || 0).attr('y2', d => this.nodeMap.get(d.target)?.y || 0)
      .on('end', onComplete)
  }

  fitToView() {
    if (!this.nodes.length) return

    const margin = 80
    const nodeRadius = 20 // Maximum node radius
    const textPadding = 25 // Text offset from node center (from dx: 25)
    const estimatedTextWidth = 120 // Conservative estimate for truncated text width
    
    const bounds = this.nodes.reduce((acc, n) => ({
      // Include node radius and text width in bounds calculation
      minX: Math.min(acc.minX, n.x - nodeRadius), 
      maxX: Math.max(acc.maxX, n.x + textPadding + estimatedTextWidth),
      minY: Math.min(acc.minY, n.y - nodeRadius), 
      maxY: Math.max(acc.maxY, n.y + nodeRadius)
    }), { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity })

    // Account for vhost boxes if they exist
    if (this.vhostLabels && this.vhostLabels.length > 1) {
      this.vhostLabels.forEach(label => {
        const vhostNodes = this.nodes.filter(n => n.vhost === label.vhost)
        if (vhostNodes.length > 0) {
          const vhostMinX = Math.min(...vhostNodes.map(n => n.x))
          const vhostMaxX = Math.max(...vhostNodes.map(n => n.x))
          const contentWidth = (vhostMaxX - vhostMinX) + 180
          const boxWidth = Math.max(400, contentWidth + 80)
          const boxStartX = vhostMinX - 40
          
          bounds.minX = Math.min(bounds.minX, boxStartX)
          bounds.maxX = Math.max(bounds.maxX, boxStartX + boxWidth)
          
          const labelY = label.y + 10
          const boxStartY = labelY - 30
          const vhostMinY = Math.min(...vhostNodes.map(n => n.y))
          const vhostMaxY = Math.max(...vhostNodes.map(n => n.y))
          const boxHeight = (vhostMaxY - boxStartY) + 60
          
          bounds.minY = Math.min(bounds.minY, boxStartY)
          bounds.maxY = Math.max(bounds.maxY, boxStartY + boxHeight)
        }
      })
    }

    const [minX, maxX, minY, maxY] = [bounds.minX - margin, bounds.maxX + margin, bounds.minY - margin, bounds.maxY + margin]
    const scale = Math.min(this.width / (maxX - minX), this.height / (maxY - minY), 1)
    const [centerX, centerY] = [(minX + maxX) / 2, (minY + maxY) / 2]
    const transform = d3.zoomIdentity.translate(this.width / 2 - centerX * scale, this.height / 2 - centerY * scale).scale(scale)

    this.svg.transition().duration(750).call(this.zoom.transform, transform)
  }

  addLegend() {
    const legend = this.svg.append('g').attr('class', 'topology-legend-fixed')
    const [x, y] = [10, this.height - 90]
    const legendWidth = 200 // More fitted to text content

    legend.append('rect').attr('x', x).attr('y', y - 5).attr('width', legendWidth).attr('height', 85)
      .attr('class', 'topology-legend-bg').attr('rx', 5)

    legend.append('text').attr('x', x + 5).attr('y', y + 10).style('font-size', '12px')
      .style('font-weight', 'bold').style('font-family', 'Inter, sans-serif')
      .attr('class', 'topology-legend-title').text('Message Flow Legend')

    const legendData = [
      { y: 25, color: 'var(--color-topology-flow-active)', dash: null, text: 'High activity' },
      { y: 40, color: 'var(--color-topology-flow-medium)', dash: null, text: 'Activity' },
      { y: 55, color: 'var(--color-topology-flow-inactive)', dash: '5,5', text: 'No activity' }
    ]

    legendData.forEach(item => {
      legend.append('line').attr('x1', x + 5).attr('y1', y + item.y).attr('x2', x + 35).attr('y2', y + item.y)
        .attr('stroke', item.color).attr('stroke-width', 2).attr('stroke-dasharray', item.dash).attr('opacity', item.dash ? 0.6 : 1)
      legend.append('text').attr('x', x + 40).attr('y', y + item.y + 5).style('font-size', '10px')
        .style('font-family', 'Inter, sans-serif').attr('class', 'topology-legend-text').text(item.text)
    })

    legend.append('text').attr('x', x + 5).attr('y', y + 75).style('font-size', '9px')
      .style('font-family', 'Inter, sans-serif').attr('class', 'topology-legend-subtitle').text('Animated lines show message flow')
  }

  startParticleAnimation() {
    this.animationId && cancelAnimationFrame(this.animationId)
    this.g.selectAll('.flow-particle').remove()

    this.link.style('stroke-dasharray', d => {
      if (d.flowRate > 0) return '5,3'
      return d._dash || 'none'
    })

    this.dashOffset = 0
    this.animateFlow()
  }


  animateFlow() {
    this.dashOffset -= 0.5

    this.link.style('stroke-dashoffset', d => {
      if (d.flowRate > 0) {
        const maxFlow = Math.max(...this.links.map(l => l.flowRate || 0))
        const speed = 0.5 + (d.flowRate / maxFlow) * 2
        return this.dashOffset * speed
      }
      return 0
    })

    this.animationId = requestAnimationFrame(() => this.animateFlow())
  }

  stopParticleAnimation() {
    this.animationId && cancelAnimationFrame(this.animationId)
    // Only restore styling if links exist
    if (this.link) {
      this.link.style('stroke-dasharray', d => d._dash || 'none').style('stroke-dashoffset', 0)
    }
    this.animationId = null
  }

  showError(message) {
    d3.select(`#${this.elementId}`).selectAll('*').remove()
      .append('div').style('padding', '20px').style('text-align', 'center')
      .style('color', '#d12771').text(message)
  }

  clearHighlightTransitions() {
    this.highlightTransitions.forEach(transition => {
      try {
        transition.interrupt()
      } catch (e) {}
    })
    this.highlightTransitions.clear()
  }


  showNodeDetails(node) {
    const popup = document.getElementById('topology-info-popup')
    if (!popup) return

    // Get connected nodes for this node
    const connections = this.getNodeConnections(node)

    // Update title
    const title = document.getElementById('popup-title')
    if (title) {
      title.textContent = `${node.type === 'exchange' ? '📨' : '📦'} ${node.name}`
    }

    // Update node info fields
    const nodeType = document.getElementById('node-type')
    if (nodeType) {
      nodeType.textContent = node.type + (node.subtype ? ` (${node.subtype})` : '')
    }

    const nodeVhost = document.getElementById('node-vhost')
    if (nodeVhost) {
      nodeVhost.textContent = node.vhost
    }

    const nodeDurable = document.getElementById('node-durable')
    if (nodeDurable) {
      nodeDurable.textContent = node.durable ? 'Yes' : 'No'
    }

    // Policy field (show/hide based on existence)
    const policyGroup = document.getElementById('policy-group')
    const nodePolicy = document.getElementById('node-policy')
    if (node.policy && policyGroup && nodePolicy) {
      policyGroup.style.display = 'block'
      nodePolicy.textContent = node.policy
    } else if (policyGroup) {
      policyGroup.style.display = 'none'
    }

    // Queue-specific fields
    const messagesGroup = document.getElementById('queue-messages-group')
    const nodeMessages = document.getElementById('node-messages')
    const consumersGroup = document.getElementById('queue-consumers-group')
    const nodeConsumers = document.getElementById('node-consumers')
    
    if (node.type === 'queue') {
      if (messagesGroup && nodeMessages) {
        messagesGroup.style.display = 'block'
        nodeMessages.textContent = node.messages.toLocaleString()
        nodeMessages.className = node.messages > 0 ? 'text-warning' : ''
      }
      if (consumersGroup && nodeConsumers) {
        consumersGroup.style.display = 'block'
        nodeConsumers.textContent = node.consumers
        nodeConsumers.className = node.consumers === 0 ? 'text-muted' : ''
      }
    } else {
      if (messagesGroup) messagesGroup.style.display = 'none'
      if (consumersGroup) consumersGroup.style.display = 'none'
    }

    // Update connections sections
    this.updateConnectionsSection('incoming', connections.incoming, '📥')
    this.updateConnectionsSection('outgoing', connections.outgoing, '📤')

    // Show/hide no-connections section
    const noConnectionsSection = document.getElementById('no-connections')
    const noConnectionsType = document.getElementById('no-connections-type')
    const defaultMessage = document.getElementById('default-message')
    
    if (connections.incoming.length === 0 && connections.outgoing.length === 0) {
      if (noConnectionsSection) noConnectionsSection.style.display = 'block'
      if (noConnectionsType) noConnectionsType.textContent = node.type
      if (defaultMessage) defaultMessage.style.display = 'none'
    } else {
      if (noConnectionsSection) noConnectionsSection.style.display = 'none'
      if (defaultMessage) defaultMessage.style.display = 'none'
    }

    popup.classList.add('show')
  }

  updateConnectionsSection(type, connections, icon) {
    const section = document.getElementById(`${type}-connections`)
    const count = document.getElementById(`${type}-count`)
    const list = document.getElementById(`${type}-list`)

    if (!section || !count || !list) return

    if (connections.length > 0) {
      section.style.display = 'block'
      count.textContent = connections.length

      // Clear existing items
      list.innerHTML = ''

      // Add connection items
      connections.forEach(conn => {
        const li = document.createElement('li')
        
        const nameDiv = document.createElement('div')
        nameDiv.className = 'connection-name'
        nameDiv.textContent = type === 'incoming' ? conn.source : conn.target
        li.appendChild(nameDiv)

        if (conn.routing_key) {
          const detailDiv = document.createElement('div')
          detailDiv.className = 'connection-detail'
          detailDiv.textContent = `🔑 ${conn.routing_key}`
          li.appendChild(detailDiv)
        }

        const flowDiv = document.createElement('div')
        if (conn.flowRate > 0) {
          flowDiv.className = 'connection-flow'
          flowDiv.textContent = `⚡ ${conn.flowRate.toFixed(2)} msg/sec`
        } else {
          flowDiv.className = 'connection-flow-inactive'
          flowDiv.textContent = '💤 No activity'
        }
        li.appendChild(flowDiv)

        list.appendChild(li)
      })

      // Add collapsible functionality
      const header = section.querySelector('.collapsible-header')
      if (header) {
        header.onclick = () => {
          const toggleIcon = header.querySelector('.toggle-icon')
          if (list.style.display === 'none') {
            list.style.display = 'block'
            if (toggleIcon) toggleIcon.textContent = '▼'
          } else {
            list.style.display = 'none'
            if (toggleIcon) toggleIcon.textContent = '▶'
          }
        }
      }
    } else {
      section.style.display = 'none'
    }
  }

  getNodeConnections(node) {
    const incoming = []
    const outgoing = []

    this.links.forEach(link => {
      if (link.target === node.id) {
        incoming.push({
          source: link.source.split(':').pop(),
          routing_key: link.routing_key,
          flowRate: link.flowRate || 0
        })
      }
      if (link.source === node.id) {
        outgoing.push({
          target: link.target.split(':').pop(),
          routing_key: link.routing_key,
          flowRate: link.flowRate || 0
        })
      }
    })

    return { incoming, outgoing }
  }


  closePopup() {
    const popup = document.getElementById('topology-info-popup')
    if (popup) {
      popup.classList.remove('show')
    }
  }

  showLargeGraphWarning() {
    const warningGroup = this.svg.append('g').attr('class', 'topology-warning')

    // Background rectangle
    warningGroup.append('rect')
      .attr('x', this.width / 2 - 280)
      .attr('y', this.height / 2 - 120)
      .attr('width', 560)
      .attr('height', 240)
      .attr('rx', 10)
      .attr('class', 'topology-legend-bg')
      .attr('stroke-width', 2)

    // Warning icon
    warningGroup.append('text')
      .attr('x', this.width / 2)
      .attr('y', this.height / 2 - 40)
      .attr('text-anchor', 'middle')
      .style('font-size', '48px')
      .text('⚠️')

    // Warning title
    warningGroup.append('text')
      .attr('x', this.width / 2)
      .attr('y', this.height / 2 - 10)
      .attr('text-anchor', 'middle')
      .attr('class', 'topology-node-text')
      .style('font-size', '18px')
      .style('font-weight', 'bold')
      .text('Too Many Nodes')

    // Warning message
    warningGroup.append('text')
      .attr('x', this.width / 2)
      .attr('y', this.height / 2 + 20)
      .attr('text-anchor', 'middle')
      .attr('class', 'topology-node-text')
      .style('font-size', '14px')
      .text(`This topology has ${this.nodes.length} nodes.`)

    warningGroup.append('text')
      .attr('x', this.width / 2)
      .attr('y', this.height / 2 + 40)
      .attr('text-anchor', 'middle')
      .attr('class', 'topology-node-text')
      .style('font-size', '14px')
      .text('Topology view is disabled for graphs with more than 200 nodes')

    warningGroup.append('text')
      .attr('x', this.width / 2)
      .attr('y', this.height / 2 + 60)
      .attr('text-anchor', 'middle')
      .attr('class', 'topology-node-text')
      .style('font-size', '14px')
      .text('to maintain browser performance.')
  }

  async refresh() { await this.loadData() }
}

window.AMQPTopologyGraph = AMQPTopologyGraph

let topologyGraph
document.addEventListener('DOMContentLoaded', () => {
  topologyGraph = new AMQPTopologyGraph('topology-graph')
})
