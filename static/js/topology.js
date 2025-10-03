import './lib/d3.v7.min.js'
import * as HTTP from './http.js'

const d3 = window.d3

// Configuration constants
const CONFIG = {
  DEFAULT_WIDTH: 1200,
  DEFAULT_HEIGHT: 600,
  NODE_SPACING: 80,
  VHOST_SPACING: 620,
  MARGINS: { DEFAULT: 80, VHOST: { X: 40, Y: 60 } },
  NODES: { EXCHANGE_RADIUS: 20, QUEUE_RADIUS: 15, NAME_MAX_LENGTH: 20 },
  LAYOUT: { START_SCALE_FACTOR: 1.8, BASE_X_OFFSET: 200 },
  MAX_NODES_THRESHOLD: 200
}

// Global state
let nodes = [], links = [], nodeMap = new Map()
let svg, g, zoom, nodeElements, linkElements
let selectedNode = null
let width = CONFIG.DEFAULT_WIDTH, height = CONFIG.DEFAULT_HEIGHT

// Cached DOM elements
const domCache = {}
function getCachedElement (id) {
  if (!domCache[id]) domCache[id] = document.getElementById(id)
  return domCache[id]
}

const filters = { searchTerm: '' }
document.addEventListener('DOMContentLoaded', init)

function init () {
  const container = getCachedElement('topology-graph')
  if (!container) return

  width = container.clientWidth || CONFIG.DEFAULT_WIDTH
  height = container.clientHeight || CONFIG.DEFAULT_HEIGHT

  setupSVG()
  setupControls()
  setupResize()
  loadData()

  // Refresh every 5 seconds
  setInterval(loadData, 5000)
}

function setupSVG () {
  const container = d3.select('#topology-graph')
  container.selectAll('*').remove()

  svg = container.append('svg')
    .attr('width', '100%')
    .attr('height', '100%')
    .attr('viewBox', `0 0 ${width} ${height}`)

  zoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on('zoom', event => g.attr('transform', event.transform))

  svg.call(zoom)
    .on('dblclick.zoom', null) // Disable double-click zoom

  g = svg.append('g')

  // Background click to clear selection
  svg.on('click', event => {
    // Only clear if not clicking on a node or its children
    const isNodeClick = event.target.closest('.node')
    if (!isNodeClick && selectedNode) {
      clearSelection()
    }
  })
}

function setupControls () {
  const searchInput = getCachedElement('topology-search')
  const fitBtn = getCachedElement('fit-view')

  if (searchInput) {
    searchInput.addEventListener('input', e => {
      filters.searchTerm = e.target.value.toLowerCase()
      applyFilters()
    })
  }
  if (fitBtn) fitBtn.addEventListener('click', fitToView)
}

function setupResize () {
  window.addEventListener('resize', () => {
    const container = getCachedElement('topology-graph')
    if (container) {
      width = container.clientWidth
      height = container.clientHeight
      svg.attr('viewBox', `0 0 ${width} ${height}`)
      setTimeout(fitToView, 100)
    }
  })
}

function loadData () {
  const vhost = window.sessionStorage.getItem('vhost')
  const baseUrls = ['api/exchanges', 'api/queues', 'api/bindings']
  const urls = baseUrls.map(url =>
    vhost && vhost !== '_all' ? `${url}/${encodeURIComponent(vhost)}` : url
  )

  Promise.all(urls.map(url => HTTP.request('GET', url)))
    .then(([exchanges, queues, bindings]) => {
      processData(exchanges, queues, bindings)
      renderGraph()
    })
    .catch(() => showError('Failed to load topology data'))
}

function createNodeFromExchange (e) {
  return {
    id: `exchange:${e.vhost}:${e.name}`,
    name: e.name, type: 'exchange', subtype: e.type,
    vhost: e.vhost, durable: e.durable, policy: e.policy
  }
}

function createNodeFromQueue (q) {
  return {
    id: `queue:${q.vhost}:${q.name}`,
    name: q.name, type: 'queue', vhost: q.vhost,
    durable: q.durable, messages: q.messages || 0,
    consumers: q.consumers || 0, policy: q.policy
  }
}

function calculateFlowRate (binding, queues, exchanges) {
  if (binding.destination_type !== 'queue') return 0

  const queue = queues.find(q => q.vhost === binding.vhost && q.name === binding.destination)
  const exchange = exchanges.find(e => e.vhost === binding.vhost && e.name === binding.source)

  return queue?.message_stats?.deliver_get_details?.rate ||
         exchange?.message_stats?.publish_out_details?.rate || 0
}

function styleLinkByFlow (link, maxFlow) {
  if (link.flowRate === 0) {
    return { color: 'var(--color-topology-flow-inactive)', opacity: 0.4, dash: '5,5' }
  } else if (link.flowRate / maxFlow < 0.5) {
    return { color: 'var(--color-topology-flow-medium)', opacity: 0.7, dash: null }
  } else {
    return { color: 'var(--color-topology-flow-active)', opacity: 0.9, dash: null }
  }
}

function processData (exchanges, queues, bindings) {
  nodes = [
    ...exchanges.filter(e => e.name).map(createNodeFromExchange),
    ...queues.map(createNodeFromQueue)
  ]

  nodeMap = new Map(nodes.map(n => [n.id, n]))

  links = bindings.filter(b =>
    b.source && nodeMap.has(`exchange:${b.vhost}:${b.source}`) &&
    nodeMap.has(`${b.destination_type}:${b.vhost}:${b.destination}`)
  ).map(b => ({
    source: `exchange:${b.vhost}:${b.source}`,
    target: `${b.destination_type}:${b.vhost}:${b.destination}`,
    routing_key: b.routing_key,
    vhost: b.vhost,
    flowRate: calculateFlowRate(b, queues, exchanges)
  }))

  const maxFlow = Math.max(...links.map(l => l.flowRate || 0))
  links.forEach(link => Object.assign(link, styleLinkByFlow(link, maxFlow)))

  calculatePositions()
}

function calculatePositions () {
  const vhosts = [...new Set(nodes.map(n => n.vhost))].sort()
  const startX = (width - (vhosts.length * 500 + (vhosts.length - 1) * 120)) / 2

  vhosts.forEach((vhost, vhostIndex) => {
    const vhostNodes = nodes.filter(n => n.vhost === vhost)
    const exchanges = vhostNodes.filter(n => n.type === 'exchange').sort((a, b) => a.name.localeCompare(b.name))
    const queues = vhostNodes.filter(n => n.type === 'queue').sort((a, b) => a.name.localeCompare(b.name))

    const baseX = startX + vhostIndex * CONFIG.VHOST_SPACING

    exchanges.forEach((node, i) => {
      node.x = baseX
      node.y = 240 + i * CONFIG.NODE_SPACING
    })

    queues.forEach((node, i) => {
      node.x = baseX + CONFIG.LAYOUT.BASE_X_OFFSET
      node.y = 240 + i * CONFIG.NODE_SPACING
    })
  })
}

function createVHostBoxes () {
  const vhosts = [...new Set(nodes.map(n => n.vhost))]
  if (vhosts.length <= 1) return

  vhosts.forEach(vhost => {
    const vhostNodes = nodes.filter(n => n.vhost === vhost)
    const minX = Math.min(...vhostNodes.map(n => n.x))
    const maxX = Math.max(...vhostNodes.map(n => n.x))
    const minY = Math.min(...vhostNodes.map(n => n.y))
    const maxY = Math.max(...vhostNodes.map(n => n.y))

    g.append('rect')
      .attr('class', 'topology-vhost-box')
      .attr('x', minX - CONFIG.MARGINS.VHOST.X)
      .attr('y', minY - CONFIG.MARGINS.VHOST.Y)
      .attr('width', Math.max(400, (maxX - minX) + 260))
      .attr('height', (maxY - minY) + 120)
      .attr('rx', 8)

    g.append('text')
      .attr('class', 'topology-vhost-label')
      .attr('x', minX - CONFIG.MARGINS.VHOST.X + (Math.max(400, (maxX - minX) + 260)) / 2)
      .attr('y', minY - 30)
      .attr('text-anchor', 'middle')
      .text(vhost)
  })
}

function truncateNodeName (name) {
  return name.length > CONFIG.NODES.NAME_MAX_LENGTH ?
    name.substring(0, CONFIG.NODES.NAME_MAX_LENGTH - 3) + '...' : name
}

function renderGraph () {
  if (nodes.length > CONFIG.MAX_NODES_THRESHOLD) {
    showLargeGraphWarning()
    return
  }

  g.selectAll('*').remove()

  // Create links with orange flow visualization
  linkElements = g.selectAll('.link')
    .data(links)
    .enter().append('line')
    .attr('class', 'topology-link')
    .attr('stroke-width', 2)
    .attr('stroke', d => d.color)
    .attr('stroke-dasharray', d => d.dash || 'none')
    .attr('opacity', d => d.opacity)

  linkElements.append('title').text(d =>
    `${d.source.split(':').pop()} → ${d.target.split(':').pop()}\nFlow: ${d.flowRate.toFixed(2)}/sec`
  )

  createVHostBoxes()

  // Create nodes
  nodeElements = g.selectAll('.node')
    .data(nodes)
    .enter().append('g')
    .attr('class', 'node topology-node')
    .attr('transform', d => `translate(${d.x},${d.y})`)
    .on('click', (event, d) => {
      event.stopPropagation()
      selectNode(d)
    })

  nodeElements.append('circle')
    .attr('class', 'topology-node-circle')
    .attr('r', d => d.type === 'exchange' ? CONFIG.NODES.EXCHANGE_RADIUS : CONFIG.NODES.QUEUE_RADIUS)
    .attr('fill', d => `var(--color-topology-${d.type})`)

  nodeElements.append('text')
    .attr('dx', 25).attr('dy', '.35em')
    .attr('class', 'topology-node-text')
    .text(d => truncateNodeName(d.name))

  nodeElements.append('title').text(d =>
    d.type === 'exchange'
      ? `Exchange: ${d.name}\nType: ${d.subtype}\nVHost: ${d.vhost}`
      : `Queue: ${d.name}\nVHost: ${d.vhost}\nMessages: ${d.messages}\nConsumers: ${d.consumers}`
  )

  updateLinkPositions()
  addLegend()

  // Simple fit to view
  setTimeout(fitToView, 100)
}

function updateLinkPositions () {
  linkElements
    .attr('x1', d => nodeMap.get(d.source)?.x || 0)
    .attr('y1', d => nodeMap.get(d.source)?.y || 0)
    .attr('x2', d => nodeMap.get(d.target)?.x || 0)
    .attr('y2', d => nodeMap.get(d.target)?.y || 0)
}

function calculateContentBounds () {
  if (!nodes.length) return null

  let bounds = nodes.reduce((acc, n) => ({
    minX: Math.min(acc.minX, n.x - 20),
    maxX: Math.max(acc.maxX, n.x + 145),
    minY: Math.min(acc.minY, n.y - 20),
    maxY: Math.max(acc.maxY, n.y + 20)
  }), { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity })

  const vhosts = [...new Set(nodes.map(n => n.vhost))]
  if (vhosts.length > 1) {
    vhosts.forEach(vhost => {
      const vhostNodes = nodes.filter(n => n.vhost === vhost)
      if (vhostNodes.length) {
        const minX = Math.min(...vhostNodes.map(n => n.x))
        const maxX = Math.max(...vhostNodes.map(n => n.x))
        const minY = Math.min(...vhostNodes.map(n => n.y))
        const maxY = Math.max(...vhostNodes.map(n => n.y))

        bounds.minX = Math.min(bounds.minX, minX - CONFIG.MARGINS.VHOST.X)
        bounds.maxX = Math.max(bounds.maxX, minX - CONFIG.MARGINS.VHOST.X + Math.max(400, (maxX - minX) + 260))
        bounds.minY = Math.min(bounds.minY, minY - CONFIG.MARGINS.VHOST.Y)
        bounds.maxY = Math.max(bounds.maxY, maxY + CONFIG.MARGINS.VHOST.Y)
      }
    })
  }
  return bounds
}

function fitToView () {
  const bounds = calculateContentBounds()
  if (!bounds) return

  const margin = CONFIG.MARGINS.DEFAULT
  const contentWidth = bounds.maxX - bounds.minX + 2 * margin
  const contentHeight = bounds.maxY - bounds.minY + 2 * margin
  const scale = Math.min(width / contentWidth, height / contentHeight, 1)

  const contentCenterX = (bounds.minX + bounds.maxX) / 2
  const contentCenterY = (bounds.minY + bounds.maxY) / 2

  const transform = d3.zoomIdentity
    .translate(width / 2 - contentCenterX * scale, height / 2 - contentCenterY * scale)
    .scale(scale)

  svg.transition().duration(750).call(zoom.transform, transform)
}

function selectNode (node) {
  selectedNode = node

  // Find connected nodes
  const connectedIds = new Set([node.id])
  links.forEach(link => {
    if (link.source === node.id) connectedIds.add(link.target)
    if (link.target === node.id) connectedIds.add(link.source)
  })

  // Highlight connected elements
  nodeElements.classed('topology-node-dimmed', d => !connectedIds.has(d.id))
  linkElements.classed('topology-link-dimmed', d =>
    !(connectedIds.has(d.source) && connectedIds.has(d.target))
  )

  showNodeDetails(node)
}

function clearSelection () {
  selectedNode = null
  nodeElements.classed('topology-node-dimmed', false)
  linkElements.classed('topology-link-dimmed', false)
  closePopup()
}

function applyFilters () {
  if (!nodeElements || !linkElements) return

  nodeElements
    .classed('topology-search-dimmed', d =>
      filters.searchTerm && !d.name.toLowerCase().includes(filters.searchTerm)
    )
    .classed('topology-node-dimmed', d =>
      selectedNode && selectedNode !== d
    )

  linkElements.classed('topology-link-dimmed', d => !!selectedNode)
}

function updateElementText (elementId, text) {
  const el = getCachedElement(elementId)
  if (el) el.textContent = text
}

function toggleElementDisplay (elementId, show) {
  const el = getCachedElement(elementId)
  if (el) {
    if (show) {
      el.classList.remove('hidden')
    } else {
      el.classList.add('hidden')
    }
  }
}

function showNodeDetails (node) {
  const popup = getCachedElement('topology-info-popup')
  if (!popup) return

  const updates = {
    'popup-title': `${node.type === 'exchange' ? '📨' : '📦'} ${node.name}`,
    'node-type': node.type + (node.subtype ? ` (${node.subtype})` : ''),
    'node-vhost': node.vhost,
    'node-durable': node.durable ? 'Yes' : 'No'
  }

  Object.entries(updates).forEach(([id, text]) => updateElementText(id, text))

  if (node.policy) {
    toggleElementDisplay('policy-group', true)
    updateElementText('node-policy', node.policy)
  } else {
    toggleElementDisplay('policy-group', false)
  }

  if (node.type === 'queue') {
    toggleElementDisplay('queue-messages-group', true)
    toggleElementDisplay('queue-consumers-group', true)
    updateElementText('node-messages', node.messages.toLocaleString())
    updateElementText('node-consumers', node.consumers)

    const messagesEl = getCachedElement('node-messages')
    if (messagesEl) messagesEl.className = node.messages > 0 ? 'text-warning' : ''
  } else {
    toggleElementDisplay('queue-messages-group', false)
    toggleElementDisplay('queue-consumers-group', false)
  }

  updateConnections(node)
  popup.classList.add('show')
}

function updateConnections (node) {
  const incoming = links.filter(l => l.target === node.id)
  const outgoing = links.filter(l => l.source === node.id)

  updateConnectionsList('incoming', incoming.map(l => ({
    name: l.source.split(':').pop(),
    routing_key: l.routing_key,
    flowRate: l.flowRate
  })))

  updateConnectionsList('outgoing', outgoing.map(l => ({
    name: l.target.split(':').pop(),
    routing_key: l.routing_key,
    flowRate: l.flowRate
  })))

  const hasConnections = incoming.length > 0 || outgoing.length > 0
  toggleElementDisplay('no-connections', !hasConnections)
  updateElementText('no-connections-type', node.type)
  toggleElementDisplay('default-message', false)
}

function formatConnectionItem (conn) {
  const routingKey = conn.routing_key ? `<div class="connection-detail">🔑 ${conn.routing_key}</div>` : ''
  return `<li><div class="connection-name">${conn.name}</div>${routingKey}</li>`
}

function setupCollapsibleHeader (section, list) {
  const header = section.querySelector('.collapsible-header')
  if (!header) return

  // Remove any existing event listeners
  const newHeader = header.cloneNode(true)
  header.parentNode.replaceChild(newHeader, header)

  newHeader.addEventListener('click', (event) => {
    event.preventDefault()
    event.stopPropagation()

    const toggleIcon = newHeader.querySelector('.toggle-icon')
    const isHidden = list.classList.contains('hidden')

    console.log('Toggle clicked, current state hidden:', isHidden, 'list:', list) // Debug log

    if (isHidden) {
      list.classList.remove('hidden')
      if (toggleIcon) toggleIcon.textContent = '▼'
    } else {
      list.classList.add('hidden')
      if (toggleIcon) toggleIcon.textContent = '▶'
    }

    console.log('After toggle, hidden:', list.classList.contains('hidden')) // Debug log
  })
}

function updateConnectionsList (type, connections) {
  const section = getCachedElement(`${type}-connections`)
  const count = getCachedElement(`${type}-count`)
  const list = getCachedElement(`${type}-list`)

  if (!section || !count || !list) return

  if (connections.length > 0) {
    section.classList.remove('hidden')
    count.textContent = connections.length
    list.innerHTML = connections.map(formatConnectionItem).join('')
    list.classList.remove('hidden')  // Show the list when populated

    // Update toggle icon to show expanded state
    const toggleIcon = section.querySelector('.toggle-icon')
    if (toggleIcon) toggleIcon.textContent = '▼'

    setupCollapsibleHeader(section, list)
  } else {
    section.classList.add('hidden')
  }
}

function closePopup () {
  const popup = getCachedElement('topology-info-popup')
  if (popup) popup.classList.remove('show')
}

function addLegend () {
  const legend = svg.append('g').attr('class', 'topology-legend-fixed')
  const [x, y] = [10, height - 105]

  legend.append('rect')
    .attr('x', x).attr('y', y - 5)
    .attr('width', 200).attr('height', 100)
    .attr('class', 'topology-legend-bg')
    .attr('rx', 5)

  legend.append('text')
    .attr('x', x + 5).attr('y', y + 10)
    .attr('class', 'topology-legend-title')
    .text('Message Flow Legend')

  const legendData = [
    { y: 25, color: 'var(--color-topology-flow-active)', dash: null, text: 'High activity' },
    { y: 40, color: 'var(--color-topology-flow-medium)', dash: null, text: 'Activity' },
    { y: 55, color: 'var(--color-topology-flow-inactive)', dash: '5,5', text: 'No activity' }
  ]

  legendData.forEach(item => {
    legend.append('line')
      .attr('x1', x + 5).attr('y1', y + item.y)
      .attr('x2', x + 35).attr('y2', y + item.y)
      .attr('stroke', item.color)
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', item.dash)
      .attr('opacity', item.dash ? 0.6 : 1)

    legend.append('text')
      .attr('x', x + 40).attr('y', y + item.y + 5)
      .attr('class', 'topology-legend-text')
      .text(item.text)
  })

}

function showError (message) {
  d3.select('#topology-graph').selectAll('*').remove()
  d3.select('#topology-graph')
    .append('div')
    .attr('class', 'topology-error')
    .text(message)
}

function showLargeGraphWarning () {
  g.selectAll('*').remove()

  const warning = g.append('g').attr('class', 'topology-warning')
  const centerX = width / 2, centerY = height / 2

  warning.append('rect')
    .attr('x', centerX - 200).attr('y', centerY - 80)
    .attr('width', 400).attr('height', 160).attr('rx', 10)
    .attr('class', 'topology-legend-bg')

  const warningTexts = [
    { y: -20, size: '18px', weight: 'bold', text: 'Too Many Nodes' },
    { y: 10, size: '14px', weight: 'normal', text: `This topology has ${nodes.length} nodes.` },
    { y: 30, size: '14px', weight: 'normal', text: `Topology view is disabled for graphs with more than ${CONFIG.MAX_NODES_THRESHOLD} nodes.` }
  ]

  warningTexts.forEach(({ y, size, weight, text }) => {
    const className = weight === 'bold' ? 'topology-warning-title' : 'topology-warning-text'
    warning.append('text')
      .attr('x', centerX).attr('y', centerY + y)
      .attr('text-anchor', 'middle')
      .attr('class', className)
      .text(text)
  })
}

// Refresh function for external use
window.refreshTopology = loadData
