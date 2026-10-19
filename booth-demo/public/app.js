'use strict'

const $ = (sel, root = document) => root.querySelector(sel)
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim()
const NODE_COLORS = [css('--node-0'), css('--node-1'), css('--node-2')]
const fmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const num = (v) => (v === null || v === undefined ? '–' : fmt.format(v))
const secs = (ms) => `${(ms / 1000).toFixed(1)} s`

let state = null
let lastMessageAt = 0

// --- Data feed --------------------------------------------------------------

function connect () {
  const es = new EventSource('/events')
  es.onmessage = (e) => {
    lastMessageAt = Date.now()
    $('#conn').hidden = true
    state = JSON.parse(e.data)
    render(state)
  }
  es.onerror = () => { $('#conn').hidden = false }
}
setInterval(() => { if (Date.now() - lastMessageAt > 3000) $('#conn').hidden = false }, 1000)

// --- Rendering --------------------------------------------------------------

function render (s) {
  document.title = s.title
  $('#title').textContent = s.title
  $('#backend').textContent = { raft: 'Raft consensus', etcd: 'etcd coordination', standalone: 'Single node' }[s.backend] || ''
  $('#term').textContent = s.term ? `Election term ${s.term}` : ''
  $('#version').textContent = s.overview.version ? `LavinMQ ${s.overview.version}` : ''
  renderBanner(s)
  renderNodes(s)
  renderHero(s)
  drawChart(s)
  renderSequence(s.scenarios.sequence)
  renderStream(s.scenarios.stream)
  renderIot(s.scenarios.iot, s.nodes)
  renderWorkQueue(s.scenarios.work_queue)
  renderEvents(s.events)
  renderOperator(s)
}

function renderBanner (s) {
  const banner = $('#banner')
  const seq = s.scenarios.sequence
  let text = null
  if (s.leaderless_since) {
    text = `⏳ No leader: the remaining nodes are electing one… ${secs(s.now - s.leaderless_since)}`
  } else if (seq.outage && seq.outage_since) {
    text = `⏸ Confirms paused while the cluster recovers… ${secs(s.now - seq.outage_since)}`
  }
  banner.hidden = !text
  if (text) banner.textContent = text
}

const STATES = {
  leader: { icon: '👑', label: 'Leader', cls: 'state-good' },
  in_sync: { icon: '✓', label: 'Follower, in sync', cls: 'state-good' },
  syncing: { icon: '⟳', label: 'Follower, catching up', cls: 'state-warning' },
  follower: { icon: '●', label: 'Follower', cls: 'state-good' },
  offline: { icon: '✕', label: 'Offline', cls: 'state-critical' }
}

function renderNodes (s) {
  const root = $('#nodes')
  s.nodes.forEach((n, i) => {
    let el = root.children[i]
    if (!el) {
      el = document.createElement('div')
      el.className = 'node'
      el.style.setProperty('--node-color', NODE_COLORS[i % NODE_COLORS.length])
      el.innerHTML = `<div class="node-head"><span class="node-name"></span><span class="node-host"></span></div>
        <div class="node-state"><span class="icon" aria-hidden="true"></span><span class="label"></span></div>
        <div class="node-facts"></div>`
      root.appendChild(el)
    }
    const st = STATES[n.state] || STATES.follower
    el.className = `node ${n.state === 'leader' ? 'leader' : ''} ${n.state}`
    $('.node-name', el).textContent = n.name
    $('.node-host', el).textContent = n.host
    $('.node-state', el).className = `node-state ${st.cls}`
    $('.icon', el).textContent = st.icon
    let label = st.label
    if (n.state === 'offline' && n.offline_since) label += ` for ${Math.round((s.now - n.offline_since) / 1000)} s`
    $('.label', el).textContent = label
    const facts = []
    if (n.online) facts.push(`<span><b>${num(n.clients)}</b> demo clients</span>`)
    if (n.voter !== null && n.online) facts.push(`<span>${n.voter ? 'Voter' : 'Learner'}</span>`)
    if (n.lag_bytes) facts.push(`<span>lag <b>${num(n.lag_bytes)}</b> B</span>`)
    $('.node-facts', el).innerHTML = facts.join('')
  })
}

function renderHero (s) {
  const pub = s.throughput.published; const del = s.throughput.delivered
  $('#pub-rate').textContent = num(pub[pub.length - 1])
  $('#del-rate').textContent = num(del[del.length - 1])
  const seq = s.scenarios.sequence
  const lost = seq.lost
  $('#lost').innerHTML = lost === 0
    ? '<span class="ok-icon" aria-hidden="true">✓</span>0'
    : `<span class="bad-icon" aria-hidden="true">✕</span>${num(lost)}`
  $('#lost-note').textContent = `of ${num(seq.published)} confirmed by the cluster`
}

// --- Throughput chart --------------------------------------------------------

const WINDOW_S = 120
const chart = { canvas: $('#chart'), hoverX: null }

function niceMax (v) {
  if (v <= 0) return 10
  const p = Math.pow(10, Math.floor(Math.log10(v)))
  for (const m of [1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * p >= v) return m * p
  return 10 * p
}

function drawChart (s) {
  const c = chart.canvas
  const dpr = window.devicePixelRatio || 1
  const w = c.clientWidth; const h = c.clientHeight
  if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
    c.width = Math.round(w * dpr)
    c.height = Math.round(h * dpr)
  }
  const ctx = c.getContext('2d')
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
  ctx.clearRect(0, 0, w, h)

  const pad = { l: 56, r: 12, t: 22, b: 22 }
  const pw = w - pad.l - pad.r; const ph = h - pad.t - pad.b
  const series = [
    { data: s.throughput.published, color: css('--series-pub') },
    { data: s.throughput.delivered, color: css('--series-del') }
  ]
  const max = niceMax(Math.max(1, ...series.flatMap((x) => x.data)))
  // x: seconds ago, 0 at the right edge
  const xOf = (ago) => pad.l + pw * (1 - ago / WINDOW_S)
  const yOf = (v) => pad.t + ph * (1 - v / max)

  // Grid and y labels: recessive
  ctx.font = '12px system-ui, sans-serif'
  ctx.fillStyle = css('--text-muted')
  ctx.strokeStyle = css('--border')
  ctx.lineWidth = 1
  ctx.textAlign = 'right'
  ctx.textBaseline = 'middle'
  for (let i = 0; i <= 4; i++) {
    const v = (max / 4) * i; const y = Math.round(yOf(v)) + 0.5
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + pw, y); ctx.stroke()
    ctx.fillText(fmt.format(v), pad.l - 8, y)
  }
  ctx.textAlign = 'center'
  ctx.textBaseline = 'top'
  for (const ago of [120, 90, 60, 30, 0]) {
    ctx.fillText(ago === 0 ? 'now' : `-${ago}s`, xOf(ago), pad.t + ph + 6)
  }

  // Outages: when the integrity publisher got no confirms
  const warning = css('--warning')
  const bands = s.outages.map((o) => [o.from, o.to])
  const seq = s.scenarios.sequence
  if (seq.outage && seq.outage_since) bands.push([seq.outage_since, s.now])
  for (const [from, to] of bands) {
    const x1 = Math.max(pad.l, xOf((s.now - from) / 1000))
    const x2 = Math.min(pad.l + pw, xOf((s.now - to) / 1000))
    if (x2 <= pad.l || x2 - x1 < 1) continue
    ctx.fillStyle = warning + '40'
    ctx.fillRect(x1, pad.t, x2 - x1, ph)
    ctx.fillStyle = css('--text-primary')
    ctx.textBaseline = 'bottom'
    ctx.fillText(secs(to - from), (x1 + x2) / 2, pad.t - 4)
  }

  // Lines, 2px, newest point at "now"
  for (const { data, color } of series) {
    if (data.length < 2) continue
    ctx.strokeStyle = color
    ctx.lineWidth = 2
    ctx.lineJoin = 'round'
    ctx.beginPath()
    data.forEach((v, i) => {
      const ago = data.length - 1 - i
      const x = xOf(ago); const y = yOf(v)
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)
    })
    ctx.stroke()
  }

  // Hover crosshair
  const tip = $('#tooltip')
  if (chart.hoverX === null || chart.hoverX < pad.l) { tip.hidden = true; return }
  const ago = Math.round((1 - (chart.hoverX - pad.l) / pw) * WINDOW_S)
  const idx = series[0].data.length - 1 - ago
  if (idx < 0 || ago < 0) { tip.hidden = true; return }
  const x = xOf(ago)
  ctx.strokeStyle = css('--text-secondary')
  ctx.lineWidth = 1
  ctx.beginPath(); ctx.moveTo(x, pad.t); ctx.lineTo(x, pad.t + ph); ctx.stroke()
  for (const { data, color } of series) {
    if (data[idx] === undefined) continue
    ctx.fillStyle = color
    ctx.strokeStyle = css('--surface-1')
    ctx.lineWidth = 2
    ctx.beginPath(); ctx.arc(x, yOf(data[idx]), 5, 0, Math.PI * 2); ctx.fill(); ctx.stroke()
  }
  tip.hidden = false
  tip.innerHTML = `${ago === 0 ? 'now' : `${ago} s ago`}<br>published <b>${num(series[0].data[idx])}</b>/s<br>delivered <b>${num(series[1].data[idx])}</b>/s`
  tip.style.left = `${Math.min(x + 12, w - tip.offsetWidth - 4)}px`
  tip.style.top = `${pad.t}px`
}

chart.canvas.addEventListener('mousemove', (e) => {
  chart.hoverX = e.offsetX
  if (state) drawChart(state)
})
chart.canvas.addEventListener('mouseleave', () => {
  chart.hoverX = null
  if (state) drawChart(state)
})

// --- Scenario cards ----------------------------------------------------------

function setKv (card, key, text, warn = false) {
  const dd = $(`[data-k="${key}"]`, card)
  if (!dd) return
  dd.textContent = text
  dd.classList.toggle('warn', warn)
}

function cardState (card, sc) {
  card.classList.toggle('stopped', !sc.running)
  $('.run', card).textContent = sc.running ? '' : 'stopped'
}

function renderSequence (sc) {
  const card = $('#sc-sequence')
  cardState(card, sc)
  setKv(card, 'published', num(sc.published))
  setKv(card, 'received', num(sc.received))
  setKv(card, 'in_flight', num(sc.in_flight))
  setKv(card, 'duplicates', num(sc.duplicates))
  setKv(card, 'last_outage', sc.last_outage === null ? '–' : `${sc.last_outage.toFixed(1)} s`)
  setKv(card, 'reconnects', num(sc.reconnects))
}

function renderStream (sc) {
  const card = $('#sc-stream')
  cardState(card, sc)
  setKv(card, 'published_rate', num(sc.published_rate))
  setKv(card, 'head', num(sc.head))
  const root = $('.readers', card)
  const head = sc.head || 0
  sc.readers.forEach((r, i) => {
    let el = root.children[i]
    if (!el) {
      el = document.createElement('div')
      el.className = 'reader'
      el.innerHTML = '<span class="r-name"></span><span class="r-rate"></span><div class="r-bar"><i></i></div><span class="r-meta"></span>'
      root.appendChild(el)
    }
    $('.r-name', el).textContent = r.label
    $('.r-rate', el).textContent = `${num(r.rate)} msg/s`
    const pct = head > 0 && r.offset !== null ? Math.max(0, Math.min(100, (r.offset / head) * 100)) : 0
    $('.r-bar i', el).style.width = `${pct}%`
    const lag = r.lag ? `${num(r.lag)} behind` : 'at the head'
    const resumes = r.resumes ? ` · resumed ${r.resumes}× from its own offset` : ''
    $('.r-meta', el).textContent = r.offset === null ? 'waiting…' : `offset ${num(r.offset)} · ${lag}${resumes}`
  })
}

function renderIot (sc, nodes) {
  const card = $('#sc-iot')
  cardState(card, sc)
  setKv(card, 'connected', `${num(sc.connected)} / ${num(sc.devices)}`, sc.connected < sc.devices)
  setKv(card, 'received_rate', num(sc.received_rate))
  setKv(card, 'reconnects', num(sc.reconnects))
  drawDots($('.dots', card), sc.dots)
}

function drawDots (c, dots) {
  const dpr = window.devicePixelRatio || 1
  const w = c.clientWidth; const h = c.clientHeight
  if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
    c.width = Math.round(w * dpr)
    c.height = Math.round(h * dpr)
  }
  const ctx = c.getContext('2d')
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
  ctx.clearRect(0, 0, w, h)
  const n = dots.length
  if (!n) return
  const cell = Math.max(3, Math.floor(Math.sqrt((w * h) / n)))
  const cols = Math.max(1, Math.floor(w / cell))
  const r = Math.max(1, cell * 0.36)
  const critical = css('--critical')
  for (let i = 0; i < n; i++) {
    const ch = dots.charCodeAt(i)
    const x = (i % cols) * cell + cell / 2; const y = Math.floor(i / cols) * cell + cell / 2
    if (y > h) break
    ctx.beginPath()
    ctx.arc(x, y, r, 0, Math.PI * 2)
    if (ch === 48) { // '0': disconnected
      ctx.strokeStyle = critical
      ctx.lineWidth = 1
      ctx.stroke()
    } else if (ch >= 97) { // 'a'+node: connected, reading just arrived
      ctx.fillStyle = NODE_COLORS[ch - 97] || NODE_COLORS[0]
      ctx.fill()
    } else { // 'A'+node: connected, no recent reading
      ctx.fillStyle = (NODE_COLORS[ch - 65] || NODE_COLORS[0]) + '55'
      ctx.fill()
    }
  }
}

function renderWorkQueue (sc) {
  const card = $('#sc-work_queue')
  cardState(card, sc)
  setKv(card, 'published_rate', num(sc.published_rate))
  setKv(card, 'consumed_rate', num(sc.consumed_rate))
  setKv(card, 'queue_depth', num(sc.queue_depth))
  setKv(card, 'consumers', num(sc.consumers))
  setKv(card, 'reconnects', num(sc.reconnects))
}

// --- Events -----------------------------------------------------------------

const EVENT_ICONS = { down: '✕', up: '↑', leader: '👑', election: '⏳', sync: '✓', recovered: '✓', transfer: '⇄', info: '•' }
let lastEventsKey = ''

function renderEvents (events) {
  const recent = events.slice(-6).reverse()
  const key = recent.map((e) => e.at + e.message).join('|')
  if (key === lastEventsKey) return
  lastEventsKey = key
  $('#events').innerHTML = recent.map((e) => {
    const t = new Date(e.at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
    return `<li class="ev-${e.kind}"><span class="icon" aria-hidden="true">${EVENT_ICONS[e.kind] || '•'}</span><span>${escapeHtml(e.message)}</span><time>${t}</time></li>`
  }).join('')
}

function escapeHtml (s) {
  return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))
}

// --- Operator controls ------------------------------------------------------

const SCENARIO_LABELS = {
  work_queue: ['Work queue', 'msg/s', 5000],
  sequence: ['Ordered delivery', 'msg/s', 2000],
  stream: ['Stream', 'msg/s', 5000],
  iot: ['IoT devices', 'devices', null]
}
const operator = $('#operator')
operator.hidden = !new URLSearchParams(location.search).has('operator')
document.addEventListener('keydown', (e) => {
  if (e.key === 'o' && !e.target.closest('input')) operator.hidden = !operator.hidden
})

async function post (url) {
  const res = await fetch(url, { method: 'POST' })
  if (!res.ok) alert(`${url}: ${res.status} ${await res.text()}`)
}

operator.addEventListener('click', (e) => {
  const btn = e.target.closest('button')
  if (!btn) return
  if (btn.dataset.op === 'transfer') post('/api/transfer-leadership')
  if (btn.dataset.op === 'reset' && confirm('Reset all counters and restart the scenarios?')) post('/api/reset')
  if (btn.dataset.start) post(`/api/scenarios/${btn.dataset.start}/${btn.dataset.running === 'true' ? 'stop' : 'start'}`)
})

function renderOperator (s) {
  if (operator.hidden) return
  const root = $('#op-scenarios')
  for (const [name, [label, unit, maxRate]] of Object.entries(SCENARIO_LABELS)) {
    const sc = s.scenarios[name]
    let el = root.querySelector(`[data-sc="${name}"]`)
    const max = maxRate || sc.max_devices
    if (!el) {
      el = document.createElement('div')
      el.className = 'op-sc'
      el.dataset.sc = name
      el.innerHTML = `<strong>${label}</strong><button class="secondary"></button>
        <label><input type="range" min="0" max="${max}" step="${max > 1000 ? 50 : 10}"><output></output></label>`
      const input = $('input', el)
      input.addEventListener('input', () => { $('output', el).textContent = `${input.value} ${unit}` })
      input.addEventListener('change', () => post(`/api/scenarios/${name}/rate?value=${input.value}`))
      root.appendChild(el)
    }
    const btn = $('button', el)
    btn.textContent = sc.running ? 'Stop' : 'Start'
    btn.dataset.start = name
    btn.dataset.running = sc.running
    const input = $('input', el)
    if (document.activeElement !== input) {
      input.value = sc.rate
      $('output', el).textContent = `${sc.rate} ${unit}`
    }
  }
}

window.addEventListener('resize', () => state && render(state))
connect()
