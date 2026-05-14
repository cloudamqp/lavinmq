import * as HTTP from './http.js'
import * as DOM from './dom.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const vhost = search.get('vhost')
const containerName = search.get('name')
const id = search.get('id')

const backLink = document.querySelector('#back-link')
if (vhost && containerName) {
  backLink.href = `replay#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(containerName)}`
}

function setSubtitle (text) {
  const el = document.getElementById('pagename-label')
  if (el) el.textContent = text
}

function fmtTs (ms) {
  if (ms === null || ms === undefined) return ''
  try {
    return new Date(Number(ms)).toISOString().replace('T', ' ').replace(/\..+/, ' UTC')
  } catch (e) {
    return String(ms)
  }
}

let currentMessage = null

function loadMessage () {
  setSubtitle(`${containerName} / ${id ? id.substring(0, 8) : ''}`)
  HTTP.request('GET', HTTP.url`api/replay/${vhost}/${containerName}/${id}`)
    .then(m => {
      currentMessage = m
      renderMessage(m)
    })
}

function renderMessage (m) {
  document.querySelector('#m-id').textContent = m.id || ''
  document.querySelector('#m-source').textContent = m.source || ''
  document.querySelector('#m-exchange').textContent = m.exchange || ''
  document.querySelector('#m-routing-key').textContent = m.routing_key || ''
  document.querySelector('#m-rule-id').textContent = m.rule_id || ''
  document.querySelector('#m-timestamp').textContent = fmtTs(m.timestamp)
  document.querySelector('#m-delivery-count').textContent = m.delivery_count != null ? String(m.delivery_count) : ''
  document.querySelector('#m-content-type').textContent = m.content_type || ''
  document.querySelector('#m-payload-bytes').textContent = String(m.payload_bytes || 0)
  document.querySelector('#m-properties').textContent = JSON.stringify(m.properties || {}, null, 2)

  const releaseTarget = document.querySelector('#m-release-target')
  if (releaseTarget) {
    const ex = m.exchange || ''
    const rk = m.routing_key || ''
    if (ex === '' && m.source) {
      releaseTarget.textContent = `queue "${m.source}" (via amq.default)`
    } else if (ex === '') {
      releaseTarget.textContent = `queue "${rk}" (via amq.default)`
    } else {
      releaseTarget.textContent = `exchange "${ex}" with routing-key "${rk}"`
    }
  }

  const bodyTextarea = document.querySelector('textarea[name="body"]')
  if (m.payload_encoding === 'string') {
    bodyTextarea.value = m.payload || ''
  } else {
    bodyTextarea.value = ''
    bodyTextarea.placeholder = `Binary payload (base64): ${m.payload || ''}`
  }
  const userHeaders = {}
  const allHeaders = (m.properties && m.properties.headers) || {}
  for (const k of Object.keys(allHeaders)) {
    if (!k.startsWith('x-source-') && k !== 'x-replay-id') userHeaders[k] = allHeaders[k]
  }
  document.querySelector('textarea[name="headers"]').value = JSON.stringify(userHeaders, null, 2)
}

document.querySelector('#patchMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const form = evt.target
  const body = {}
  const bodyText = form.querySelector('textarea[name="body"]').value
  if (bodyText.length > 0 && bodyText !== (currentMessage ? currentMessage.payload : '')) {
    body.body = bodyText
  }
  const headersText = form.querySelector('textarea[name="headers"]').value.trim()
  if (headersText.length > 0) {
    try {
      body.headers = JSON.parse(headersText)
    } catch (e) {
      DOM.toast('Headers must be valid JSON', 'error')
      return
    }
  }
  const force = form.querySelector('input[name="force"]').checked
  let url = HTTP.url`api/replay/${vhost}/${containerName}/${id}`
  if (force) url += '?force=true'
  HTTP.request('PATCH', url, { body })
    .then(() => {
      DOM.toast('Message updated; new id assigned')
      window.location.href = `replay#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(containerName)}`
    })
})

document.querySelector('#releaseMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const resetReplay = evt.target.querySelector('input[name="reset-replay"]').checked
  if (!window.confirm('Release this message back to its original source?')) return
  let url = HTTP.url`api/replay/${vhost}/${containerName}/${id}/release`
  if (resetReplay) url += '?reset_replay=true'
  HTTP.request('POST', url)
    .then(() => {
      DOM.toast('Message released')
      window.location.href = `replay#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(containerName)}`
    })
})

document.querySelector('#deleteMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  if (!window.confirm('Permanently delete this replay message?')) return
  HTTP.request('DELETE', HTTP.url`api/replay/${vhost}/${containerName}/${id}`)
    .then(() => {
      DOM.toast('Message deleted')
      window.location.href = `replay#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(containerName)}`
    })
})

if (vhost && containerName && id) {
  loadMessage()
} else {
  DOM.toast('Missing vhost / name / id in URL', 'error')
}
