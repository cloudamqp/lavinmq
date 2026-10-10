import * as HTTP from './http.js'
import * as DOM from './dom.js'

const params = new URLSearchParams(window.location.hash.substring(1))
const vhost = params.get('vhost')
const group = params.get('group')
const identifier = params.get('rule')
document.title = identifier + ' | LavinMQ'
document.querySelector('#pagename-label').textContent = identifier + ' in group ' + group

const groupPage = HTTP.url`mqtt-permission-group#vhost=${vhost}&name=${group}`
const ruleUrl = HTTP.url`api/mqtt/permission-groups/${vhost}/${group}/rules/${identifier}`
const form = document.forms.setRule

document.getElementById('rule-identifier').textContent = identifier
document.getElementById('rule-vhost').textContent = vhost
const groupLink = document.getElementById('group-link')
groupLink.href = groupPage
groupLink.textContent = group
document.getElementById('cancel-link').href = groupPage

// There is no endpoint for a single rule, so find it in the group's rules.
HTTP.request('GET', HTTP.url`api/mqtt/permission-groups/${vhost}/${group}/rules`).then(rules => {
  const rule = rules.find(r => r.identifier === identifier)
  if (!rule) {
    DOM.toast.error(`Rule '${identifier}' not found in group '${group}'`)
    return
  }
  form.elements.pattern.value = rule.pattern
  form.elements.read.checked = rule.read
  form.elements.write.checked = rule.write
}).catch(() => {})

form.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const body = {
    pattern: data.get('pattern'),
    read: data.has('read'),
    write: data.has('write')
  }
  HTTP.request('PUT', ruleUrl, { body })
    .then(() => { window.location = groupPage })
    .catch(() => {})
})

document.querySelector('#deleteRule').addEventListener('submit', function (evt) {
  evt.preventDefault()
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', ruleUrl)
      .then(() => { window.location = groupPage })
      .catch(() => {})
  }
})
