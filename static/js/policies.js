import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Vhosts from './vhosts.js'

Vhosts.addVhostOptions('createPolicy').then(() => {
  autofill_editpolicy(policiesTable.getData())
})

let url = '/api/policies'
const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += '/' + encodeURIComponent(vhost)
}
const tableOptions = {
  url,
  keyColumns: ['vhost', 'name'],
  pagination: true,
  columnSelector: true,
  search: true
}
const policiesTable = Table.renderTable('table', tableOptions, (tr, item) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  tr.cells[1].classList.add('self-link')
  tr.cells[1].onclick = () => { autofill_editpolicy(item, false) }
  Table.renderCell(tr, 2, item.pattern)
  Table.renderCell(tr, 3, item['apply-to'])
  Table.renderCell(tr, 4, JSON.stringify(item.definition))
  Table.renderCell(tr, 5, item.priority)

  const btn = document.createElement('button')
  btn.classList.add('btn-danger')
  btn.innerHTML = 'Delete'
  btn.onclick = function () {
    const name = encodeURIComponent(item.name)
    const vhost = encodeURIComponent(item.vhost)
    const url = '/api/policies/' + vhost + '/' + name
    if (window.confirm('Are you sure? This policy cannot be recovered after deletion.')) {
      HTTP.request('DELETE', url)
        .then(() => {
          DOM.removeNodes(tr)
        }).catch(HTTP.standardErrorHandler)
    }
  }
  Table.renderCell(tr, 6, btn, 'right')
})

document.querySelector('#createPolicy').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = encodeURIComponent(data.get('name').trim())
  const vhost = encodeURIComponent(data.get('vhost'))
  const url = '/api/policies/' + vhost + '/' + name
  const body = {
    pattern: data.get('pattern').trim(),
    definition: DOM.parseJSON(data.get('definition')),
    'apply-to': data.get('apply-to'),
    priority: parseInt(data.get('priority'))
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      policiesTable.fetchAndUpdate()
      evt.target.reset()
    }).catch(HTTP.standardErrorHandler)
})
document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON("definition", e)
}

function autofill_editpolicy(policies, otherOrigin = true) {
  let policy = null
  if (otherOrigin) {
    const urlParams = new URLSearchParams(window.location.search);
    const pname = urlParams.get('name')
    const pvhost = urlParams.get('vhost');
    if (!(pname && pvhost)) {
      return
    }
    policy = policies.filter(item => {
      return item.name === pname && item.vhost === pvhost
    })[0]
  } else {
    policy = policies
  }

  document.getElementById('addPolicyVhost').value = policy.vhost
  document.getElementsByName('name')[0].value = policy.name
  document.getElementById('addPolicyApplyTo').value = policy["apply-to"]
  document.getElementsByName('pattern')[0].value = policy.pattern
  document.getElementsByName('definition')[0].value = JSON.stringify(policy.definition)
  document.getElementsByName('priority')[0].value = policy.priority
}

