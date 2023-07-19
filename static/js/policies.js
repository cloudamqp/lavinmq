import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Form from './form.js'
import { UrlDataSource } from './datasource.js'

// this module is only included from /policies and /operator-policies
const baseUrl = `api${document.location.pathname}`
let url = baseUrl

const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += '/' + encodeURIComponent(vhost)
}
const policiesDataSource = new UrlDataSource(url)
const tableOptions = {
  dataSource: policiesDataSource,
  keyColumns: ['vhost', 'name'],
  pagination: true,
  columnSelector: true,
  search: true
}
const policiesTable = Table.renderTable('table', tableOptions, (tr, item) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, item.pattern)
  Table.renderCell(tr, 3, item['apply-to'])
  Table.renderCell(tr, 4, JSON.stringify(item.definition))
  Table.renderCell(tr, 5, item.priority)

  const buttons = document.createElement('div')
  buttons.classList.add('buttons')
  const deleteBtn = document.createElement('button')
  deleteBtn.classList.add('btn-danger')
  deleteBtn.textContent = 'Delete'
  deleteBtn.onclick = function () {
    const name = encodeURIComponent(item.name)
    const vhost = encodeURIComponent(item.vhost)
    const url = `${baseUrl}/${vhost}/${name}`
    if (window.confirm('Are you sure? This policy cannot be recovered after deletion.')) {
      HTTP.request('DELETE', url)
        .then(() => tr.parentNode.removeChild(tr))
    }
  }
  const editBtn = document.createElement('button')
  editBtn.classList.add('btn-secondary')
  editBtn.textContent = 'Edit'
  editBtn.onclick = function () {
    Form.editItem('#createPolicy', item, {
      definition: item => Helpers.formatJSONargument(item.definition || {})
    })
  }
  buttons.append(editBtn, deleteBtn)
  Table.renderCell(tr, 6, buttons, 'right')
})

document.querySelector('#createPolicy').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = encodeURIComponent(data.get('name').trim())
  const vhost = encodeURIComponent(data.get('vhost'))
  const url = `${baseUrl}/${vhost}/${name}`
  const body = {
    pattern: data.get('pattern').trim(),
    definition: DOM.parseJSON(data.get('definition')),
    'apply-to': data.get('apply-to'),
    priority: parseInt(data.get('priority'))
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      policiesTable.reload()
      evt.target.reset()
    })
})
document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON('createPolicy', 'definition', e)
}

Helpers.addVhostOptions('createPolicy')
