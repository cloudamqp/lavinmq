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
  url += HTTP.url`/${vhost}`
}
const policiesDataSource = new UrlDataSource(url)

const showManagedToggle = document.querySelector('#show-managed-policies')
if (showManagedToggle) {
  const stored = window.sessionStorage.getItem('show-managed-policies') === 'true'
  showManagedToggle.checked = stored
  const baseQueryParams = policiesDataSource.queryParams.bind(policiesDataSource)
  policiesDataSource.queryParams = function (params) {
    const merged = baseQueryParams(params)
    if (showManagedToggle.checked) {
      merged.set('include_managed', 'true')
    } else {
      merged.delete('include_managed')
    }
    return merged
  }
  showManagedToggle.addEventListener('change', () => {
    window.sessionStorage.setItem('show-managed-policies', String(showManagedToggle.checked))
    policiesDataSource.reload({ updateState: false })
  })
}
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
  if (item.name.startsWith('__queue-filter__')) {
    const queueName = item.name.substring('__queue-filter__'.length)
    const link = document.createElement('a')
    link.href = HTTP.url`queue#vhost=${item.vhost}&name=${queueName}&scrollTo=filter`
    link.className = 'btn btn-outlined'
    link.textContent = 'Manage on queue'
    link.title = 'Auto-managed by the queue\'s Live Filter card'
    buttons.append(link)
  } else {
    const deleteBtn = DOM.button.delete({
      click: function () {
        const name = item.name
        const vhost = item.vhost
        const url = HTTP.url`${HTTP.noencode(baseUrl)}/${vhost}/${name}`
        if (window.confirm('Are you sure? This policy cannot be recovered after deletion.')) {
          HTTP.request('DELETE', url)
            .then(() => tr.parentNode.removeChild(tr))
        }
      }
    })
    const editBtn = DOM.button.edit({
      click: function () {
        Form.editItem('#createPolicy', item, {
          definition: item => Helpers.formatJSONargument(item.definition || {})
        })
      }
    })
    buttons.append(editBtn, deleteBtn)
  }
  Table.renderCell(tr, 6, buttons, 'right')
})

document.querySelector('#createPolicy').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = data.get('name').trim()
  const vhost = data.get('vhost')
  const url = HTTP.url`${HTTP.noencode(baseUrl)}/${vhost}/${name}`
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
document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelperJSON('createPolicy', 'definition', e)
})

Helpers.addVhostOptions('createPolicy')
