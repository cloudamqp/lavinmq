import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Form from './form.js'
import { UrlDataSource } from './datasource.js'

interface PolicyItem {
  vhost: string
  name: string
  pattern: string
  'apply-to': string
  definition: Record<string, unknown>
  priority: number
}

// this module is only included from /policies and /operator-policies
const baseUrl = `api${document.location.pathname}`
let url = baseUrl

const vhost = window.sessionStorage.getItem('vhost')
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
}
const policiesDataSource = new UrlDataSource<PolicyItem>(url)
const tableOptions = {
  dataSource: policiesDataSource,
  keyColumns: ['vhost', 'name'],
  pagination: true,
  columnSelector: true,
  search: true,
}
const policiesTable = Table.renderTable<PolicyItem>('table', tableOptions, (tr, item) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  Table.renderCell(tr, 2, item.pattern)
  Table.renderCell(tr, 3, item['apply-to'])
  Table.renderCell(tr, 4, JSON.stringify(item.definition))
  Table.renderCell(tr, 5, item.priority)

  const buttons = document.createElement('div')
  buttons.classList.add('buttons')
  const deleteBtn = DOM.button.delete({
    click: function () {
      const name = item.name
      const itemVhost = item.vhost
      const deleteUrl = HTTP.url`${HTTP.noencode(baseUrl)}/${itemVhost}/${name}`
      if (window.confirm('Are you sure? This policy cannot be recovered after deletion.')) {
        HTTP.request('DELETE', deleteUrl).then(() => tr.parentNode?.removeChild(tr))
      }
    },
  })
  const editBtn = DOM.button.edit({
    click: function () {
      Form.editItem<PolicyItem>('#createPolicy', item, {
        definition: (policyItem) => Helpers.formatJSONargument(policyItem.definition || {}),
      })
    },
  })
  buttons.append(editBtn, deleteBtn)
  Table.renderCell(tr, 6, buttons, 'right')
})

const createForm = document.querySelector('#createPolicy')
if (createForm) {
  createForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const data = new FormData(form)
    const name = (data.get('name') as string).trim()
    const formVhost = data.get('vhost') as string
    const createUrl = HTTP.url`${HTTP.noencode(baseUrl)}/${formVhost}/${name}`
    const body = {
      pattern: (data.get('pattern') as string).trim(),
      definition: DOM.parseJSON(data.get('definition') as string),
      'apply-to': data.get('apply-to'),
      priority: parseInt(data.get('priority') as string, 10),
    }
    HTTP.request('PUT', createUrl, { body }).then(() => {
      policiesTable.reload()
      form.reset()
    })
  })
}

const dataTags = document.querySelector('#dataTags')
if (dataTags) {
  dataTags.addEventListener('click', (e) => {
    Helpers.argumentHelperJSON('createPolicy', 'definition', e)
  })
}

Helpers.addVhostOptions('createPolicy')
