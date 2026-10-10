import * as HTTP from './http.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
import * as Form from './form.js'
import { UrlDataSource } from './datasource.js'

const params = new URLSearchParams(window.location.hash.substring(1))
const vhost = params.get('vhost')
const group = params.get('name')
document.title = group + ' | LavinMQ'
document.querySelector('#pagename-label').textContent = group + ' in virtual host ' + vhost

const groupUrl = HTTP.url`api/mqtt/permission-groups/${vhost}/${group}`

// The hash identifies the group, so keep the data sources from reading it as
// their own query state, where `name` means search term.
const dataSource = url => new UrlDataSource(url, { useQueryState: false, autoReloadTimeout: 0 })

const membersTable = Table.renderTable('members', {
  dataSource: dataSource(groupUrl + '/members'),
  keyColumns: ['username'],
  countId: 'members-count',
  pagination: true
}, (tr, item, all) => {
  if (!all) return
  // Plain text, no user page link: a member does not have to exist in the local user store
  Table.renderCell(tr, 0, item.username)
  const btn = DOM.button.delete({
    text: 'Remove',
    click: function () {
      if (!window.confirm(`Are you sure? '${item.username}' loses the access that this group grants.`)) return
      const url = HTTP.url`api/mqtt/permission-groups/${vhost}/${group}/members/${item.username}`
      HTTP.request('DELETE', url)
        .then(() => membersTable.reload())
        .catch(() => {})
    }
  })
  Table.renderCell(tr, 1, btn, 'right')
})

const rulesTable = Table.renderTable('rules', {
  dataSource: dataSource(groupUrl + '/rules'),
  keyColumns: ['identifier'],
  countId: 'rules-count',
  pagination: true
}, (tr, item, all) => {
  Table.renderCell(tr, 1, item.pattern)
  Table.renderCell(tr, 2, item.read ? '●' : '○', 'center')
  Table.renderCell(tr, 3, item.write ? '●' : '○', 'center')
  if (!all) return
  Table.renderCell(tr, 0, item.identifier)
  const buttons = document.createElement('div')
  buttons.classList.add('buttons')
  const editBtn = DOM.button.edit({
    click: function () {
      Form.editItem('#setRule', item)
    }
  })
  const deleteBtn = DOM.button.delete({
    text: 'Remove',
    click: function () {
      if (!window.confirm(`Are you sure? Clients lose the access that the rule '${item.identifier}' grants.`)) return
      const url = HTTP.url`api/mqtt/permission-groups/${vhost}/${group}/rules/${item.identifier}`
      HTTP.request('DELETE', url)
        .then(() => rulesTable.reload())
        .catch(() => {})
    }
  })
  buttons.append(editBtn, deleteBtn)
  Table.renderCell(tr, 4, buttons, 'right')
})

document.querySelector('#addMember').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const username = new window.FormData(this).get('username').trim()
  const url = HTTP.url`api/mqtt/permission-groups/${vhost}/${group}/members/${username}`
  HTTP.request('PUT', url)
    .then(() => {
      membersTable.reload()
      DOM.toast(`Member added: '${username}'`)
      evt.target.reset()
    })
    .catch(() => {})
})

document.querySelector('#setRule').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const identifier = data.get('identifier').trim()
  const url = HTTP.url`api/mqtt/permission-groups/${vhost}/${group}/rules/${identifier}`
  const body = {
    pattern: data.get('pattern'),
    read: data.has('read'),
    write: data.has('write')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      rulesTable.reload()
      DOM.toast(`Rule saved: '${identifier}'`)
      evt.target.reset()
    })
    .catch(() => {})
})

document.querySelector('#deleteGroup').addEventListener('submit', function (evt) {
  evt.preventDefault()
  if (window.confirm('Are you sure? This deletes the group with all its members and rules.')) {
    HTTP.request('DELETE', groupUrl)
      .then(() => { window.location = 'mqtt-permissions' })
      .catch(() => {})
  }
})

// Suggest existing users when adding a member; typing any name still works.
HTTP.request('GET', 'api/users').then(users => {
  const list = document.getElementById('userList')
  for (const user of Array.isArray(users) ? users : users.items) {
    list.appendChild(new window.Option(user.name))
  }
}).catch(() => {})
