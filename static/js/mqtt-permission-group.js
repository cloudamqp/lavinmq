import * as HTTP from './http.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
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
  pagination: true,
  search: true
}, (tr, item, all) => {
  if (!all) return
  if (item.username === '*') {
    Table.renderCell(tr, 0, '*')
  } else {
    const userLink = document.createElement('a')
    userLink.href = HTTP.url`user#name=${item.username}`
    userLink.textContent = item.username
    Table.renderCell(tr, 0, userLink)
  }
  const btn = DOM.button.delete({
    text: 'Remove',
    click: function () {
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
  countId: 'rules-count'
}, (tr, item, all) => {
  Table.renderCell(tr, 1, item.pattern)
  Table.renderCell(tr, 2, item.read ? '●' : '○', 'center')
  Table.renderCell(tr, 3, item.write ? '●' : '○', 'center')
  if (all) {
    const ruleLink = document.createElement('a')
    ruleLink.href = HTTP.url`mqtt-permission-rule#vhost=${vhost}&group=${group}&rule=${item.identifier}`
    ruleLink.textContent = item.identifier
    Table.renderCell(tr, 0, ruleLink)
  }
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
