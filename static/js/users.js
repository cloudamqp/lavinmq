import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'

let usersTable = null
HTTP.request('GET', 'api/permissions').then(permissions => {
  const tableOptions = {
    url: 'api/users',
    keyColumns: ['vhost', 'name'],
    autoReloadTimeout: 0,
    pagination: true,
    columnSelector: true,
    search: true
  }
  usersTable = Table.renderTable('users', tableOptions, (tr, item, all) => {
    if (all) {
      const userLink = document.createElement('a')
      userLink.href = item.vhost ? HTTP.url`user#name=${item.name}&vhost=${item.vhost}` : HTTP.url`user#name=${item.name}`
      userLink.textContent = item.name
      Table.renderCell(tr, 0, userLink)
      Table.renderCell(tr, 1, item.vhost ? item.vhost : '(global)')
    }
    const hasPassword = item.password_hash ? '●' : '○'
    // Vhost scoped users can only have permissions on their own vhost
    const vhosts = permissions
      .filter(p => p.user === item.name && (item.vhost ? p.vhost_scoped && p.vhost === item.vhost : !p.vhost_scoped))
      .map(p => p.vhost).join(', ')
    Table.renderCell(tr, 2, item.tags)
    Table.renderCell(tr, 3, vhosts)
    Table.renderCell(tr, 4, hasPassword)
  })
}).catch(e => {
  Table.toggleDisplayError('users', e.status === 403 ? 'You need administrator role to see this view' : e.body)
})

document.querySelector('#createUser').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const username = data.get('username').trim()
  const vhost = data.get('vhost')
  const url = vhost ? HTTP.url`api/vhosts/${vhost}/users/${username}` : HTTP.url`api/users/${username}`
  let toastText = `User created: '${username}'`
  const trs = document.querySelectorAll('#table tbody tr')
  trs.forEach((tr) => {
    if (JSON.stringify(username) === tr.dataset.name && JSON.stringify(vhost || null) === tr.dataset.vhost) {
      window.confirm(`Are you sure? This will update existing user: '${username}'`)
      toastText = `Upated existing user: '${username}'`
      if (data.get('tags') === '') { data.set('tags', tr.childNodes[2].textContent) }
    }
  })
  const body = {
    tags: data.get('tags')
  }
  if (data.get('password') !== '') {
    body.password = data.get('password')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      usersTable.reload()
      DOM.toast(toastText)
      evt.target.reset()
    })
    .catch(() => {})
})

Helpers.addVhostOptions('createUser', { addAll: false }).then(() => {
  const select = document.querySelector('#createUser select[name="vhost"]')
  const globalOpt = document.createElement('option')
  globalOpt.value = ''
  globalOpt.textContent = '(global)'
  select.prepend(globalOpt)
  select.value = ''
})

document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelper('createUser', 'tags', e)
})

document.querySelector('#generatePassword').addEventListener('click', generatePassword)

document.querySelector('.password-toggle').addEventListener('click', togglePasswordAsPlainText)

function generatePassword () {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-=[]{}|;:,.<>?'
  const password = Array.from(window.crypto.getRandomValues(new Uint8Array(16)), x => chars[x % chars.length]).join('')
  const input = document.querySelector('#createUser input[name="password"]')
  input.value = password
  input.type = 'text'
  setTimeout(() => { input.type = 'password' }, 500)
}

function togglePasswordAsPlainText () {
  const input = document.querySelector('#createUser input[name="password"]')
  const isPassword = input.type === 'password'
  input.type = isPassword ? 'text' : 'password'
}
