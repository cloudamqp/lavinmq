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
      userLink.href = HTTP.url`user#name=${item.name}`
      userLink.textContent = item.name
      Table.renderCell(tr, 0, userLink)
    }
    const hasPassword = item.password_hash ? '●' : '○'
    const vhosts = permissions.filter(p => p.user === item.name).map(p => p.vhost).join(', ')
    Table.renderCell(tr, 1, item.tags)
    Table.renderCell(tr, 2, vhosts)
    Table.renderCell(tr, 3, hasPassword)
  })
}).catch(e => {
  Table.toggleDisplayError('users', e.status === 403 ? 'You need administrator role to see this view' : e.body)
})

document.querySelector('#createUser').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const username = data.get('username').trim()
  const url = HTTP.url`api/users/${username}`
  let toastText = `User created: '${username}'`
  const trs = document.querySelectorAll('#table tbody tr')
  trs.forEach((tr) => {
    if (username === tr.getAttribute('data-name')) {
      window.confirm(`Are you sure? This will update existing user: '${username}'`)
      toastText = `Upated existing user: '${username}'`
      if (data.get('tags') === '') { data.set('tags', tr.childNodes[1].textContent) }
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
})

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelper('createUser', 'tags', e)
}

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
