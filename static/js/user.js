import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
import * as Form from './form.js'

const user = new URLSearchParams(window.location.hash.substring(1)).get('name')

function updateUser () {
  const userUrl = HTTP.url`api/users/${user}`
  HTTP.request('GET', userUrl)
    .then(item => {
      const hasPassword = item.password_hash ? '●' : '○'
      document.getElementById('tags').textContent = item.tags
      document.getElementById('hasPassword').textContent = hasPassword
      tagHelper(item.tags)
    })
}

function tagHelper (tags) {
  const vals = tags.split(',')
  vals.forEach((val) => {
    const currentVal = document.querySelector('[name=tags]').value
    document.querySelector('[name=tags]').value = currentVal ? currentVal + ', ' + val : val
  })
}

const permissionsUrl = HTTP.url`api/users/${user}/permissions`
const tableOptions = { url: permissionsUrl, keyColumns: ['vhost'], autoReloadTimeout: 0, countId: 'permissions-count' }
const permissionsTable = Table.renderTable('permissions', tableOptions, (tr, item, all) => {
  Table.renderCell(tr, 1, item.configure)
  Table.renderCell(tr, 2, item.write)
  Table.renderCell(tr, 3, item.read)
  if (all) {
    const buttons = document.createElement('div')
    buttons.classList.add('buttons')
    const deleteBtn = DOM.button.delete({
      text: 'Clear',
      click: function () {
        const url = HTTP.url`api/permissions/${item.vhost}/${item.user}`
        HTTP.request('DELETE', url)
          .then(() => {
            tr.parentNode.removeChild(tr)
          })
      }
    })
    const editBtn = DOM.button.edit({
      click: function () {
        Form.editItem('#setPermission', item)
      }
    })
    buttons.append(editBtn, deleteBtn)
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 4, buttons, 'right')
  }
})

Helpers.addVhostOptions('setPermission')

document.querySelector('#setPermission').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = data.get('vhost')
  const url = HTTP.url`api/permissions/${vhost}/${user}`
  const body = {
    configure: data.get('configure'),
    write: data.get('write'),
    read: data.get('read')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      permissionsTable.reload()
      evt.target.reset()
    })
})

document.querySelector('[name=remove_password]').addEventListener('change', function () {
  const pwd = document.querySelector('[name=password]')
  if (this.checked) {
    pwd.disabled = true
    pwd.required = false
  } else {
    pwd.disabled = false
    pwd.required = true
  }
})
document.querySelector('#updateUser').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const pwd = document.querySelector('[name=password]')
  const data = new window.FormData(this)
  const url = HTTP.url`api/users/${user}`
  const body = {
    tags: data.get('tags')
  }
  if (data.get('remove_password') === 'on') {
    body.password_hash = ''
  } else if (data.get('password') !== '') {
    body.password = data.get('password')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      updateUser()
      DOM.toast('User updated')
      evt.target.reset()
      pwd.disabled = false
      pwd.required = true
    })
})

document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelper('updateUser', 'tags', e)
})

document.querySelector('#deleteUser').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = HTTP.url`api/users/${user}`
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = 'users' })
  }
})

document.addEventListener('DOMContentLoaded', _ => {
  document.title = user + ' | LavinMQ'
  document.querySelector('#pagename-label').textContent = user
  updateUser()
})
