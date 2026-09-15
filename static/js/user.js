import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
import * as Form from './form.js'

const urlParams = new URLSearchParams(window.location.hash.substring(1))
const user = urlParams.get('name')
// Set for users scoped to a single vhost
const userVhost = urlParams.get('vhost')
const userUrl = userVhost ? HTTP.url`api/vhosts/${userVhost}/users/${user}` : HTTP.url`api/users/${user}`
const permissionsUrl = userVhost ? HTTP.url`api/vhosts/${userVhost}/users/${user}/permissions` : HTTP.url`api/users/${user}/permissions`

function permissionUrl (vhost) {
  return userVhost ? permissionsUrl : HTTP.url`api/permissions/${vhost}/${user}`
}

function updateUser () {
  HTTP.request('GET', userUrl)
    .then(item => {
      const hasPassword = item.password_hash ? '●' : '○'
      document.getElementById('tags').textContent = item.tags
      document.getElementById('hasPassword').textContent = hasPassword
      document.getElementById('scope').textContent = item.vhost ? item.vhost : '(global)'
      tagHelper(item.tags)
    })
    .catch(() => {})
}

function tagHelper (tags) {
  const vals = tags.split(',')
  vals.forEach((val) => {
    const currentVal = document.querySelector('[name=tags]').value
    document.querySelector('[name=tags]').value = currentVal ? currentVal + ', ' + val : val
  })
}

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
        HTTP.request('DELETE', permissionUrl(item.vhost))
          .then(() => {
            tr.parentNode.removeChild(tr)
          })
          .catch(() => {})
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

if (userVhost) {
  // A vhost scoped user can only have permissions on its own vhost
  const select = document.querySelector('#setPermission select[name="vhost"]')
  const opt = document.createElement('option')
  opt.value = userVhost
  opt.textContent = userVhost
  select.appendChild(opt)
  select.value = userVhost
} else {
  Helpers.addVhostOptions('setPermission')
}

document.querySelector('#setPermission').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = data.get('vhost')
  const url = permissionUrl(vhost)
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
    .catch(() => {})
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
  const url = userUrl
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
    .catch(() => {})
})

document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelper('updateUser', 'tags', e)
})

document.querySelector('#deleteUser').addEventListener('submit', function (evt) {
  evt.preventDefault()
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', userUrl)
      .then(() => { window.location = 'users' })
      .catch(() => {})
  }
})

document.addEventListener('DOMContentLoaded', _ => {
  const title = userVhost ? `${userVhost}:${user}` : user
  document.title = title + ' | LavinMQ'
  document.querySelector('#pagename-label').textContent = title
  updateUser()
})
