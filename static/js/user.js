import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'
import * as Form from './form.js'

const user = new URLSearchParams(window.location.hash.substring(1)).get('name')
const urlEncodedUsername = encodeURIComponent(user)

function updateUser () {
  const userUrl = 'api/users/' + urlEncodedUsername
  HTTP.request('GET', userUrl)
    .then(item => {
      const hasPassword = item.password_hash ? '●' : '○'
      document.getElementById('tags').textContent = item.tags
      document.getElementById('hasPassword').textContent = hasPassword
      tagHelper(item.tags)
    }).catch(HTTP.standardErrorHandler)
}

function tagHelper(tags) {
  const vals = tags.split(',')
  vals.forEach((val) =>  {
    const currentVal = document.querySelector(`[name=tags]`).value
    document.querySelector(`[name=tags]`).value = currentVal ? currentVal + ', ' + val : val
  })
}

const permissionsUrl = 'api/users/' + urlEncodedUsername + '/permissions'
const tableOptions = { url: permissionsUrl, keyColumns: ['vhost'], interval: 0, countId: "permissions-count" }
const permissionsTable = Table.renderTable('permissions', tableOptions, (tr, item, all) => {
  Table.renderCell(tr, 1, item.configure)
  Table.renderCell(tr, 2, item.write)
  Table.renderCell(tr, 3, item.read)
  if (all) {
    const buttons = document.createElement('div')
    buttons.classList.add('buttons')
    const deleteBtn = document.createElement('button')
    deleteBtn.classList.add('btn-danger')
    deleteBtn.innerText = 'Clear'
    deleteBtn.onclick = function () {
      const username = encodeURIComponent(item.user)
      const vhost = encodeURIComponent(item.vhost)
      const url = 'api/permissions/' + vhost + '/' + username
      HTTP.request('DELETE', url)
        .then(() => {
          tr.parentNode.removeChild(tr)
        }).catch(HTTP.standardErrorHandler)
    }
    const editBtn = document.createElement('button')
    editBtn.classList.add('btn-secondary')
    editBtn.innerText = 'Edit'
    editBtn.onclick = function() {
      Form.editItem('#setPermission', item)
    }
    buttons.append(editBtn, deleteBtn)
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 4, buttons, 'right')
  }
})

Helpers.addVhostOptions('setPermission')

document.querySelector('#setPermission').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = encodeURIComponent(data.get('vhost'))
  const url = 'api/permissions/' + vhost + '/' + urlEncodedUsername
  const body = {
    configure: data.get('configure'),
    write: data.get('write'),
    read: data.get('read')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      permissionsTable.reload()
      evt.target.reset()
    }).catch(HTTP.standardErrorHandler)
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
  const data = new window.FormData(this)
  const url = 'api/users/' + urlEncodedUsername
  const body = {
    tags: data.get('tags')
  }
  if (data.get('remove_password') === 'on') {
    body.password_hash = ''
  } else if (data.get('password') != "") {
    body.password = data.get('password')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      updateUser()
      DOM.toast("User updated")
      evt.target.reset()
      pwd.disabled = false
      pwd.required = true
    }).catch(HTTP.standardErrorHandler)
})

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelper('updateUser', 'tags', e)
}

document.querySelector('#deleteUser').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = 'api/users/' + urlEncodedUsername
  if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = 'users' })
      .catch(HTTP.standardErrorHandler)
  }
})

document.addEventListener('DOMContentLoaded', _ => {
  document.title = user + ' | LavinMQ'
  document.querySelector('#pagename-label').textContent = user
  updateUser()
})

