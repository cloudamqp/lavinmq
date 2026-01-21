import * as HTTP from './http.js'
import * as Table from './table.js'
import * as DOM from './dom.js'

interface VhostInfo {
  messages_ready: number
  messages_unacknowledged: number
  messages: number
}

interface VhostLimit {
  value: {
    'max-connections'?: number
    'max-queues'?: number
  }
}

interface PermissionItem {
  user: string
  configure: string
  write: string
  read: string
}

interface UserItem {
  name: string
}

const vhost = new URLSearchParams(window.location.hash.substring(1)).get('name') ?? ''
document.title = vhost + ' | LavinMQ'
const pagenameLabel = document.querySelector('#pagename-label')
if (pagenameLabel) pagenameLabel.textContent = vhost

const vhostUrl = HTTP.url`api/vhosts/${vhost}`
HTTP.request<VhostInfo>('GET', vhostUrl).then((item) => {
  if (!item) return
  const messagesReadyEl = document.getElementById('messages_ready')
  const messagesUnackedEl = document.getElementById('messages_unacknowledged')
  const messagesTotalEl = document.getElementById('messages_total')
  if (messagesReadyEl) messagesReadyEl.textContent = item.messages_ready.toLocaleString()
  if (messagesUnackedEl) messagesUnackedEl.textContent = item.messages_unacknowledged.toLocaleString()
  if (messagesTotalEl) messagesTotalEl.textContent = item.messages.toLocaleString()
})

function fetchLimits(): void {
  HTTP.request<VhostLimit[]>('GET', HTTP.url`api/vhost-limits/${vhost}`).then((arr) => {
    const limits = arr?.[0] ?? { value: {} }
    const maxConnections = limits.value['max-connections'] ?? ''
    const maxConnectionsEl = document.getElementById('max-connections')
    if (maxConnectionsEl) maxConnectionsEl.textContent = String(maxConnections)
    const maxConnectionsInput = (document.forms.namedItem('setLimits') as HTMLFormElement | null)?.elements.namedItem(
      'max-connections'
    ) as HTMLInputElement | null
    if (maxConnectionsInput) maxConnectionsInput.value = String(maxConnections)
    const maxQueues = limits.value['max-queues'] ?? ''
    const maxQueuesEl = document.getElementById('max-queues')
    if (maxQueuesEl) maxQueuesEl.textContent = String(maxQueues)
    const maxQueuesInput = (document.forms.namedItem('setLimits') as HTMLFormElement | null)?.elements.namedItem(
      'max-queues'
    ) as HTMLInputElement | null
    if (maxQueuesInput) maxQueuesInput.value = String(maxQueues)
  })
}
fetchLimits()

const permissionsUrl = HTTP.url`api/vhosts/${vhost}/permissions`
const tableOptions = { url: permissionsUrl, keyColumns: ['user'], countId: 'permissions-count' }
const permissionsTable = Table.renderTable<PermissionItem>('permissions', tableOptions, (tr, item, all) => {
  Table.renderCell(tr, 1, item.configure)
  Table.renderCell(tr, 2, item.write)
  Table.renderCell(tr, 3, item.read)
  if (all) {
    const btn = DOM.button.delete({
      text: 'Clear',
      click: function () {
        const url = HTTP.url`api/permissions/${vhost}/${item.user}`
        HTTP.request('DELETE', url).then(() => tr.parentNode?.removeChild(tr))
      },
    })
    const userLink = document.createElement('a')
    userLink.href = HTTP.url`user#name=${item.user}`
    userLink.textContent = item.user
    Table.renderCell(tr, 0, userLink)
    Table.renderCell(tr, 4, btn, 'right')
  }
})

function addUserOptions(users: UserItem[]): void {
  const form = document.forms.namedItem('setPermission') as HTMLFormElement | null
  const select = form?.elements.namedItem('user') as HTMLSelectElement | null
  if (!select) return
  while (select.options.length) select.remove(0)
  for (let i = 0; i < users.length; i++) {
    const opt = document.createElement('option')
    opt.text = users[i]?.name ?? ''
    select.add(opt)
  }
}

function fetchUsers(cb: (users: UserItem[]) => void): void {
  const url = 'api/users'
  const raw = window.sessionStorage.getItem(url)
  if (raw) {
    const users = JSON.parse(raw) as UserItem[]
    cb(users)
  }
  HTTP.request<UserItem[]>('GET', url)
    .then(function (users) {
      if (!users) return
      try {
        window.sessionStorage.setItem('api/users', JSON.stringify(users))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      cb(users)
    })
    .catch(function (e: Error) {
      console.error(e.message)
    })
}
fetchUsers(addUserOptions)

const setPermForm = document.querySelector('#setPermission')
if (setPermForm) {
  setPermForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const data = new FormData(form)
    const url = HTTP.url`api/permissions/${vhost}/${data.get('user') as string}`
    const body = {
      configure: data.get('configure'),
      write: data.get('write'),
      read: data.get('read'),
    }
    HTTP.request('PUT', url, { body }).then(() => {
      permissionsTable.reload()
      form.reset()
    })
  })
}

const setLimitsForm = document.forms.namedItem('setLimits')
if (setLimitsForm) {
  setLimitsForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const form = evt.target as HTMLFormElement
    const maxConnectionsUrl = HTTP.url`api/vhost-limits/${vhost}/max-connections`
    const maxConnectionsInput = form['max-connections'] as HTMLInputElement
    const maxConnectionsBody = { value: Number(maxConnectionsInput.value || -1) }
    const maxQueuesUrl = HTTP.url`api/vhost-limits/${vhost}/max-queues`
    const maxQueuesInput = form['max-queues'] as HTMLInputElement
    const maxQueuesBody = { value: Number(maxQueuesInput.value || -1) }
    Promise.all([
      HTTP.request('PUT', maxConnectionsUrl, { body: maxConnectionsBody }),
      HTTP.request('PUT', maxQueuesUrl, { body: maxQueuesBody }),
    ]).then(fetchLimits)
  })
}

const deleteForm = document.querySelector('#deleteVhost')
if (deleteForm) {
  deleteForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = HTTP.url`api/vhosts/${vhost}`
    if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
      HTTP.request('DELETE', url).then(() => {
        window.location.href = 'vhosts'
      })
    }
  })
}
