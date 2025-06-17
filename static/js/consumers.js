import * as Table from './table.js'
import * as HTTP from './http.js'
import * as DOM from './dom.js'

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/consumers'
if (vhost && vhost !== '_all') {
  url = HTTP.url`api/consumers/${vhost}`
}

const tableOptions = {
  url,
  keyColumns: ['channel_details', 'consumer_tag'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: true
}

Table.renderTable('table', tableOptions, function (tr, item, firstRender) {
  if (!firstRender) return
  const channelLink = document.createElement('a')
  channelLink.href = HTTP.url`channel#name=${item.channel_details.name}`
  channelLink.textContent = item.channel_details.name
  const ack = item.ack_required ? '●' : '○'
  const exclusive = item.exclusive ? '●' : '○'
  const cancelForm = document.createElement('form')
  const btn = DOM.button.delete({
    text: 'Cancel',
    type: 'submit'
  })

  cancelForm.appendChild(btn)
  const vhost = item.queue.vhost
  const consumerTag = item.consumer_tag
  const conn = item.channel_details.connection_name
  const ch = item.channel_details.number
  const actionPath = HTTP.url`api/consumers/${vhost}/${conn}/${ch}/${consumerTag}`
  cancelForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    if (!window.confirm('Are you sure?')) return false
    HTTP.request('DELETE', actionPath)
      .then(() => {
        DOM.toast('Consumer cancelled')
      })
  })

  Table.renderCell(tr, 0, item.queue.vhost)
  Table.renderCell(tr, 1, item.queue.name)
  Table.renderCell(tr, 2, channelLink)
  Table.renderCell(tr, 3, item.consumer_tag)
  Table.renderCell(tr, 4, ack, 'center')
  Table.renderCell(tr, 5, exclusive, 'center')
  Table.renderCell(tr, 6, item.prefetch_count, 'right')
  Table.renderCell(tr, 7, cancelForm, 'center')
})
