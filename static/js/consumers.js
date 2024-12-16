import * as Table from './table.js'
import * as HTTP from './http.js'
import * as DOM from './dom.js'

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/consumers'
if (vhost && vhost !== '_all') {
  url = `api/consumers/${encodeURIComponent(vhost)}`
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
  channelLink.href = 'channel#name=' + encodeURIComponent(item.channel_details.name)
  channelLink.textContent = item.channel_details.name
  const ack = item.ack_required ? '●' : '○'
  const exclusive = item.exclusive ? '●' : '○'
  const cancelForm = document.createElement('form')
  const btn = document.createElement('button')
  btn.classList.add('btn-small-outlined-danger')
  btn.type = 'submit'
  btn.textContent = 'Cancel'

  cancelForm.appendChild(btn)
  const urlEncodedVhost = encodeURIComponent(item.queue.vhost)
  const urlEncodedConsumerTag = encodeURIComponent(item.consumer_tag)
  const conn = encodeURIComponent(item.channel_details.connection_name)
  const ch = encodeURIComponent(item.channel_details.number)
  const actionPath = `api/consumers/${urlEncodedVhost}/${conn}/${ch}/${urlEncodedConsumerTag}`
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
