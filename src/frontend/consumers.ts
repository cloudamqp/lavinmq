import * as Table from './table.js'
import * as HTTP from './http.js'
import * as DOM from './dom.js'

interface ChannelDetails {
  name: string
  connection_name: string
  number: number
}

interface QueueDetails {
  name: string
  vhost: string
}

interface ConsumerItem {
  channel_details: ChannelDetails
  consumer_tag: string
  queue: QueueDetails
  ack_required: boolean
  exclusive: boolean
  prefetch_count: number
}

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/consumers'
if (vhost && vhost !== '_all') {
  url = HTTP.url`api/consumers/${vhost}`
}

const tableOptions = {
  url,
  keyColumns: ['channel_details', 'consumer_tag'],
  pagination: true,
  columnSelector: true,
  search: true,
}

Table.renderTable<ConsumerItem>('table', tableOptions, function (tr, item, firstRender) {
  if (!firstRender) return
  const channelLink = document.createElement('a')
  channelLink.href = HTTP.url`channel#name=${item.channel_details.name}`
  channelLink.textContent = item.channel_details.name
  const ack = item.ack_required ? '\u25CF' : '\u25CB'
  const exclusive = item.exclusive ? '\u25CF' : '\u25CB'
  const cancelForm = document.createElement('form')
  const btn = DOM.button.delete({
    text: 'Cancel',
    type: 'submit',
  })

  cancelForm.appendChild(btn)
  const itemVhost = item.queue.vhost
  const consumerTag = item.consumer_tag
  const conn = item.channel_details.connection_name
  const ch = item.channel_details.number
  const actionPath = HTTP.url`api/consumers/${itemVhost}/${conn}/${String(ch)}/${consumerTag}`
  cancelForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    if (!window.confirm('Are you sure?')) return
    HTTP.request('DELETE', actionPath).then(() => {
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
