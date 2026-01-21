import * as Table from './table.js'
import { UrlDataSource } from './datasource.js'
import * as HTTP from './http.js'

interface UnackedItem {
  delivery_tag: string
  consumer_tag: string
  channel_name: string
  unacked_for_seconds: number
}

const search = new URLSearchParams(window.location.hash.substring(1))
const queue = search.get('name') ?? ''
const vhost = search.get('vhost') ?? ''
const url = HTTP.url`api/queues/${vhost}/${queue}/unacked`
const queueLink = document.getElementById('queue-link') as HTMLAnchorElement | null
if (queueLink) {
  queueLink.href = HTTP.url`queue#vhost=${vhost}&name=${queue}`
  queueLink.innerHTML = queue
}
const pagenameLabel = document.querySelector('#pagename-label')
if (pagenameLabel) {
  pagenameLabel.textContent = `${queue} in virtual host ${vhost}`
}

const tableOptions = {
  dataSource: new UrlDataSource<UnackedItem>(url, { useQueryState: false }),
  keyColumns: ['delivery_tag', 'channel_name'],
  pagination: true,
  columnSelector: true,
  search: false,
  countId: 'unacked-count',
}

Table.renderTable<UnackedItem>('table', tableOptions, function (tr, item, firstRender) {
  if (firstRender) {
    Table.renderCell(tr, 0, item.delivery_tag)
    Table.renderCell(tr, 1, item.consumer_tag)
    const channel = document.createElement('a')
    channel.href = HTTP.url`channel#name=${item.channel_name}`
    channel.textContent = item.channel_name
    Table.renderCell(tr, 2, channel)
  }
  Table.renderCell(tr, 3, item.unacked_for_seconds, 'right')
})
