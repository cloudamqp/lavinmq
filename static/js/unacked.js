import * as Table from './table.js'
import { UrlDataSource } from './datasource.js'
import * as HTTP from './http.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const queue = search.get('name')
const vhost = search.get('vhost')
const url = HTTP.url`api/queues/${vhost}/${queue}/unacked`
document.getElementById('queue-link').href = HTTP.url`queue#vhost=${vhost}&name=${queue}`
document.getElementById('queue-link').innerHTML = queue
document.querySelector('#pagename-label').textContent = `${queue} in virtual host ${vhost}`

const tableOptions = {
  dataSource: new UrlDataSource(url, { useQueryState: false }),
  keyColumns: ['delivery_tag', 'channel_name'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: false,
  countId: 'unacked-count'
}

Table.renderTable('table', tableOptions, function (tr, item, firstRender) {
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
