import * as Table from './table.js'
import { UrlDataSource } from './datasource.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const queue = search.get('name')
const vhost = search.get('vhost')
const url = `api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queue)}/unacked`
const channel_base_url = 'api/channel#name='
document.getElementById('queue-link').href = `/queue#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(queue)}`
document.getElementById('queue-link').innerHTML = queue
document.querySelector('#pagename-label').textContent = queue + ' in virtual host ' + vhost

const tableOptions = {
  dataSource: new UrlDataSource(url, { useQueryState: false }),
  keyColumns: ['message_tag', 'consumer_tag', 'channel_name', 'unacked_for_seconds'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: false,
  countId: 'unacked-count'
}

Table.renderTable('table', tableOptions, function (tr, item, firstRender) {
  if (!firstRender) return
  Table.renderCell(tr, 0, item.message_tag)
  Table.renderCell(tr, 1, item.consumer_tag)
  const channel = document.createElement('a')
  channel.href = channel_base_url + item.channel_name
  channel.textContent = item.channel_name
  Table.renderCell(tr, 2, channel)
  Table.renderCell(tr, 3, item.unacked_for_seconds)
})
