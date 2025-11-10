import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import { UrlDataSource } from './datasource.js'

Helpers.addVhostOptions('declare')
const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/queues'
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
}
const queueDataSource = new UrlDataSource(url)
const tableOptions = {
  dataSource: queueDataSource,
  keyColumns: ['vhost', 'name'],
  pagination: true,
  columnSelector: true,
  search: true
}
const performMultiAction = (el) => {
  const action = el.target.dataset.action
  const elems = document.querySelectorAll('input[data-name]:checked')
  const totalCount = elems.length
  let performed = 0
  elems.forEach(el => {
    const data = el.dataset
    let url
    switch (action) {
      case 'delete':
        url = HTTP.url`api/queues/${data.vhost}/${data.name}`
        break
      case 'purge':
        url = HTTP.url`api/queues/${data.vhost}/${data.name}/contents`
        break
    }
    if (!url) return
    HTTP.request('DELETE', url).then(() => {
      performed += 1
      if (performed === totalCount) {
        multiSelectControls.classList.add('hide')
        elems.forEach(e => { e.checked = false })
        document.getElementById('multi-check-all').checked = false
        queuesTable.reload()
      }
    }).catch(e => {
      DOM.toast(`Failed to perform action on ${data.name}`, 'error')
      queuesTable.reload()
    })
  })
}
const multiSelectControls = document.getElementById('multiselect-controls')
document.querySelectorAll('#multiselect-controls [data-action]')
  .forEach(e => e.addEventListener('click', performMultiAction))
document.querySelector('#multiselect-controls .popup-close').addEventListener('click', () => {
  toggleMultiActionControls(false, 0)
})
const toggleMultiActionControls = (show, count) => {
  const currentlyHidden = multiSelectControls.classList.contains('hide')
  if (currentlyHidden && show && count > 0) {
    multiSelectControls.classList.remove('hide')
  } else if (!currentlyHidden && !show) {
    multiSelectControls.classList.add('hide')
  }
  document.getElementById('multi-queue-count').textContent = count
}
const rowCheckboxChanged = (e) => {
  const checked = document.querySelectorAll('input[data-name]:checked')
  toggleMultiActionControls(true, checked.length)
}
document.getElementById('multi-check-all').addEventListener('change', (el) => {
  const checked = el.target.checked
  let c = 0
  document.querySelectorAll('input[data-name]').forEach((el) => {
    el.checked = checked
    c += 1
  })
  toggleMultiActionControls(checked, c)
})

const queuesTable = Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    const features = document.createElement('span')
    features.className = 'features'
    if (item.durable) {
      const durable = document.createElement('span')
      durable.textContent = 'D '
      durable.title = 'Durable'
      features.appendChild(durable)
    }
    if (item.auto_delete) {
      const autoDelete = document.createElement('span')
      autoDelete.textContent = 'AD '
      autoDelete.title = 'Auto Delete'
      features.appendChild(autoDelete)
    }
    if (item.exclusive) {
      const exclusive = document.createElement('span')
      exclusive.textContent = 'E '
      exclusive.title = 'Exclusive'
      features.appendChild(exclusive)
    }
    if (Object.keys(item.arguments).length > 0) {
      const argsTooltip = Object.entries(item.arguments)
        .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
        .join('\n')
      const argsSpan = document.createElement('span')
      argsSpan.textContent = 'Args '
      argsSpan.title = argsTooltip
      features.appendChild(argsSpan)
    }
    const queueLink = document.createElement('a')
    const qType = item.arguments['x-queue-type']
    if (qType === 'stream') {
      queueLink.href = HTTP.url`stream#vhost=${item.vhost}&name=${item.name}`
    } else {
      queueLink.href = HTTP.url`queue#vhost=${item.vhost}&name=${item.name}`
    }
    queueLink.textContent = item.name

    const checkbox = document.createElement('input')
    checkbox.type = 'checkbox'
    checkbox.setAttribute('data-vhost', item.vhost)
    checkbox.setAttribute('data-name', item.name)
    checkbox.addEventListener('change', rowCheckboxChanged)
    Table.renderCell(tr, 0, checkbox, 'checkbox')
    Table.renderCell(tr, 1, item.vhost)
    Table.renderCell(tr, 2, queueLink)
    Table.renderCell(tr, 3, features, 'center')
  }

  let policyLink = ''
  if (item.policy) {
    policyLink = document.createElement('a')
    policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
    policyLink.textContent = item.policy
  }
  Table.renderCell(tr, 4, policyLink, 'center')
  Table.renderCell(tr, 5, item.consumers, 'right')
  Table.renderCell(tr, 6, null, 'center ' + 'state-' + item.state)
  Table.renderCell(tr, 7, Helpers.formatNumber(item.messages_ready), 'right')
  Table.renderCell(tr, 8, Helpers.formatNumber(item.messages_unacknowledged), 'right')
  Table.renderCell(tr, 9, Helpers.formatNumber(item.messages), 'right')
  Table.renderCell(tr, 10, Helpers.formatNumber(item.message_stats.publish_details.rate), 'right')
  Table.renderCell(tr, 11, Helpers.formatNumber(item.message_stats.deliver_details.rate), 'right')
  Table.renderCell(tr, 12, Helpers.formatNumber(item.message_stats.redeliver_details.rate), 'right')
  Table.renderCell(tr, 13, Helpers.formatNumber(item.message_stats.ack_details.rate), 'right')
  Table.renderCell(tr, 14, Helpers.nFormatter(item.total_bytes) + 'B', 'right')
})

document.querySelector('#declare').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = data.get('vhost')
  const queue = data.get('name').trim()
  const url = HTTP.url`api/queues/${vhost}/${queue}`
  const body = {
    durable: data.get('durable') === '1',
    auto_delete: data.get('auto_delete') === '1',
    arguments: DOM.parseJSON(data.get('arguments'))
  }
  HTTP.request('PUT', url, { body })
    .then((response) => {
      if (response?.is_error) { return }
      queuesTable.reload()
      evt.target.reset()
      evt.target.querySelector('select[name="vhost"]').value = decodeURIComponent(vhost) // Keep selected vhost selected
      DOM.toast('Queue ' + queue + ' created')
    })
})
queuesTable.on('updated', _ => {
  const checked = document.querySelectorAll('input[data-name]:checked')
  document.getElementById('multi-queue-count').textContent = checked.length
})

document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelperJSON('declare', 'arguments', e)
})
