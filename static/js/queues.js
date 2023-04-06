import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Dom from './dom.js'
import * as Table from './table.js'
import { UrlDataSource } from './datasource.js'

Helpers.addVhostOptions('declare')
const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/queues'
if (vhost && vhost !== '_all') {
  url += '/' + encodeURIComponent(vhost)
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
  const elems = document.querySelectorAll("input[data-name]:checked")
  const totalCount = elems.length
  let performed = 0
  elems.forEach(el => {
    const data = el.dataset;
    let url;
    switch(action) {
      case "delete":
        url = `api/queues/${data.vhost}/${data.name}`
        break
      case "purge":
        url = `api/queues/${data.vhost}/${data.name}/contents`
        break
    }
    if(!url) return;
    HTTP.request('DELETE', url).then(() => {
      performed += 1
      if(performed == totalCount) {
        multiSelectControls.classList.add("hide")
        elems.forEach(e => e.checked = false)
        document.getElementById("multi-check-all").checked = false
        queuesTable.reload()
      }
    }).catch(e => {
      Dom.toast(`Failed to perform action on ${data.name}`, "error")
      queuesTable.reload()
    })
  })
}
const multiSelectControls = document.getElementById("multiselect-controls")
document.querySelectorAll("#multiselect-controls [data-action]")
  .forEach(e => e.addEventListener("click", performMultiAction))
document.querySelector("#multiselect-controls .popup-close").addEventListener("click", () => {
  toggleMultiActionControls(false, 0)
})
const toggleMultiActionControls = (show, count) => {
  multiSelectControls.classList.toggle("hide", !(show && count > 0))
  document.getElementById("multi-queue-count").textContent = count;
}
const rowCheckboxChanged = (e) => {
  const checked = document.querySelectorAll("input[data-name]:checked")
  toggleMultiActionControls(true, checked.length)
}
document.getElementById("multi-check-all").addEventListener("change", (el) => {
  const checked = el.target.checked;
  let c = 0;
  document.querySelectorAll("input[data-name]").forEach((el) => {
    el.checked = checked;
    c += 1
  })
  toggleMultiActionControls(checked, c)
})

const queuesTable = Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    let features = ''
    features += item.durable ? ' D' : ''
    features += item.auto_delete ? ' AD' : ''
    features += item.exclusive ? ' E' : ''
    features += Object.keys(item.arguments).length > 0  ? ' Args ' : ''
    const queueLink = document.createElement('a')
    queueLink.href = 'queue#vhost=' + encodeURIComponent(item.vhost) + '&name=' + encodeURIComponent(item.name)
    queueLink.textContent = item.name

    const checkbox = document.createElement('input')
    checkbox.type='checkbox'
    checkbox.setAttribute('data-vhost', encodeURIComponent(item.vhost))
    checkbox.setAttribute('data-name', encodeURIComponent(item.name))
    checkbox.addEventListener('change', rowCheckboxChanged)
    Table.renderCell(tr, 0, checkbox)
    Table.renderCell(tr, 1, item.vhost)
    Table.renderCell(tr, 2, queueLink)
    Table.renderCell(tr, 3, features, 'center')
  }

  let policyLink = ''
  if (item.policy) {
    policyLink = document.createElement('a')
    policyLink.href = 'policies#name=' + encodeURIComponent(item.policy) + '&vhost=' + encodeURIComponent(item.vhost)
    policyLink.textContent = item.policy
  }
  Table.renderCell(tr, 4, policyLink, 'center')
  Table.renderCell(tr, 5, item.consumers, 'right')
  Table.renderCell(tr, 6, null, 'center ' + 'state-' + item.state)
  Table.renderCell(tr, 7, Helpers.formatNumber(item.ready), 'right')
  Table.renderCell(tr, 8, Helpers.formatNumber(item.unacked), 'right')
  Table.renderCell(tr, 9, Helpers.formatNumber(item.messages), 'right')
  Table.renderCell(tr, 10, Helpers.formatNumber(item.message_stats.publish_details.rate), 'right')
  Table.renderCell(tr, 11, Helpers.formatNumber(item.message_stats.deliver_details.rate), 'right')
  Table.renderCell(tr, 12, Helpers.formatNumber(item.message_stats.redeliver_details.rate), 'right')
  Table.renderCell(tr, 13, Helpers.formatNumber(item.message_stats.ack_details.rate), 'right')
})

document.querySelector('#declare').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = encodeURIComponent(data.get('vhost'))
  const queue = encodeURIComponent(data.get('name').trim())
  const url = 'api/queues/' + vhost + '/' + queue
  const body = {
    durable: data.get('durable') === '1',
    auto_delete: data.get('auto_delete') === '1',
    arguments: Dom.parseJSON(data.get('arguments'))
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      queuesTable.reload()
      evt.target.reset()
      Dom.toast('Queue ' + queue + ' created')
    }).catch(HTTP.standardErrorHandler)
})
queuesTable.on('updated', _ => {
  const checked = document.querySelectorAll("input[data-name]:checked")
  console.log(checked)
  toggleMultiActionControls(true, checked.length)
})

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON('declare', 'arguments', e)
}
