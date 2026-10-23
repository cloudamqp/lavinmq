import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'

Helpers.addVhostOptions('addExchange')

HTTP.request('GET', 'api/overview').then(function (response) {
  const exchangeTypes = response.exchange_types
  const select = document.forms.addExchange.elements.type
  exchangeTypes.forEach(item => {
    const name = (item && typeof item === 'object') ? item.name : item
    const human = (item && typeof item === 'object' && 'human' in item) ? item.human : undefined
    const opt = document.createElement('option')
    opt.text = human || name
    opt.value = name
    select.add(opt)
  })
}).catch(() => {})

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/exchanges'
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
}
const tableOptions = {
  url,
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
        url = HTTP.url`api/exchanges/${data.vhost}/${data.name}`
        break
    }
    if (!url) return
    HTTP.request('DELETE', url).then(() => {
      performed += 1
      if (performed === totalCount) {
        multiSelectControls.classList.add('hide')
        elems.forEach(e => { e.checked = false })
        document.getElementById('multi-check-all').checked = false
        exchangeTable.reload()
      }
    }).catch(() => {
      DOM.toast.error(`Failed to perform action on ${data.name}`)
      exchangeTable.reload()
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
  document.getElementById('multi-exchange-count').textContent = count
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

const exchangeTable = Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    const features = document.createElement('span')
    features.className = 'features'
    if (item.durable) {
      const durable = document.createElement('span')
      durable.textContent = 'D'
      durable.title = 'Durable'
      features.appendChild(durable)
    }
    if (item.auto_delete) {
      const autoDelete = document.createElement('span')
      autoDelete.textContent = ' AD'
      autoDelete.title = 'Auto Delete'
      features.appendChild(autoDelete)
    }
    if (item.internal) {
      const internal = document.createElement('span')
      internal.textContent = ' I'
      internal.title = 'Internal'
      features.appendChild(internal)
    }
    if (item.arguments['x-delayed-exchange']) {
      const delayed = document.createElement('span')
      delayed.textContent = ' d'
      delayed.title = 'Delayed'
      features.appendChild(delayed)
    }
    const exchangeLink = document.createElement('a')
    exchangeLink.href = HTTP.url`exchange#vhost=${item.vhost}&name=${item.name}`
    exchangeLink.textContent = item.name

    const checkbox = document.createElement('input')
    checkbox.type = 'checkbox'
    checkbox.setAttribute('data-vhost', item.vhost)
    checkbox.setAttribute('data-name', item.name)
    checkbox.addEventListener('change', rowCheckboxChanged)
    Table.renderCell(tr, 0, checkbox, 'checkbox')
    Table.renderCell(tr, 1, item.vhost)
    Table.renderCell(tr, 2, exchangeLink)
    Table.renderCell(tr, 3, item.type)
    Table.renderCell(tr, 4, features, 'center')
  }
  let policyLink = ''
  if (item.policy) {
    policyLink = document.createElement('a')
    policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
    policyLink.textContent = item.policy
  }
  Table.renderCell(tr, 5, policyLink, 'center')
  Table.renderCell(tr, 6, item.message_stats.publish_in_details.rate, 'center')
  Table.renderCell(tr, 7, item.message_stats.publish_out_details.rate, 'center')
})

exchangeTable.on('updated', _ => {
  const checked = document.querySelectorAll('input[data-name]:checked')
  document.getElementById('multi-exchange-count').textContent = checked.length
})

document.querySelector('#addExchange').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = data.get('vhost')
  const exchange = data.get('name').trim()
  const url = HTTP.url`api/exchanges/${vhost}/${exchange}`
  const body = {
    durable: data.get('durable') === '1',
    auto_delete: data.get('auto_delete') === '1',
    internal: data.get('internal') === '1',
    delayed: data.get('delayed') === '1',
    type: data.get('type'),
    arguments: DOM.parseJSON(data.get('arguments'))
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      exchangeTable.reload()
      DOM.toast('Exchange ' + exchange + ' created')
      evt.target.reset()
    })
    .catch(() => {})
})
