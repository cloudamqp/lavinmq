import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as HTTP from './http.js'
import * as Chart from './chart.js'
import { DataSource, type PaginatedResponse } from './datasource.js'

interface QueueDetails {
  name: string
}

interface ConsumerItem {
  consumer_tag: string
  queue: QueueDetails
  ack_required: boolean
  exclusive: boolean
  prefetch_count: number
}

interface ConnectionDetails {
  name: string
}

interface ChannelResponse {
  vhost: string
  state: string
  user: string
  messages_unacknowledged: number
  consumer_details: ConsumerItem[]
  connection_details: ConnectionDetails
  prefetch_count: number
  global_prefetch_count: number
  confirm: boolean
  message_stats: Record<string, number>
}

interface RequestResult {
  is_error?: boolean
}

const channel = new URLSearchParams(window.location.hash.substring(1)).get('name') ?? ''
const channelUrl = HTTP.url`api/channels/${channel}`
const chart = Chart.render('chart', 'msgs/s')
let vhost: string | null = null
document.title = channel + ' | LavinMQ'

class ConsumersDataSource extends DataSource<ConsumerItem> {
  constructor() {
    super({ autoReloadTimeout: 0, useQueryState: false })
  }
  setConsumers(consumers: ConsumerItem[]): void {
    this.items = consumers
  }
  override reload(): Promise<PaginatedResponse<ConsumerItem> | ConsumerItem[] | void> {
    return Promise.resolve()
  }
}

const consumersDataSource = new ConsumersDataSource()

const consumerTableOpts = {
  dataSource: consumersDataSource,
  keyColumns: ['consumer_tag'],
  countId: 'consumer-count',
}
Table.renderTable<ConsumerItem>('table', consumerTableOpts, function (tr, item) {
  Table.renderCell(tr, 0, item.consumer_tag)
  const queueLink = document.createElement('a')
  queueLink.href = HTTP.url`queue#vhost=${vhost ?? ''}&name=${item.queue.name}`
  queueLink.textContent = item.queue.name
  const ack = item.ack_required ? '\u25CF' : '\u25CB'
  const exclusive = item.exclusive ? '\u25CF' : '\u25CB'
  Table.renderCell(tr, 1, queueLink)
  Table.renderCell(tr, 2, ack, 'center')
  Table.renderCell(tr, 3, exclusive, 'center')
  Table.renderCell(tr, 4, Helpers.formatNumber(item.prefetch_count), 'right')
})

interface PrefetchForm {
  form: HTMLFormElement
  updateForm: (value: number) => void
}

interface PrefetchView {
  text: HTMLDivElement
  updateView: (value: number) => void
}

const prefetchForm = (cb: (newValue: number) => void): PrefetchForm => {
  const input = document.createElement('input')
  input.type = 'number'

  const save = DOM.button.submit()
  const reset = DOM.button.reset()
  const form = document.createElement('form')
  form.classList.add('prefetch-form')
  form.addEventListener('submit', (event) => {
    event.preventDefault()
    const prefetch = parseInt(input.value, 10)
    HTTP.request<RequestResult>('PUT', channelUrl, { body: { prefetch } }).then((r) => {
      if (!(r && r.is_error)) {
        cb(prefetch)
      }
    })
  })
  form.append(input, save, reset)
  const updateForm = (value: number): void => {
    input.value = String(value)
  }
  return { form, updateForm }
}
const prefetchView = (cb: () => void): PrefetchView => {
  const text = document.createElement('div')
  text.title = 'Click to edit'
  text.style.cursor = 'pointer'
  text.addEventListener('click', cb)
  const updateView = (value: number): void => {
    text.textContent = Helpers.formatNumber(value)
  }
  return { text, updateView }
}

interface PrefetchHandler {
  el: HTMLDivElement
  update: (value: number) => void
}

const prefetchHandler = (): PrefetchHandler => {
  const el = document.createElement('div')
  let editing = false
  let form: HTMLFormElement
  let text: HTMLDivElement
  let updateForm: (value: number) => void
  let updateView: (value: number) => void

  const rerender = (editMode: boolean): void => {
    editing = editMode
    if (editMode) {
      el.replaceChild(form, text)
    } else {
      el.replaceChild(text, form)
    }
  }
  const formResult = prefetchForm((newValue) => {
    updateView(newValue)
    rerender(false)
  })
  form = formResult.form
  updateForm = formResult.updateForm

  const viewResult = prefetchView(() => rerender(true))
  text = viewResult.text
  updateView = viewResult.updateView

  const update = (value: number): void => {
    if (!editing) updateForm(value)
    updateView(value)
  }
  el.appendChild(text)
  return { el, update }
}

const prefetch = prefetchHandler()
const chPrefetchEl = document.getElementById('ch-prefetch')
if (chPrefetchEl) chPrefetchEl.appendChild(prefetch.el)

function updateChannel(): void {
  HTTP.request<ChannelResponse>('GET', channelUrl).then((item) => {
    if (!item) return
    Chart.update(chart, item.message_stats)
    vhost = item.vhost
    const stateEl = document.getElementById('ch-state')
    if (stateEl && item.state !== stateEl.textContent) {
      stateEl.textContent = item.state
    }
    const unackedEl = document.getElementById('ch-unacked')
    if (unackedEl) unackedEl.textContent = String(item.messages_unacknowledged)
    consumersDataSource.setConsumers(item.consumer_details)
    const pagenameLabel = document.getElementById('pagename-label')
    if (pagenameLabel) pagenameLabel.textContent = `${channel} in virtual host ${item.vhost}`
    const usernameEl = document.getElementById('ch-username')
    if (usernameEl) usernameEl.textContent = item.user
    const connectionLink = document.querySelector<HTMLAnchorElement>('#ch-connection a')
    if (connectionLink) {
      connectionLink.href = HTTP.url`connection#name=${item.connection_details.name}`
      connectionLink.textContent = item.connection_details.name
    }
    prefetch.update(item.prefetch_count)
    if (item.confirm) {
      const chMode = document.getElementById('ch-mode')
      if (chMode) {
        const confirmSpan = document.createElement('span')
        confirmSpan.textContent = 'Confirm'
        confirmSpan.title = 'Confirm mode enables publisher acknowledgements for reliable message delivery'
        chMode.replaceChildren(confirmSpan)
      }
    }
    const globalPrefetchEl = document.getElementById('ch-global-prefetch')
    if (globalPrefetchEl) globalPrefetchEl.textContent = Helpers.formatNumber(item.global_prefetch_count)
  })
}
updateChannel()
setInterval(updateChannel, 5000)
