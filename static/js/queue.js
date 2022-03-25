(function(avalanchemq) {
  const queue = new URLSearchParams(window.location.search).get('name')
  const vhost = new URLSearchParams(window.location.search).get('vhost')
  const urlEncodedQueue = encodeURIComponent(queue)
  const urlEncodedVhost = encodeURIComponent(vhost)
  const escapeHTML = avalanchemq.dom.escapeHTML
  const pauseQueueForm = document.querySelector('#pauseQueue')
  const resumeQueueForm = document.querySelector('#resumeQueue')
  document.title = queue + ' | AvalancheMQ'
  let consumerListLength = 20
  const consumersTable = avalanchemq.table.renderTable('table', { keyColumns: [] }, function (tr, item) {
    const channelLink = document.createElement('a')
    channelLink.href = '/channel?name=' + encodeURIComponent(item.channel_details.name)
    channelLink.textContent = item.channel_details.name
    const ack = item.ack_required ? '●' : '○'
    const exclusive = item.exclusive ? '●' : '○'
    const cancelForm = document.createElement('form')
    const btn = document.createElement('button')
    btn.classList.add('btn-primary')
    btn.type = 'submit'
    btn.innerText = 'Cancel'
    cancelForm.appendChild(btn)
    const urlEncodedConsumerTag = encodeURIComponent(item.consumer_tag)
    const conn = encodeURIComponent(item.channel_details.connection_name)
    const ch = encodeURIComponent(item.channel_details.number)
    const actionPath = `/api/consumers/${urlEncodedVhost}/${conn}/${ch}/${urlEncodedConsumerTag}`
    cancelForm.addEventListener('submit', function (evt) {
      evt.preventDefault()
      avalanchemq.http.request('DELETE', actionPath)
        .then(() => {
          avalanchemq.dom.toast(`Consumer canceled ${item.consumer_tag}`)
          updateQueue(false)
        }).catch(avalanchemq.http.standardErrorHandler).catch(e => clearInterval(qTimer))
    })
    avalanchemq.table.renderCell(tr, 0, channelLink)
    avalanchemq.table.renderCell(tr, 1, item.consumer_tag)
    avalanchemq.table.renderCell(tr, 2, ack, 'center')
    avalanchemq.table.renderCell(tr, 3, exclusive, 'center')
    avalanchemq.table.renderCell(tr, 4, item.prefetch_count, 'right')
    avalanchemq.table.renderCell(tr, 5, cancelForm, 'center')
  })

  const loadMoreConsumersBtn = document.getElementById("load-more-consumers");
  loadMoreConsumersBtn.addEventListener('click', (e) => {
    consumerListLength += 10;
    updateQueue(true)
  });

  function handleQueueState (state) {
    document.getElementById('q-state').textContent = state
    switch (state) {
    case 'paused':
      pauseQueueForm.classList.add('hide')
      resumeQueueForm.classList.remove('hide')
      break
    case 'running':
      pauseQueueForm.classList.remove('hide')
      resumeQueueForm.classList.add('hide')
      break
    default:
      pauseQueueForm.disabled = true
      resumeQueueForm.disabled = true
    }
  }

  const chart = avalanchemq.chart.render('chart', 'msgs/s')
  const queueUrl = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue
  function updateQueue (all) {
    avalanchemq.http.request('GET', queueUrl + '?consumer_list_length=' + consumerListLength)
      .then(item => {
        avalanchemq.chart.update(chart, item.message_stats)
        handleQueueState(item.state)
        document.getElementById('q-unacked').textContent = item.unacked
        document.getElementById('q-unacked-bytes').textContent = avalanchemq.helpers.nFormatter(item.unacked_bytes) + 'B'
        document.getElementById('q-total').textContent = avalanchemq.helpers.formatNumber(item.messages)
        document.getElementById('q-total-bytes').textContent = avalanchemq.helpers.nFormatter(item.unacked_bytes + item.ready_bytes) + 'B'
        document.getElementById('q-ready').textContent = avalanchemq.helpers.formatNumber(item.ready)
        document.getElementById('q-ready-bytes').textContent = avalanchemq.helpers.nFormatter(item.ready_bytes) + 'B'
        document.getElementById('q-consumers').textContent = avalanchemq.helpers.formatNumber(item.consumers)
        if (item.first_message_timestamp !== undefined && item.first_message_timestamp !== 0) {
          document.getElementById('q-first-timestamp').textContent = avalanchemq.helpers.formatTimestamp(item.first_message_timestamp)
          document.getElementById('q-last-timestamp').textContent = avalanchemq.helpers.formatTimestamp(item.last_message_timestamp)
        } else {
          document.getElementById('q-first-timestamp').textContent = " - "
          document.getElementById('q-last-timestamp').textContent = " - "
        }
        item.consumer_details.filtered_count = item.consumers
        consumersTable.updateTable(item.consumer_details)
        const hasMoreConsumers = item.consumer_details.length < item.consumers
        loadMoreConsumersBtn.classList.toggle("visible", hasMoreConsumers)
        if(hasMoreConsumers) {
          loadMoreConsumersBtn.textContent = `Showing ${item.consumer_details.length} of total ${item.consumers} consumers, click to load more`
        }
        if (all) {
          let features = ''
          features += item.durable ? ' D' : ''
          features += item.auto_delete ? ' AD' : ''
          features += item.exclusive ? ' E' : ''
          document.getElementById('q-features').textContent = features
          document.querySelector('#queue').textContent = queue + ' in virtual host ' + item.vhost
          document.querySelector('.queue').textContent = queue
          if (item.policy) {
            const policyLink = document.createElement('a')
            policyLink.href = '/policies?name=' + encodeURIComponent(item.policy) + '&vhost=' + encodeURIComponent(item.vhost)
            policyLink.textContent = item.policy
            avalanchemq.dom.setChild('#q-policy', policyLink)
          }
          const qArgs = document.getElementById('q-arguments')
          let args = ''
          for (const arg in item.arguments) {
            args += `<div>${arg}: ${item.arguments[arg]}</div>`
          }
          qArgs.innerHTML = args
        }
      }).catch(avalanchemq.http.standardErrorHandler).catch(e => clearInterval(qTimer))
  }
  updateQueue(true)
  const qTimer = setInterval(updateQueue, 5000)

  const tableOptions = { url: queueUrl + '/bindings', keyColumns: ['properties_key'], interval: 5000 }
  const bindingsTable = avalanchemq.table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
    if (!all) return
    if (item.source === '') {
      const td = avalanchemq.table.renderCell(tr, 0, '(Default exchange binding)')
      td.setAttribute('colspan', 4)
    } else {
      const btn = document.createElement('button')
      btn.classList.add('btn-secondary')
      btn.innerHTML = 'Unbind'
      const e = encodeURIComponent(item.source)
      btn.onclick = function () {
        const p = encodeURIComponent(item.properties_key)
        const url = '/api/bindings/' + urlEncodedVhost + '/e/' + e + '/q/' + urlEncodedQueue + '/' + p
        avalanchemq.http.request('DELETE', url)
          .then(() => { avalanchemq.dom.removeNodes(tr) })
          .catch(avalanchemq.http.standardErrorHandler)
      }
      const exchangeLink = `<a href="/exchange?vhost=${urlEncodedVhost}&name=${escapeHTML(e)}">${escapeHTML(item.source)}</a>`
      avalanchemq.table.renderHtmlCell(tr, 0, exchangeLink)
      avalanchemq.table.renderCell(tr, 1, item.routing_key)
      avalanchemq.table.renderHtmlCell(tr, 2, '<pre>' + JSON.stringify(item.arguments || {}) + '</pre>')
      avalanchemq.table.renderCell(tr, 3, btn, 'right')
    }
  })

  document.querySelector('#addBinding').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const e = encodeURIComponent(data.get('source').trim())
    const url = '/api/bindings/' + urlEncodedVhost + '/e/' + e + '/q/' + urlEncodedQueue
    const args = avalanchemq.dom.parseJSON(data.get('arguments'))
    const body = {
      routing_key: data.get('routing_key').trim(),
      arguments: args
    }
    avalanchemq.http.request('POST', url, { body })
      .then(() => {
        bindingsTable.fetchAndUpdate()
        evt.target.reset()
        avalanchemq.dom.toast('Exchange ' + e + ' bound to queue')
      }).catch(avalanchemq.http.alertErrorHandler)
  })

  function addProperty (key, value) { // eslint-disable-line no-unused-vars
    const el = document.querySelector('#publishMessage textarea[name=properties]')
    const properties = avalanchemq.dom.parseJSON(el.value || '{}')
    properties[key] = value
    el.value = JSON.stringify(properties)
  }

  document.querySelector('#publishMessage').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const url = '/api/exchanges/' + urlEncodedVhost + '/amq.default/publish'
    const properties = avalanchemq.dom.parseJSON(data.get('properties'))
    properties.delivery_mode = parseInt(data.get('delivery_mode'))
    properties.headers = avalanchemq.dom.parseJSON(data.get('headers'))
    const body = {
      payload: data.get('payload'),
      payload_encoding: 'string',
      routing_key: queue,
      properties
    }
    avalanchemq.http.request('POST', url, { body })
      .then(() => {
        avalanchemq.dom.toast('Published message to ' + queue)
        updateQueue(false)
      }).catch(avalanchemq.http.alertErrorHandler)
  })

  document.querySelector('#getMessages').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/get'
    const body = {
      count: parseInt(data.get('messages')),
      ack_mode: data.get('mode'),
      encoding: data.get('encoding'),
      truncate: 50000
    }
    avalanchemq.http.request('POST', url, { body })
      .then(messages => {
        if (messages.length === 0) {
          window.alert('No messages in queue')
          return
        }
        updateQueue(false)
        const messagesContainer = document.getElementById('messages')
        avalanchemq.dom.removeChildren(messagesContainer)
        const template = document.getElementById('message-template')
        for (let i = 0; i < messages.length; i++) {
          const message = messages[i]
          const msgNode = template.cloneNode(true)
          msgNode.removeAttribute('id')
          msgNode.querySelector('.message-number').textContent = i + 1
          msgNode.querySelector('.messages-remaining').textContent = message.message_count
          const exchange = message.exchange === '' ? '(AMQP default)' : message.exchange
          msgNode.querySelector('.message-exchange').textContent = exchange
          msgNode.querySelector('.message-routing-key').textContent = message.routing_key
          msgNode.querySelector('.message-redelivered').textContent = message.redelivered
          msgNode.querySelector('.message-properties').textContent = JSON.stringify(message.properties)
          msgNode.querySelector('.message-size').textContent = message.payload_bytes
          msgNode.querySelector('.message-encoding').textContent = message.payload_encoding
          msgNode.querySelector('.message-payload').textContent = message.payload
          msgNode.classList.remove('hide')
          messagesContainer.appendChild(msgNode)
        }
      }).catch(avalanchemq.http.alertErrorHandler)
  })

  document.querySelector('#moveMessages').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const username = avalanchemq.auth.getUsername()
    const password = avalanchemq.auth.getPassword()
    const uri = 'amqp://' + encodeURIComponent(username) + ':' + encodeURIComponent(password) + '@localhost/' + urlEncodedVhost
    const dest = document.querySelector('[name=shovel-destination]').value.trim()
    const name = 'Move ' + queue + ' to ' + dest
    const url = '/api/parameters/shovel/' + urlEncodedVhost + '/' + encodeURIComponent(name)
    const body = {
      name: name,
      value: {
        'src-uri': uri,
        'src-queue': queue,
        'dest-uri': uri,
        'dest-queue': dest,
        'src-prefetch-count': 1000,
        'ack-mode': 'on-confirm',
        'src-delete-after': 'queue-length'
      }
    }
    avalanchemq.http.request('PUT', url, { body })
      .then(() => {
        evt.target.reset()
        avalanchemq.dom.toast('Moving messages to ' + dest)
      }).catch(avalanchemq.http.standardErrorHandler)
  })

  document.querySelector('#purgeQueue').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/contents'
    if (window.confirm('Are you sure? Messages cannot be recovered after purging.')) {
      avalanchemq.http.request('DELETE', url)
        .then(() => { avalanchemq.dom.toast('Queue purged!') })
        .catch(avalanchemq.http.standardErrorHandler)
    }
  })

  document.querySelector('#deleteQueue').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue
    if (window.confirm('Are you sure? The queue is going to be deleted. Messages cannot be recovered after deletion.')) {
      avalanchemq.http.request('DELETE', url)
        .then(() => { window.location = '/queues' })
        .catch(avalanchemq.http.standardErrorHandler)
    }
  })

  pauseQueueForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/pause'
    if (window.confirm('Are you sure? This will suspend deliveries to all consumers.')) {
      avalanchemq.http.request('PUT', url)
        .then(() => {
          avalanchemq.dom.toast('Queue paused!')
          handleQueueState('paused')
        })
        .catch(avalanchemq.http.standardErrorHandler)
    }
  })

  resumeQueueForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/resume'
    if (window.confirm('Are you sure? This will resume deliveries to all consumers.')) {
      avalanchemq.http.request('PUT', url)
        .then(() => {
          avalanchemq.dom.toast('Queue resumed!')
          handleQueueState('running')
        })
        .catch(avalanchemq.http.standardErrorHandler)
    }
  })

  avalanchemq.helpers.autoCompleteDatalist('exchange-list', 'exchanges')
  avalanchemq.helpers.autoCompleteDatalist('queue-list', 'queues')
}(window.avalanchemq))
