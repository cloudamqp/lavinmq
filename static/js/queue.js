(function(lavinmq) {
  const queue = new URLSearchParams(window.location.search).get('name')
  const vhost = new URLSearchParams(window.location.search).get('vhost')
  const urlEncodedQueue = encodeURIComponent(queue)
  const urlEncodedVhost = encodeURIComponent(vhost)
  const escapeHTML = lavinmq.dom.escapeHTML
  const pauseQueueForm = document.querySelector('#pauseQueue')
  const resumeQueueForm = document.querySelector('#resumeQueue')
  const messageSnapshotForm = document.querySelector('#messageSnapshot')
  document.title = queue + ' | LavinMQ'
  let consumerListLength = 20
  const consumersTable = lavinmq.table.renderTable('table', { keyColumns: [] }, function (tr, item) {
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
      lavinmq.http.request('DELETE', actionPath)
        .then(() => {
          lavinmq.dom.toast(`Consumer canceled ${item.consumer_tag}`)
          updateQueue(false)
        }).catch(lavinmq.http.standardErrorHandler).catch(e => clearInterval(qTimer))
    })
    lavinmq.table.renderCell(tr, 0, channelLink)
    lavinmq.table.renderCell(tr, 1, item.consumer_tag)
    lavinmq.table.renderCell(tr, 2, ack, 'center')
    lavinmq.table.renderCell(tr, 3, exclusive, 'center')
    lavinmq.table.renderCell(tr, 4, item.prefetch_count, 'right')
    lavinmq.table.renderCell(tr, 5, cancelForm, 'center')
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

  const chart = lavinmq.chart.render('chart', 'msgs/s')
  const queueUrl = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue
  function updateQueue (all) {
    lavinmq.http.request('GET', queueUrl + '?consumer_list_length=' + consumerListLength)
      .then(item => {
        lavinmq.chart.update(chart, item.message_stats)
        handleQueueState(item.state)
        document.getElementById('q-unacked').textContent = item.unacked
        document.getElementById('q-unacked-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_bytes) + 'B'
        document.getElementById('q-unacked-avg-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_avg_bytes) + 'B'
        document.getElementById('q-total').textContent = lavinmq.helpers.formatNumber(item.messages)
        document.getElementById('q-total-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_bytes + item.ready_bytes) + 'B'
        document.getElementById('q-total-avg-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_avg_bytes + item.ready_avg_bytes) + 'B'
        document.getElementById('q-ready').textContent = lavinmq.helpers.formatNumber(item.ready)
        document.getElementById('q-ready-bytes').textContent = lavinmq.helpers.nFormatter(item.ready_bytes) + 'B'
        document.getElementById('q-ready-avg-bytes').textContent = lavinmq.helpers.nFormatter(item.ready_avg_bytes) + 'B'
        document.getElementById('q-consumers').textContent = lavinmq.helpers.formatNumber(item.consumers)
        if (item.first_message_timestamp !== undefined && item.first_message_timestamp !== 0) {
          document.getElementById('q-first-timestamp').textContent = lavinmq.helpers.formatTimestamp(item.first_message_timestamp)
          document.getElementById('q-last-timestamp').textContent = lavinmq.helpers.formatTimestamp(item.last_message_timestamp)
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
            lavinmq.dom.setChild('#q-policy', policyLink)
          }
          const qArgs = document.getElementById('q-arguments')
          let args = ''
          for (const arg in item.arguments) {
            args += `<div>${arg}: ${item.arguments[arg]}</div>`
          }
          qArgs.innerHTML = args
        }
      }).catch(lavinmq.http.standardErrorHandler).catch(e => clearInterval(qTimer))
  }
  updateQueue(true)
  const qTimer = setInterval(updateQueue, 5000)

  const tableOptions = { url: queueUrl + '/bindings', keyColumns: ['properties_key'], interval: 5000 }
  const bindingsTable = lavinmq.table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
    if (!all) return
    if (item.source === '') {
      const td = lavinmq.table.renderCell(tr, 0, '(Default exchange binding)')
      td.setAttribute('colspan', 4)
    } else {
      const btn = document.createElement('button')
      btn.classList.add('btn-secondary')
      btn.innerHTML = 'Unbind'
      const e = encodeURIComponent(item.source)
      btn.onclick = function () {
        const p = encodeURIComponent(item.properties_key)
        const url = '/api/bindings/' + urlEncodedVhost + '/e/' + e + '/q/' + urlEncodedQueue + '/' + p
        lavinmq.http.request('DELETE', url)
          .then(() => { lavinmq.dom.removeNodes(tr) })
          .catch(lavinmq.http.standardErrorHandler)
      }
      const exchangeLink = `<a href="/exchange?vhost=${urlEncodedVhost}&name=${escapeHTML(e)}">${escapeHTML(item.source)}</a>`
      lavinmq.table.renderHtmlCell(tr, 0, exchangeLink)
      lavinmq.table.renderCell(tr, 1, item.routing_key)
      lavinmq.table.renderHtmlCell(tr, 2, '<pre>' + JSON.stringify(item.arguments || {}) + '</pre>')
      lavinmq.table.renderCell(tr, 3, btn, 'right')
    }
  })

  document.querySelector('#addBinding').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const e = encodeURIComponent(data.get('source').trim())
    const url = '/api/bindings/' + urlEncodedVhost + '/e/' + e + '/q/' + urlEncodedQueue
    const args = lavinmq.dom.parseJSON(data.get('arguments'))
    const body = {
      routing_key: data.get('routing_key').trim(),
      arguments: args
    }
    lavinmq.http.request('POST', url, { body })
      .then(() => {
        bindingsTable.fetchAndUpdate()
        evt.target.reset()
        lavinmq.dom.toast('Exchange ' + e + ' bound to queue')
      }).catch(lavinmq.http.alertErrorHandler)
  })

  function addProperty (key, value) { // eslint-disable-line no-unused-vars
    const el = document.querySelector('#publishMessage textarea[name=properties]')
    const properties = lavinmq.dom.parseJSON(el.value || '{}')
    properties[key] = value
    el.value = JSON.stringify(properties)
  }

  document.querySelector('#publishMessage').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const url = '/api/exchanges/' + urlEncodedVhost + '/amq.default/publish'
    const properties = lavinmq.dom.parseJSON(data.get('properties'))
    properties.delivery_mode = parseInt(data.get('delivery_mode'))
    properties.headers = lavinmq.dom.parseJSON(data.get('headers'))
    const body = {
      payload: data.get('payload'),
      payload_encoding: 'string',
      routing_key: queue,
      properties
    }
    lavinmq.http.request('POST', url, { body })
      .then(() => {
        lavinmq.dom.toast('Published message to ' + queue)
        updateQueue(false)
      }).catch(lavinmq.http.alertErrorHandler)
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
    lavinmq.http.request('POST', url, { body })
      .then(messages => {
        if (messages.length === 0) {
          window.alert('No messages in queue')
          return
        }
        updateQueue(false)
        const messagesContainer = document.getElementById('messages')
        lavinmq.dom.removeChildren(messagesContainer)
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
      }).catch(lavinmq.http.alertErrorHandler)
  })

  document.querySelector('#moveMessages').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const username = lavinmq.auth.getUsername()
    const password = lavinmq.auth.getPassword()
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
    lavinmq.http.request('PUT', url, { body })
      .then(() => {
        evt.target.reset()
        lavinmq.dom.toast('Moving messages to ' + dest)
      }).catch(lavinmq.http.standardErrorHandler)
  })

  document.querySelector('#purgeQueue').addEventListener('submit', function (evt) {
    evt.preventDefault()
    let params = ""
    let countElem = evt.target.querySelector("input[name='count']")
    if(countElem && countElem.value) {
      params = `?count=${countElem.value}`
    }
    const url = `/api/queues/${urlEncodedVhost}/${urlEncodedQueue}/contents${params}`
    if (window.confirm('Are you sure? Messages cannot be recovered after purging.')) {
      lavinmq.http.request('DELETE', url)
        .then(() => { lavinmq.dom.toast('Queue purged!') })
        .catch(lavinmq.http.standardErrorHandler)
    }
  })

  document.querySelector('#deleteQueue').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue
    if (window.confirm('Are you sure? The queue is going to be deleted. Messages cannot be recovered after deletion.')) {
      lavinmq.http.request('DELETE', url)
        .then(() => { window.location = '/queues' })
        .catch(lavinmq.http.standardErrorHandler)
    }
  })

  pauseQueueForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/pause'
    if (window.confirm('Are you sure? This will suspend deliveries to all consumers.')) {
      lavinmq.http.request('PUT', url)
        .then(() => {
          lavinmq.dom.toast('Queue paused!')
          handleQueueState('paused')
        })
        .catch(lavinmq.http.standardErrorHandler)
    }
  })

  resumeQueueForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/resume'
    if (window.confirm('Are you sure? This will resume deliveries to all consumers.')) {
      lavinmq.http.request('PUT', url)
        .then(() => {
          lavinmq.dom.toast('Queue resumed!')
          handleQueueState('running')
        })
        .catch(lavinmq.http.standardErrorHandler)
    }
  })

  messageSnapshotForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = '/api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/size-snapshot'
    if (window.confirm('Are you sure? This will take a snapshot of queue message sizes.')) {
      lavinmq.http.request('GET', url)
        .then(item => {
          lavinmq.dom.toast('Queue size snapshot')
          handleQueueState('running')
          document.getElementById('ms-q-unacked').textContent = item.unacked
          document.getElementById('ms-q-unacked-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_bytes) + 'B'
          document.getElementById('ms-q-unacked-avg-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_avg_bytes) + 'B'
          document.getElementById('ms-q-unacked-min-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_min_bytes) + 'B'
          document.getElementById('ms-q-unacked-max-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_max_bytes) + 'B'
          document.getElementById('ms-q-total').textContent = lavinmq.helpers.formatNumber(item.messages)
          document.getElementById('ms-q-total-bytes').textContent = lavinmq.helpers.nFormatter(item.unacked_bytes + item.ready_bytes) + 'B'
          document.getElementById('ms-q-ready').textContent = lavinmq.helpers.formatNumber(item.ready)
          document.getElementById('ms-q-ready-bytes').textContent = lavinmq.helpers.nFormatter(item.ready_bytes) + 'B'
          document.getElementById('ms-q-ready-avg-bytes').textContent = lavinmq.helpers.nFormatter(item.ready_avg_bytes) + 'B'
          document.getElementById('ms-q-ready-min-bytes').textContent = lavinmq.helpers.nFormatter(item.ready_min_bytes) + 'B'
          document.getElementById('ms-q-ready-max-bytes').textContent = lavinmq.helpers.nFormatter(item.ready_max_bytes) + 'B'
          if (item.first_message_timestamp !== undefined && item.first_message_timestamp !== 0) {
            document.getElementById('ms-q-first-timestamp').textContent = lavinmq.helpers.formatTimestamp(item.first_message_timestamp)
            document.getElementById('ms-q-last-timestamp').textContent = lavinmq.helpers.formatTimestamp(item.last_message_timestamp)
          } else {
            document.getElementById('q-first-timestamp').textContent = " - "
            document.getElementById('q-last-timestamp').textContent = " - "
          }
          document.getElementById('ms-date-time').textContent = lavinmq.helpers.formatTimestamp(new Date())
        })
        .catch(lavinmq.http.standardErrorHandler)
    }
  })

  lavinmq.helpers.autoCompleteDatalist('exchange-list', 'exchanges')
  lavinmq.helpers.autoCompleteDatalist('queue-list', 'queues')
}(window.lavinmq))
