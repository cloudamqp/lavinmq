function queue(name, opts = {}) {
  const defaults = {
    name: name,
    vhost: '/',
    durable: true,
    auto_delete: false,
    exclusive: false,
    arguments: [],
    policy: '',
    consumers: 0,
    state: 'running',
    ready: 0,
    unacked: 0,
    mesages: 0,
    message_stats: {
      publish_details: { rate: 0 },
      deliver_details: { rate: 0 },
      redeliver_details: { rate: 0 },
      ack_details: { rate: 0 },
    }
  }
  return Object.assign({}, defaults, opts)
}

function response(queues = [], opts = {}) {
  const defaults = {
    page: 1,
    page_size: queues.length,
    page_count: queues.size > 0 ? 1 : 0,
    filtered_count: queues.length,
    total_count: queues.length,
    item_count: queues.length,
    items: queues
  }
  return Object.assign({}, defaults, opts)
}

export { queue, response }
