import * as HTTP from './http.js';
import * as Helpers from './helpers.js';
import * as DOM from './dom.js';
import * as Table from './table.js';
import * as Chart from './chart.js';
import * as Auth from './auth.js';
import { UrlDataSource, DataSource } from './datasource.js';
const search = new URLSearchParams(window.location.hash.substring(1));
const queue = search.get('name') ?? '';
const vhost = search.get('vhost') ?? '';
const pauseQueueForm = document.querySelector('#pauseQueue');
const resumeQueueForm = document.querySelector('#resumeQueue');
const restartQueueForm = document.querySelector('#restartQueue');
document.title = queue + ' | LavinMQ';
let consumerListLength = 20;
class ConsumersDataSource extends DataSource {
    constructor() {
        super({ autoReloadTimeout: 0, useQueryState: false });
    }
    setConsumers(consumers) {
        this.items = consumers;
    }
    reload() {
        return Promise.resolve();
    }
}
const consumersDataSource = new ConsumersDataSource();
const consumersTableOpts = {
    keyColumns: ['consumer_tag', 'channel_details'],
    countId: 'consumer-count',
    dataSource: consumersDataSource,
};
Table.renderTable('table', consumersTableOpts, function (tr, item) {
    const channelLink = document.createElement('a');
    channelLink.href = HTTP.url `channel#name=${item.channel_details.name}`;
    channelLink.textContent = item.channel_details.name;
    const ack = item.ack_required ? '●' : '○';
    const exclusive = item.exclusive ? '●' : '○';
    const cancelForm = document.createElement('form');
    const btn = DOM.button.delete({ text: 'Cancel', type: 'submit' });
    cancelForm.appendChild(btn);
    const conn = item.channel_details.connection_name;
    const ch = item.channel_details.number;
    const actionPath = HTTP.url `api/consumers/${vhost}/${conn}/${String(ch)}/${item.consumer_tag}`;
    cancelForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        if (!window.confirm('Are you sure?'))
            return;
        HTTP.request('DELETE', actionPath).then(() => {
            DOM.toast('Consumer cancelled');
            updateQueue(false);
        });
    });
    Table.renderCell(tr, 0, channelLink);
    Table.renderCell(tr, 1, item.consumer_tag);
    Table.renderCell(tr, 2, ack, 'center');
    Table.renderCell(tr, 3, exclusive, 'center');
    Table.renderCell(tr, 4, item.prefetch_count, 'right');
    Table.renderCell(tr, 5, cancelForm, 'right');
});
const loadMoreConsumersBtn = document.getElementById('load-more-consumers');
if (loadMoreConsumersBtn) {
    loadMoreConsumersBtn.addEventListener('click', () => {
        consumerListLength += 10;
        updateQueue(true);
    });
}
function handleQueueState(state) {
    const stateEl = document.getElementById('q-state');
    if (stateEl)
        stateEl.textContent = state;
    switch (state) {
        case 'paused':
            restartQueueForm?.classList.add('hide');
            pauseQueueForm?.classList.add('hide');
            resumeQueueForm?.classList.remove('hide');
            break;
        case 'running':
            restartQueueForm?.classList.add('hide');
            pauseQueueForm?.classList.remove('hide');
            resumeQueueForm?.classList.add('hide');
            break;
        case 'closed':
            restartQueueForm?.classList.remove('hide');
            pauseQueueForm?.classList.add('hide');
            resumeQueueForm?.classList.add('hide');
            break;
        default:
            if (pauseQueueForm)
                pauseQueueForm.disabled = true;
            if (resumeQueueForm)
                resumeQueueForm.disabled = true;
    }
}
const chart = Chart.render('chart', 'msgs/s');
const queueUrl = HTTP.url `api/queues/${vhost}/${queue}`;
function updateQueue(all) {
    HTTP.request('GET', queueUrl + '?consumer_list_length=' + consumerListLength).then((item) => {
        if (!item)
            return;
        const qType = item.arguments['x-queue-type'];
        if (qType === 'stream') {
            window.location.href = `/stream#vhost=${encodeURIComponent(vhost ?? '')}&name=${encodeURIComponent(queue ?? '')}`;
        }
        Chart.update(chart, item.message_stats);
        handleQueueState(item.state);
        const unackEl = document.getElementById('q-messages-unacknowledged');
        if (unackEl)
            unackEl.textContent = String(item.messages_unacknowledged);
        const unackBytesEl = document.getElementById('q-message-bytes-unacknowledged');
        if (unackBytesEl)
            unackBytesEl.textContent = Helpers.nFormatter(item.message_bytes_unacknowledged) + 'B';
        const unackAvgEl = document.getElementById('q-unacked-avg-bytes');
        if (unackAvgEl)
            unackAvgEl.textContent = Helpers.nFormatter(item.unacked_avg_bytes) + 'B';
        const totalEl = document.getElementById('q-total');
        if (totalEl)
            totalEl.textContent = Helpers.formatNumber(item.messages);
        const totalBytesEl = document.getElementById('q-total-bytes');
        if (totalBytesEl)
            totalBytesEl.textContent = Helpers.nFormatter(item.total_bytes) + 'B';
        const totalAvgBytes = item.messages !== 0 ? (item.message_bytes_unacknowledged + item.message_bytes_ready) / item.messages : 0;
        const totalAvgBytesEl = document.getElementById('q-total-avg-bytes');
        if (totalAvgBytesEl)
            totalAvgBytesEl.textContent = Helpers.nFormatter(totalAvgBytes) + 'B';
        const readyEl = document.getElementById('q-messages-ready');
        if (readyEl)
            readyEl.textContent = Helpers.formatNumber(item.ready);
        const readyBytesEl = document.getElementById('q-message-bytes-ready');
        if (readyBytesEl)
            readyBytesEl.textContent = Helpers.nFormatter(item.ready_bytes) + 'B';
        const readyAvgEl = document.getElementById('q-ready-avg-bytes');
        if (readyAvgEl)
            readyAvgEl.textContent = Helpers.nFormatter(item.ready_avg_bytes) + 'B';
        const consumersEl = document.getElementById('q-consumers');
        if (consumersEl)
            consumersEl.textContent = Helpers.formatNumber(item.consumers);
        const unackedLink = document.getElementById('unacked-link');
        if (unackedLink)
            unackedLink.href = HTTP.url `/unacked#name=${queue}&vhost=${item.vhost}`;
        item.consumer_details.filtered_count = item.consumers;
        consumersDataSource.setConsumers(item.consumer_details);
        const hasMoreConsumers = item.consumer_details.length < item.consumers;
        loadMoreConsumersBtn?.classList.toggle('visible', hasMoreConsumers);
        if (hasMoreConsumers && loadMoreConsumersBtn) {
            loadMoreConsumersBtn.textContent = `Showing ${item.consumer_details.length} of total ${item.consumers} consumers, click to load more`;
        }
        if (all) {
            const features = [];
            if (item.durable)
                features.push('Durable');
            if (item.auto_delete)
                features.push('Auto delete');
            if (item.exclusive)
                features.push('Exclusive');
            const featuresEl = document.getElementById('q-features');
            if (featuresEl)
                featuresEl.innerText = features.join(', ');
            const pagenameLabel = document.querySelector('#pagename-label');
            if (pagenameLabel)
                pagenameLabel.textContent = queue + ' in virtual host ' + item.vhost;
            const queueLabel = document.querySelector('.queue');
            if (queueLabel)
                queueLabel.textContent = queue;
            if (item.policy) {
                const policyLink = document.createElement('a');
                policyLink.href = HTTP.url `policies#name=${item.policy}&vhost=${item.vhost}`;
                policyLink.textContent = item.policy;
                document.getElementById('q-policy')?.appendChild(policyLink);
            }
            if (item.operator_policy) {
                const policyLink = document.createElement('a');
                policyLink.href = HTTP.url `operator-policies#name=${item.operator_policy}&vhost=${item.vhost}`;
                policyLink.textContent = item.operator_policy;
                document.getElementById('q-operator-policy')?.appendChild(policyLink);
            }
            if (item.effective_policy_definition) {
                const effectivePolicyEl = document.getElementById('q-effective-policy-definition');
                if (effectivePolicyEl)
                    effectivePolicyEl.textContent = DOM.jsonToText(item.effective_policy_definition);
            }
            const qArgs = document.getElementById('q-arguments');
            if (qArgs) {
                for (const arg in item.arguments) {
                    const div = document.createElement('div');
                    div.textContent = `${arg}: ${item.arguments[arg]}`;
                    if (item.effective_arguments.includes(arg)) {
                        div.classList.add('active-argument');
                        div.title = 'Active argument';
                    }
                    else {
                        div.classList.add('inactive-argument');
                        div.title = 'Passive argument';
                    }
                    qArgs.appendChild(div);
                }
            }
        }
    });
}
updateQueue(true);
setInterval(updateQueue, 5000);
const tableOptions = {
    dataSource: new UrlDataSource(queueUrl + '/bindings', { useQueryState: false }),
    keyColumns: ['source', 'properties_key'],
    countId: 'bindings-count',
};
const bindingsTable = Table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
    if (!all)
        return;
    if (item.source === '') {
        const td = Table.renderCell(tr, 0, '(Default exchange binding)');
        td?.setAttribute('colspan', '4');
    }
    else {
        const btn = DOM.button.delete({
            text: 'Unbind',
            click: function () {
                const url = HTTP.url `api/bindings/${vhost}/e/${item.source}/q/${queue}/${item.properties_key}`;
                HTTP.request('DELETE', url).then(() => {
                    tr.parentNode?.removeChild(tr);
                });
            },
        });
        const exchangeLink = document.createElement('a');
        exchangeLink.href = HTTP.url `exchange#vhost=${vhost}&name=${item.source}`;
        exchangeLink.textContent = item.source;
        Table.renderCell(tr, 0, exchangeLink);
        Table.renderCell(tr, 1, item.routing_key);
        const pre = document.createElement('pre');
        pre.textContent = JSON.stringify(item.arguments || {});
        Table.renderCell(tr, 2, pre);
        Table.renderCell(tr, 3, btn, 'right');
    }
});
const addBindingForm = document.querySelector('#addBinding');
if (addBindingForm) {
    addBindingForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const e = data.get('source').trim();
        const url = HTTP.url `api/bindings/${vhost}/e/${e}/q/${queue}`;
        const args = DOM.parseJSON(data.get('arguments'));
        const body = {
            routing_key: data.get('routing_key').trim(),
            arguments: args,
        };
        HTTP.request('POST', url, { body })
            .then(() => {
            bindingsTable.reload();
            form.reset();
            DOM.toast('Exchange ' + e + ' bound to queue');
        })
            .catch((err) => {
            if (err.status === 404) {
                DOM.toast.error(`Exchange '${e}' does not exist and needs to be created first.`);
            }
        });
    });
}
const publishForm = document.querySelector('#publishMessage');
if (publishForm) {
    publishForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const url = HTTP.url `api/exchanges/${vhost}/amq.default/publish`;
        const properties = DOM.parseJSON(data.get('properties'));
        properties['delivery_mode'] = parseInt(data.get('delivery_mode'), 10);
        const headers = DOM.parseJSON(data.get('headers'));
        properties['headers'] = { ...properties['headers'], ...headers };
        const body = {
            payload: data.get('payload'),
            payload_encoding: data.get('payload_encoding'),
            routing_key: queue,
            properties,
        };
        HTTP.request('POST', url, { body }).then((res) => {
            if (res?.routed) {
                DOM.toast('Message published');
                updateQueue(false);
            }
            else {
                DOM.toast.warn('Message not published');
            }
        });
    });
}
const getMessagesForm = document.querySelector('#getMessages');
if (getMessagesForm) {
    getMessagesForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const url = HTTP.url `api/queues/${vhost}/${queue}/get`;
        const body = {
            count: parseInt(data.get('messages'), 10),
            ack_mode: data.get('mode'),
            encoding: data.get('encoding'),
            truncate: 50000,
        };
        HTTP.request('POST', url, { body }).then((messages) => {
            if (!messages || messages.length === 0) {
                window.alert('No messages in queue');
                return;
            }
            updateQueue(false);
            const messagesContainer = document.getElementById('messages');
            if (!messagesContainer)
                return;
            messagesContainer.textContent = '';
            const template = document.getElementById('message-template');
            if (!template)
                return;
            for (const message of messages) {
                const msgNode = template.cloneNode(true);
                msgNode.removeAttribute('id');
                const numberEl = msgNode.querySelector('.message-number');
                if (numberEl)
                    numberEl.textContent = String(messages.indexOf(message) + 1);
                const remainingEl = msgNode.querySelector('.messages-remaining');
                if (remainingEl)
                    remainingEl.textContent = String(message.message_count);
                const exchange = message.exchange === '' ? '(AMQP default)' : message.exchange;
                const exchangeEl = msgNode.querySelector('.message-exchange');
                if (exchangeEl)
                    exchangeEl.textContent = exchange;
                const routingKeyEl = msgNode.querySelector('.message-routing-key');
                if (routingKeyEl)
                    routingKeyEl.textContent = message.routing_key;
                const redeliveredEl = msgNode.querySelector('.message-redelivered');
                if (redeliveredEl)
                    redeliveredEl.textContent = String(message.redelivered);
                const propsEl = msgNode.querySelector('.message-properties');
                if (propsEl)
                    propsEl.textContent = JSON.stringify(message.properties);
                const sizeEl = msgNode.querySelector('.message-size');
                if (sizeEl)
                    sizeEl.textContent = String(message.payload_bytes);
                const encodingEl = msgNode.querySelector('.message-encoding');
                if (encodingEl)
                    encodingEl.textContent = message.payload_encoding;
                const payloadEl = msgNode.querySelector('.message-payload');
                if (payloadEl)
                    payloadEl.textContent = message.payload;
                msgNode.classList.remove('hide');
                messagesContainer.appendChild(msgNode);
            }
        });
    });
}
const moveMessagesForm = document.querySelector('#moveMessages');
if (moveMessagesForm) {
    moveMessagesForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const username = Auth.getUsername() ?? '';
        const password = Auth.getPassword() ?? '';
        const uri = HTTP.url `amqp://${username}:${password}@localhost/${vhost}`;
        const destInput = document.querySelector('[name=shovel-destination]');
        const dest = destInput?.value.trim() ?? '';
        const name = 'Move ' + queue + ' to ' + dest;
        const url = HTTP.url `api/parameters/shovel/${vhost}/${name}`;
        const body = {
            name,
            value: {
                'src-uri': uri,
                'src-queue': queue,
                'dest-uri': uri,
                'dest-queue': dest,
                'src-prefetch-count': 1000,
                'ack-mode': 'on-confirm',
                'src-delete-after': 'queue-length',
            },
        };
        HTTP.request('PUT', url, { body }).then(() => {
            form.reset();
            DOM.toast('Moving messages to ' + dest);
        });
    });
}
const purgeForm = document.querySelector('#purgeQueue');
if (purgeForm) {
    purgeForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        let params = '';
        const countElem = form.querySelector("input[name='count']");
        if (countElem && countElem.value) {
            params = `?count=${countElem.value}`;
        }
        const url = HTTP.url `api/queues/${vhost}/${queue}/contents${HTTP.noencode(params)}`;
        if (window.confirm('Are you sure? Messages cannot be recovered after purging.')) {
            HTTP.request('DELETE', url).then(() => {
                DOM.toast('Queue purged!');
            });
        }
    });
}
const deleteForm = document.querySelector('#deleteQueue');
if (deleteForm) {
    deleteForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const url = HTTP.url `api/queues/${vhost}/${queue}`;
        if (window.confirm('Are you sure? The queue is going to be deleted. Messages cannot be recovered after deletion.')) {
            HTTP.request('DELETE', url).then(() => {
                window.location.href = 'queues';
            });
        }
    });
}
if (pauseQueueForm) {
    pauseQueueForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const url = HTTP.url `api/queues/${vhost}/${queue}/pause`;
        if (window.confirm('Are you sure? This will suspend deliveries to all consumers.')) {
            HTTP.request('PUT', url).then(() => {
                DOM.toast('Queue paused!');
                handleQueueState('paused');
            });
        }
    });
}
if (resumeQueueForm) {
    resumeQueueForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const url = HTTP.url `api/queues/${vhost}/${queue}/resume`;
        if (window.confirm('Are you sure? This will resume deliveries to all consumers.')) {
            HTTP.request('PUT', url).then(() => {
                DOM.toast('Queue resumed!');
                handleQueueState('running');
            });
        }
    });
}
if (restartQueueForm) {
    restartQueueForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const url = HTTP.url `api/queues/${vhost}/${queue}/restart`;
        if (window.confirm('Are you sure? This will restart the queue.')) {
            HTTP.request('PUT', url).then((res) => {
                if (res && res.is_error)
                    return;
                DOM.toast('Queue restarted!');
                handleQueueState('running');
            });
        }
    });
}
Helpers.autoCompleteDatalist('exchange-list', 'exchanges', vhost);
Helpers.autoCompleteDatalist('queue-list', 'queues', vhost);
const dataTags = document.querySelector('#dataTags');
if (dataTags) {
    dataTags.addEventListener('click', (e) => {
        Helpers.argumentHelperJSON('publishMessage', 'properties', e);
    });
}
//# sourceMappingURL=queue.js.map