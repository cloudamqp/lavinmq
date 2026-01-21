import * as HTTP from './http.js';
import * as Table from './table.js';
import * as Helpers from './helpers.js';
import * as DOM from './dom.js';
import * as Form from './form.js';
import { UrlDataSource } from './datasource.js';
Helpers.addVhostOptions('createShovel');
function renderState(item) {
    if (item.error) {
        const state = document.createElement('a');
        state.classList.add('arg-tooltip');
        state.appendChild(document.createTextNode(item.state ?? ''));
        const tooltip = document.createElement('span');
        tooltip.classList.add('tooltiptext');
        tooltip.textContent = item.error;
        state.appendChild(tooltip);
        return state;
    }
    else {
        return item.state ?? '';
    }
}
const vhost = window.sessionStorage.getItem('vhost');
let url = 'api/parameters/shovel';
let statusUrl = 'api/shovels';
if (vhost && vhost !== '_all') {
    url += HTTP.url `/${vhost}`;
    statusUrl += HTTP.url `/${vhost}`;
}
class ShovelsDataSource extends UrlDataSource {
    statusUrl;
    constructor(baseUrl, shovelsStatusUrl) {
        super(baseUrl);
        this.statusUrl = shovelsStatusUrl;
    }
    _reload() {
        const p1 = super._reload();
        const p2 = HTTP.request('GET', this.statusUrl);
        return Promise.all([p1, p2]).then((values) => {
            const response = values[0];
            let shovels = 'items' in response ? response.items : response;
            const status = values[1] ?? [];
            shovels = shovels.map((item) => {
                const st = status.find((s) => s.name === item.name && s.vhost === item.vhost);
                item.state = st?.state;
                item.error = st?.error;
                return item;
            });
            return shovels;
        });
    }
}
const dataSource = new ShovelsDataSource(url, statusUrl);
const tableOptions = { keyColumns: ['vhost', 'name'], columnSelector: true, dataSource };
Table.renderTable('table', tableOptions, (tr, item) => {
    Table.renderCell(tr, 0, item.vhost);
    Table.renderCell(tr, 1, item.name);
    const srcUri = item.value['src-uri'];
    if (Array.isArray(srcUri)) {
        Table.renderCell(tr, 2, srcUri.map((uri) => decodeURI(uri.replace(/:([^:]+)@/, ':***@'))).join(', '));
    }
    else {
        Table.renderCell(tr, 2, decodeURI(srcUri.replace(/:([^:]+)@/, ':***@')));
    }
    const srcDiv = document.createElement('span');
    const consumerArgs = item.value['src-consumer-args'] || {};
    let srcType = 'exchange';
    if (consumerArgs['x-stream-offset']) {
        srcType = 'stream';
    }
    else if (item.value['src-queue']) {
        srcType = 'queue';
    }
    if (srcType === 'exchange') {
        srcDiv.textContent = item.value['src-exchange'] ?? '';
    }
    else {
        srcDiv.textContent = item.value['src-queue'] ?? '';
    }
    srcDiv.appendChild(document.createElement('br'));
    srcDiv.appendChild(document.createElement('small')).textContent = srcType;
    Table.renderCell(tr, 3, srcDiv);
    Table.renderCell(tr, 4, item.value['src-prefetch-count']);
    const destUri = item.value['dest-uri'];
    if (Array.isArray(destUri)) {
        Table.renderCell(tr, 5, destUri.map((uri) => decodeURI(uri.replace(/:([^:]+)@/, ':***@'))).join(', '));
    }
    else {
        Table.renderCell(tr, 5, decodeURI(destUri.replace(/:([^:]+)@/, ':***@')));
    }
    const dest = document.createElement('span');
    if (item.value['dest-queue']) {
        dest.textContent = item.value['dest-queue'];
        dest.appendChild(document.createElement('br'));
        dest.appendChild(document.createElement('small')).textContent = 'queue';
    }
    else if (item.value['dest-exchange']) {
        dest.textContent = item.value['dest-exchange'];
        dest.appendChild(document.createElement('br'));
        dest.appendChild(document.createElement('small')).textContent = 'exchange';
    }
    else {
        dest.textContent = 'http';
    }
    Table.renderCell(tr, 6, dest);
    Table.renderCell(tr, 7, item.value['reconnect-delay']);
    Table.renderCell(tr, 8, item.value['ack-mode']);
    Table.renderCell(tr, 9, item.value['src-delete-after']);
    Table.renderCell(tr, 10, renderState(item));
    const btns = document.createElement('div');
    btns.classList.add('buttons');
    const deleteBtn = DOM.button.delete({
        click: function () {
            const deleteUrl = HTTP.url `api/parameters/shovel/${item.vhost}/${item.name}`;
            if (window.confirm('Are you sure? This shovel can not be restored after deletion.')) {
                HTTP.request('DELETE', deleteUrl).then(() => {
                    tr.parentNode?.removeChild(tr);
                    DOM.toast(`Shovel ${item.name} deleted`);
                });
            }
        },
    });
    const editBtn = DOM.button.edit({
        click: function () {
            Form.editItem('#createShovel', item, {
                'src-type': () => srcType,
                'dest-type': (i) => (i.value['dest-queue'] ? 'queue' : 'exchange'),
                'src-endpoint': (i) => i.value['src-queue'] || i.value['src-exchange'],
                'dest-endpoint': (i) => i.value['dest-queue'] || i.value['dest-exchange'],
                'src-offset': () => consumerArgs['x-stream-offset'],
            });
        },
    });
    const pauseLabel = ['Running', 'Starting'].includes(item.state ?? '') ? 'Pause' : 'Resume';
    const pauseBtn = DOM.button.edit({
        click: function () {
            const isRunning = item.state === 'Running';
            const action = isRunning ? 'pause' : 'resume';
            const actionUrl = HTTP.url `api/shovels/${item.vhost}/${item.name}/${action}`;
            if (window.confirm('Are you sure?')) {
                HTTP.request('PUT', actionUrl)
                    .then(() => {
                    dataSource.reload();
                    DOM.toast(`Shovel ${item.name} ${isRunning ? 'paused' : 'resumed'}`);
                })
                    .catch((err) => {
                    console.error(err);
                    DOM.toast.error(`Shovel ${item.name} failed to ${isRunning ? 'pause' : 'resume'}`);
                });
            }
        },
        text: pauseLabel,
    });
    btns.append(editBtn, pauseBtn, deleteBtn);
    Table.renderCell(tr, 11, btns, 'right');
});
const srcTypeSelect = document.querySelector('[name=src-type]');
if (srcTypeSelect) {
    srcTypeSelect.addEventListener('change', function () {
        const srcRoutingKey = document.getElementById('srcRoutingKey');
        const srcOffset = document.getElementById('srcOffset');
        srcRoutingKey?.classList.toggle('hide', this.value !== 'exchange');
        srcOffset?.classList.toggle('hide', this.value !== 'stream');
    });
}
const destUriInput = document.querySelector('[name=dest-uri]');
if (destUriInput) {
    destUriInput.addEventListener('change', function () {
        const isHttp = this.value.startsWith('http');
        document.querySelectorAll('.amqp-dest-field').forEach((e) => {
            e.classList.toggle('hide', isHttp);
        });
    });
}
const createForm = document.querySelector('#createShovel');
if (createForm) {
    createForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const name = data.get('name').trim();
        const formVhost = data.get('vhost');
        const createUrl = HTTP.url `api/parameters/shovel/${formVhost}/${name}`;
        const body = {
            value: {
                'src-uri': data.get('src-uri'),
                'dest-uri': data.get('dest-uri'),
                'src-prefetch-count': parseInt(data.get('src-prefetch-count'), 10),
                'src-delete-after': data.get('src-delete-after'),
                'reconnect-delay': parseInt(data.get('reconnect-delay'), 10),
                'ack-mode': data.get('ack-mode'),
            },
        };
        const srcType = data.get('src-type');
        const offset = data.get('src-offset');
        switch (srcType) {
            case 'queue':
                body.value['src-queue'] = data.get('src-endpoint');
                break;
            case 'exchange':
                body.value['src-exchange'] = data.get('src-endpoint');
                body.value['src-exchange-key'] = data.get('src-exchange-key');
                break;
            case 'stream':
                body.value['src-queue'] = data.get('src-endpoint');
                if (offset.length) {
                    const args = body.value['src-consumer-args'] || {};
                    if (/^\d+$/.test(offset)) {
                        args['x-stream-offset'] = parseInt(offset, 10);
                    }
                    else {
                        args['x-stream-offset'] = offset;
                    }
                    body.value['src-consumer-args'] = args;
                }
                break;
        }
        if (data.get('dest-type') === 'queue') {
            body.value['dest-queue'] = data.get('dest-endpoint');
        }
        else {
            body.value['dest-exchange'] = data.get('dest-endpoint');
        }
        HTTP.request('PUT', createUrl, { body }).then(() => {
            dataSource.reload();
            form.reset();
            DOM.toast(`Shovel ${name} saved`);
        });
    });
}
//# sourceMappingURL=shovels.js.map