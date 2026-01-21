import * as HTTP from './http.js';
import * as Table from './table.js';
import * as Helpers from './helpers.js';
import * as DOM from './dom.js';
import * as Form from './form.js';
Helpers.addVhostOptions('createUpstream');
let url = 'api/parameters/federation-upstream';
let linksUrl = 'api/federation-links';
const vhost = window.sessionStorage.getItem('vhost');
if (vhost && vhost !== '_all') {
    url += HTTP.url `/${vhost}`;
    linksUrl += HTTP.url `/${vhost}`;
}
const utOpts = { url, keyColumns: ['vhost', 'name'] };
const upstreamsTable = Table.renderTable('upstreamTable', utOpts, (tr, item) => {
    Table.renderCell(tr, 0, item.vhost);
    Table.renderCell(tr, 1, item.name);
    Table.renderCell(tr, 2, decodeURI(item.value.uri.replace(/:([^:]+)@/, ':***@')));
    Table.renderCell(tr, 3, item.value['prefetch-count']);
    Table.renderCell(tr, 4, item.value['reconnect-delay']);
    Table.renderCell(tr, 5, item.value['ack-mode']);
    Table.renderCell(tr, 6, item.value.exchange);
    Table.renderCell(tr, 7, item.value['max-hops']);
    Table.renderCell(tr, 8, item.value.expires);
    Table.renderCell(tr, 9, item.value['message-ttl']);
    Table.renderCell(tr, 10, item.value.queue);
    Table.renderCell(tr, 11, item.value['consumer-tag']);
    const buttons = document.createElement('div');
    buttons.classList.add('buttons');
    const deleteBtn = DOM.button.delete({
        click: function () {
            const deleteUrl = HTTP.url `api/parameters/federation-upstream/${item.vhost}/${item.name}`;
            if (!window.confirm(`Delete federation upstream ${item.name} ?`))
                return;
            HTTP.request('DELETE', deleteUrl).then(() => {
                tr.parentNode?.removeChild(tr);
                DOM.toast(`Upstream ${item.name} deleted`);
            });
        },
    });
    const editBtn = DOM.button.edit({
        click: function () {
            Form.editItem('#createUpstream', item);
        },
    });
    buttons.append(editBtn, deleteBtn);
    Table.renderCell(tr, 11, buttons, 'right');
});
const linksOpts = { url: linksUrl, keyColumns: ['vhost', 'name'], countId: 'links-count' };
Table.renderTable('linksTable', linksOpts, (tr, item) => {
    const resourceDiv = document.createElement('span');
    resourceDiv.textContent = item.resource;
    resourceDiv.appendChild(document.createElement('br'));
    resourceDiv.appendChild(document.createElement('small')).textContent = item.type;
    Table.renderCell(tr, 0, item.vhost);
    Table.renderCell(tr, 1, item.name);
    Table.renderCell(tr, 2, decodeURI(item.uri));
    Table.renderCell(tr, 3, resourceDiv);
    Table.renderCell(tr, 4, item.timestamp);
});
const createForm = document.querySelector('#createUpstream');
if (createForm) {
    createForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const name = data.get('name').trim();
        const formVhost = data.get('vhost');
        const createUrl = HTTP.url `api/parameters/federation-upstream/${formVhost}/${name}`;
        const body = {
            value: {
                uri: data.get('uri'),
                'prefetch-count': parseInt(data.get('prefetch-count'), 10),
                'reconnect-delay': parseInt(data.get('reconnect-delay'), 10),
                'ack-mode': data.get('ack-mode'),
                exchange: data.get('exchange'),
                'max-hops': parseInt(data.get('max-hops'), 10),
                expires: parseInt(data.get('expires'), 10),
                'message-ttl': parseInt(data.get('message-ttl'), 10),
                queue: data.get('queue'),
                'consumer-tag': data.get('consumer-tag'),
            },
        };
        HTTP.request('PUT', createUrl, { body }).then(() => {
            upstreamsTable.reload();
            form.reset();
            DOM.toast(`Upstream ${name} saved`);
        });
    });
}
//# sourceMappingURL=federation.js.map