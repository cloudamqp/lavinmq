import * as HTTP from './http.js';
import * as Table from './table.js';
import * as DOM from './dom.js';
const vhost = new URLSearchParams(window.location.hash.substring(1)).get('name') ?? '';
document.title = vhost + ' | LavinMQ';
const pagenameLabel = document.querySelector('#pagename-label');
if (pagenameLabel)
    pagenameLabel.textContent = vhost;
const vhostUrl = HTTP.url `api/vhosts/${vhost}`;
HTTP.request('GET', vhostUrl).then((item) => {
    if (!item)
        return;
    const messagesReadyEl = document.getElementById('messages_ready');
    const messagesUnackedEl = document.getElementById('messages_unacknowledged');
    const messagesTotalEl = document.getElementById('messages_total');
    if (messagesReadyEl)
        messagesReadyEl.textContent = item.messages_ready.toLocaleString();
    if (messagesUnackedEl)
        messagesUnackedEl.textContent = item.messages_unacknowledged.toLocaleString();
    if (messagesTotalEl)
        messagesTotalEl.textContent = item.messages.toLocaleString();
});
function fetchLimits() {
    HTTP.request('GET', HTTP.url `api/vhost-limits/${vhost}`).then((arr) => {
        const limits = arr?.[0] ?? { value: {} };
        const maxConnections = limits.value['max-connections'] ?? '';
        const maxConnectionsEl = document.getElementById('max-connections');
        if (maxConnectionsEl)
            maxConnectionsEl.textContent = String(maxConnections);
        const maxConnectionsInput = document.forms.namedItem('setLimits')?.elements.namedItem('max-connections');
        if (maxConnectionsInput)
            maxConnectionsInput.value = String(maxConnections);
        const maxQueues = limits.value['max-queues'] ?? '';
        const maxQueuesEl = document.getElementById('max-queues');
        if (maxQueuesEl)
            maxQueuesEl.textContent = String(maxQueues);
        const maxQueuesInput = document.forms.namedItem('setLimits')?.elements.namedItem('max-queues');
        if (maxQueuesInput)
            maxQueuesInput.value = String(maxQueues);
    });
}
fetchLimits();
const permissionsUrl = HTTP.url `api/vhosts/${vhost}/permissions`;
const tableOptions = { url: permissionsUrl, keyColumns: ['user'], countId: 'permissions-count' };
const permissionsTable = Table.renderTable('permissions', tableOptions, (tr, item, all) => {
    Table.renderCell(tr, 1, item.configure);
    Table.renderCell(tr, 2, item.write);
    Table.renderCell(tr, 3, item.read);
    if (all) {
        const btn = DOM.button.delete({
            text: 'Clear',
            click: function () {
                const url = HTTP.url `api/permissions/${vhost}/${item.user}`;
                HTTP.request('DELETE', url).then(() => tr.parentNode?.removeChild(tr));
            },
        });
        const userLink = document.createElement('a');
        userLink.href = HTTP.url `user#name=${item.user}`;
        userLink.textContent = item.user;
        Table.renderCell(tr, 0, userLink);
        Table.renderCell(tr, 4, btn, 'right');
    }
});
function addUserOptions(users) {
    const form = document.forms.namedItem('setPermission');
    const select = form?.elements.namedItem('user');
    if (!select)
        return;
    while (select.options.length)
        select.remove(0);
    for (let i = 0; i < users.length; i++) {
        const opt = document.createElement('option');
        opt.text = users[i]?.name ?? '';
        select.add(opt);
    }
}
function fetchUsers(cb) {
    const url = 'api/users';
    const raw = window.sessionStorage.getItem(url);
    if (raw) {
        const users = JSON.parse(raw);
        cb(users);
    }
    HTTP.request('GET', url)
        .then(function (users) {
        if (!users)
            return;
        try {
            window.sessionStorage.setItem('api/users', JSON.stringify(users));
        }
        catch (e) {
            console.error('Saving sessionStorage', e);
        }
        cb(users);
    })
        .catch(function (e) {
        console.error(e.message);
    });
}
fetchUsers(addUserOptions);
const setPermForm = document.querySelector('#setPermission');
if (setPermForm) {
    setPermForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const url = HTTP.url `api/permissions/${vhost}/${data.get('user')}`;
        const body = {
            configure: data.get('configure'),
            write: data.get('write'),
            read: data.get('read'),
        };
        HTTP.request('PUT', url, { body }).then(() => {
            permissionsTable.reload();
            form.reset();
        });
    });
}
const setLimitsForm = document.forms.namedItem('setLimits');
if (setLimitsForm) {
    setLimitsForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const maxConnectionsUrl = HTTP.url `api/vhost-limits/${vhost}/max-connections`;
        const maxConnectionsInput = form['max-connections'];
        const maxConnectionsBody = { value: Number(maxConnectionsInput.value || -1) };
        const maxQueuesUrl = HTTP.url `api/vhost-limits/${vhost}/max-queues`;
        const maxQueuesInput = form['max-queues'];
        const maxQueuesBody = { value: Number(maxQueuesInput.value || -1) };
        Promise.all([
            HTTP.request('PUT', maxConnectionsUrl, { body: maxConnectionsBody }),
            HTTP.request('PUT', maxQueuesUrl, { body: maxQueuesBody }),
        ]).then(fetchLimits);
    });
}
const deleteForm = document.querySelector('#deleteVhost');
if (deleteForm) {
    deleteForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const url = HTTP.url `api/vhosts/${vhost}`;
        if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
            HTTP.request('DELETE', url).then(() => {
                window.location.href = 'vhosts';
            });
        }
    });
}
//# sourceMappingURL=vhost.js.map