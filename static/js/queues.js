import * as HTTP from './http.js';
import * as Helpers from './helpers.js';
import * as DOM from './dom.js';
import * as Table from './table.js';
import { UrlDataSource } from './datasource.js';
Helpers.addVhostOptions('declare');
const vhost = window.sessionStorage.getItem('vhost');
let url = 'api/queues';
if (vhost && vhost !== '_all') {
    url += HTTP.url `/${vhost}`;
}
const queueDataSource = new UrlDataSource(url);
const tableOptions = {
    dataSource: queueDataSource,
    keyColumns: ['vhost', 'name'],
    pagination: true,
    columnSelector: true,
    search: true,
};
const multiSelectControls = document.getElementById('multiselect-controls');
const performMultiAction = (el) => {
    const target = el.target;
    const action = target.dataset.action;
    const elems = document.querySelectorAll('input[data-name]:checked');
    const totalCount = elems.length;
    let performed = 0;
    elems.forEach((elem) => {
        const data = elem.dataset;
        let actionUrl;
        const dataVhost = data.vhost ?? '';
        const dataName = data.name ?? '';
        switch (action) {
            case 'delete':
                actionUrl = HTTP.url `api/queues/${dataVhost}/${dataName}`;
                break;
            case 'purge':
                actionUrl = HTTP.url `api/queues/${dataVhost}/${dataName}/contents`;
                break;
        }
        if (!actionUrl)
            return;
        HTTP.request('DELETE', actionUrl)
            .then(() => {
            performed += 1;
            if (performed === totalCount) {
                multiSelectControls?.classList.add('hide');
                elems.forEach((e) => {
                    e.checked = false;
                });
                const checkAll = document.getElementById('multi-check-all');
                if (checkAll)
                    checkAll.checked = false;
                queuesTable.reload();
            }
        })
            .catch(() => {
            DOM.toast.error(`Failed to perform action on ${data.name}`);
            queuesTable.reload();
        });
    });
};
document.querySelectorAll('#multiselect-controls [data-action]').forEach((e) => e.addEventListener('click', performMultiAction));
const popupClose = document.querySelector('#multiselect-controls .popup-close');
if (popupClose) {
    popupClose.addEventListener('click', () => {
        toggleMultiActionControls(false, 0);
    });
}
const toggleMultiActionControls = (show, count) => {
    const currentlyHidden = multiSelectControls?.classList.contains('hide');
    if (currentlyHidden && show && count > 0) {
        multiSelectControls?.classList.remove('hide');
    }
    else if (!currentlyHidden && !show) {
        multiSelectControls?.classList.add('hide');
    }
    const countEl = document.getElementById('multi-queue-count');
    if (countEl)
        countEl.textContent = String(count);
};
const rowCheckboxChanged = () => {
    const checked = document.querySelectorAll('input[data-name]:checked');
    toggleMultiActionControls(true, checked.length);
};
const multiCheckAll = document.getElementById('multi-check-all');
if (multiCheckAll) {
    multiCheckAll.addEventListener('change', (el) => {
        const target = el.target;
        const checked = target.checked;
        let c = 0;
        document.querySelectorAll('input[data-name]').forEach((elem) => {
            elem.checked = checked;
            c += 1;
        });
        toggleMultiActionControls(checked, c);
    });
}
const queuesTable = Table.renderTable('table', tableOptions, function (tr, item, all) {
    if (all) {
        if (item.internal) {
            tr.classList.add('internal');
        }
        else {
            tr.classList.remove('internal');
        }
        const features = document.createElement('span');
        features.className = 'features';
        if (item.durable) {
            const durable = document.createElement('span');
            durable.textContent = 'D ';
            durable.title = 'Durable';
            features.appendChild(durable);
        }
        if (item.auto_delete) {
            const autoDelete = document.createElement('span');
            autoDelete.textContent = 'AD ';
            autoDelete.title = 'Auto Delete';
            features.appendChild(autoDelete);
        }
        if (item.exclusive) {
            const exclusive = document.createElement('span');
            exclusive.textContent = 'E ';
            exclusive.title = 'Exclusive';
            features.appendChild(exclusive);
        }
        if (Object.keys(item.arguments).length > 0) {
            const argsTooltip = Object.entries(item.arguments)
                .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
                .join('\n');
            const argsSpan = document.createElement('span');
            argsSpan.textContent = 'Args ';
            argsSpan.title = argsTooltip;
            features.appendChild(argsSpan);
        }
        const queueLink = document.createElement('a');
        const qType = item.arguments['x-queue-type'];
        if (qType === 'stream') {
            queueLink.href = HTTP.url `stream#vhost=${item.vhost}&name=${item.name}`;
        }
        else {
            queueLink.href = HTTP.url `queue#vhost=${item.vhost}&name=${item.name}`;
        }
        queueLink.textContent = item.name;
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.setAttribute('data-vhost', item.vhost);
        checkbox.setAttribute('data-name', item.name);
        checkbox.addEventListener('change', rowCheckboxChanged);
        Table.renderCell(tr, 0, checkbox, 'checkbox');
        Table.renderCell(tr, 1, item.vhost);
        Table.renderCell(tr, 2, queueLink);
        Table.renderCell(tr, 3, features, 'center');
    }
    let policyLink = '';
    if (item.policy) {
        policyLink = document.createElement('a');
        policyLink.href = HTTP.url `policies#name=${item.policy}&vhost=${item.vhost}`;
        policyLink.textContent = item.policy;
    }
    Table.renderCell(tr, 4, policyLink, 'center');
    Table.renderCell(tr, 5, item.consumers, 'right');
    Table.renderCell(tr, 6, null, 'center ' + 'state-' + item.state);
    Table.renderCell(tr, 7, Helpers.formatNumber(item.messages_ready), 'right');
    Table.renderCell(tr, 8, Helpers.formatNumber(item.messages_unacknowledged), 'right');
    Table.renderCell(tr, 9, Helpers.formatNumber(item.messages), 'right');
    Table.renderCell(tr, 10, Helpers.formatNumber(item.message_stats.publish_details.rate), 'right');
    Table.renderCell(tr, 11, Helpers.formatNumber(item.message_stats.deliver_details.rate), 'right');
    Table.renderCell(tr, 12, Helpers.formatNumber(item.message_stats.redeliver_details.rate), 'right');
    Table.renderCell(tr, 13, Helpers.formatNumber(item.message_stats.ack_details.rate), 'right');
    Table.renderCell(tr, 14, Helpers.nFormatter(item.total_bytes) + 'B', 'right');
});
const declareForm = document.querySelector('#declare');
if (declareForm) {
    declareForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const formVhost = data.get('vhost');
        const queue = data.get('name').trim();
        const createUrl = HTTP.url `api/queues/${formVhost}/${queue}`;
        const body = {
            durable: data.get('durable') === '1',
            auto_delete: data.get('auto_delete') === '1',
            arguments: DOM.parseJSON(data.get('arguments')),
        };
        HTTP.request('PUT', createUrl, { body }).then((response) => {
            if (response?.is_error) {
                return;
            }
            queuesTable.reload();
            form.reset();
            const vhostSelect = form.querySelector('select[name="vhost"]');
            if (vhostSelect)
                vhostSelect.value = decodeURIComponent(formVhost);
            DOM.toast('Queue ' + queue + ' created');
        });
    });
}
queuesTable.on('updated', () => {
    const checked = document.querySelectorAll('input[data-name]:checked');
    const countEl = document.getElementById('multi-queue-count');
    if (countEl)
        countEl.textContent = String(checked.length);
});
const dataTags = document.querySelector('#dataTags');
if (dataTags) {
    dataTags.addEventListener('click', (e) => {
        Helpers.argumentHelperJSON('declare', 'arguments', e);
    });
}
//# sourceMappingURL=queues.js.map