import * as HTTP from './http.js';
import * as Helpers from './helpers.js';
import * as Table from './table.js';
import * as DOM from './dom.js';
import * as Form from './form.js';
const user = new URLSearchParams(window.location.hash.substring(1)).get('name') ?? '';
function updateUser() {
    const userUrl = HTTP.url `api/users/${user}`;
    HTTP.request('GET', userUrl).then((item) => {
        if (!item)
            return;
        const hasPassword = item.password_hash ? '\u25CF' : '\u25CB';
        const tagsEl = document.getElementById('tags');
        const hasPasswordEl = document.getElementById('hasPassword');
        if (tagsEl)
            tagsEl.textContent = item.tags;
        if (hasPasswordEl)
            hasPasswordEl.textContent = hasPassword;
        tagHelper(item.tags);
    });
}
function tagHelper(tags) {
    const vals = tags.split(',');
    vals.forEach((val) => {
        const input = document.querySelector('[name=tags]');
        if (input) {
            const currentVal = input.value;
            input.value = currentVal ? currentVal + ', ' + val : val;
        }
    });
}
const permissionsUrl = HTTP.url `api/users/${user}/permissions`;
const tableOptions = { url: permissionsUrl, keyColumns: ['vhost'], autoReloadTimeout: 0, countId: 'permissions-count' };
const permissionsTable = Table.renderTable('permissions', tableOptions, (tr, item, all) => {
    Table.renderCell(tr, 1, item.configure);
    Table.renderCell(tr, 2, item.write);
    Table.renderCell(tr, 3, item.read);
    if (all) {
        const buttons = document.createElement('div');
        buttons.classList.add('buttons');
        const deleteBtn = DOM.button.delete({
            text: 'Clear',
            click: function () {
                const url = HTTP.url `api/permissions/${item.vhost}/${item.user}`;
                HTTP.request('DELETE', url).then(() => {
                    tr.parentNode?.removeChild(tr);
                });
            },
        });
        const editBtn = DOM.button.edit({
            click: function () {
                Form.editItem('#setPermission', item);
            },
        });
        buttons.append(editBtn, deleteBtn);
        Table.renderCell(tr, 0, item.vhost);
        Table.renderCell(tr, 4, buttons, 'right');
    }
});
Helpers.addVhostOptions('setPermission');
const setPermForm = document.querySelector('#setPermission');
if (setPermForm) {
    setPermForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const vhost = data.get('vhost');
        const url = HTTP.url `api/permissions/${vhost}/${user}`;
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
const removePasswordCheckbox = document.querySelector('[name=remove_password]');
if (removePasswordCheckbox) {
    removePasswordCheckbox.addEventListener('change', function () {
        const pwd = document.querySelector('[name=password]');
        if (pwd) {
            if (this.checked) {
                pwd.disabled = true;
                pwd.required = false;
            }
            else {
                pwd.disabled = false;
                pwd.required = true;
            }
        }
    });
}
const updateForm = document.querySelector('#updateUser');
if (updateForm) {
    updateForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const pwd = document.querySelector('[name=password]');
        const data = new FormData(form);
        const url = HTTP.url `api/users/${user}`;
        const body = {
            tags: data.get('tags'),
        };
        if (data.get('remove_password') === 'on') {
            body.password_hash = '';
        }
        else if (data.get('password') !== '') {
            body.password = data.get('password');
        }
        HTTP.request('PUT', url, { body }).then(() => {
            updateUser();
            DOM.toast('User updated');
            form.reset();
            if (pwd) {
                pwd.disabled = false;
                pwd.required = true;
            }
        });
    });
}
const dataTags = document.querySelector('#dataTags');
if (dataTags) {
    dataTags.addEventListener('click', (e) => {
        Helpers.argumentHelper('updateUser', 'tags', e);
    });
}
const deleteForm = document.querySelector('#deleteUser');
if (deleteForm) {
    deleteForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const url = HTTP.url `api/users/${user}`;
        if (window.confirm('Are you sure? This object cannot be recovered after deletion.')) {
            HTTP.request('DELETE', url).then(() => {
                window.location.href = 'users';
            });
        }
    });
}
document.addEventListener('DOMContentLoaded', () => {
    document.title = user + ' | LavinMQ';
    const pagenameLabel = document.querySelector('#pagename-label');
    if (pagenameLabel)
        pagenameLabel.textContent = user;
    updateUser();
});
//# sourceMappingURL=user.js.map