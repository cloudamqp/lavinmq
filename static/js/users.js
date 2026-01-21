import * as HTTP from './http.js';
import * as Helpers from './helpers.js';
import * as Table from './table.js';
import * as DOM from './dom.js';
let usersTable = null;
HTTP.request('GET', 'api/permissions')
    .then((permissions) => {
    const tableOptions = {
        url: 'api/users',
        keyColumns: ['vhost', 'name'],
        autoReloadTimeout: 0,
        pagination: true,
        columnSelector: true,
        search: true,
    };
    usersTable = Table.renderTable('users', tableOptions, (tr, item, all) => {
        if (all) {
            const userLink = document.createElement('a');
            userLink.href = HTTP.url `user#name=${item.name}`;
            userLink.textContent = item.name;
            Table.renderCell(tr, 0, userLink);
        }
        const hasPassword = item.password_hash ? '\u25CF' : '\u25CB';
        const vhosts = (permissions ?? [])
            .filter((p) => p.user === item.name)
            .map((p) => p.vhost)
            .join(', ');
        Table.renderCell(tr, 1, item.tags);
        Table.renderCell(tr, 2, vhosts);
        Table.renderCell(tr, 3, hasPassword);
    });
})
    .catch((e) => {
    Table.toggleDisplayError('users', e.status === 403 ? 'You need administrator role to see this view' : e.body ?? '');
});
const createForm = document.querySelector('#createUser');
if (createForm) {
    createForm.addEventListener('submit', function (evt) {
        evt.preventDefault();
        const form = evt.target;
        const data = new FormData(form);
        const username = data.get('username').trim();
        const url = HTTP.url `api/users/${username}`;
        let toastText = `User created: '${username}'`;
        const trs = document.querySelectorAll('#table tbody tr');
        trs.forEach((tr) => {
            if (username === tr.getAttribute('data-name')) {
                window.confirm(`Are you sure? This will update existing user: '${username}'`);
                toastText = `Upated existing user: '${username}'`;
                if (data.get('tags') === '') {
                    const tagsCell = tr.childNodes[1];
                    data.set('tags', tagsCell?.textContent ?? '');
                }
            }
        });
        const body = {
            tags: data.get('tags'),
        };
        if (data.get('password') !== '') {
            body.password = data.get('password');
        }
        HTTP.request('PUT', url, { body }).then(() => {
            usersTable?.reload();
            DOM.toast(toastText);
            form.reset();
        });
    });
}
const dataTags = document.querySelector('#dataTags');
if (dataTags) {
    dataTags.addEventListener('click', (e) => {
        Helpers.argumentHelper('createUser', 'tags', e);
    });
}
const generateBtn = document.querySelector('#generatePassword');
if (generateBtn) {
    generateBtn.addEventListener('click', generatePassword);
}
const toggleBtn = document.querySelector('.password-toggle');
if (toggleBtn) {
    toggleBtn.addEventListener('click', togglePasswordAsPlainText);
}
function generatePassword() {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-=[]{}|;:,.<>?';
    const password = Array.from(window.crypto.getRandomValues(new Uint8Array(16)), (x) => chars[x % chars.length]).join('');
    const input = document.querySelector('#createUser input[name="password"]');
    if (input) {
        input.value = password;
        input.type = 'text';
        setTimeout(() => {
            input.type = 'password';
        }, 500);
    }
}
function togglePasswordAsPlainText() {
    const input = document.querySelector('#createUser input[name="password"]');
    if (input) {
        const isPassword = input.type === 'password';
        input.type = isPassword ? 'text' : 'password';
    }
}
//# sourceMappingURL=users.js.map