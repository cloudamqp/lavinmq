export function parseJSON(data) {
    try {
        if (data.length) {
            return JSON.parse(data);
        }
        return {};
    }
    catch (e) {
        if (e instanceof SyntaxError) {
            window.alert('Input must be JSON');
        }
        throw e;
    }
}
export function jsonToText(obj) {
    if (obj == null)
        return '';
    return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '').trim();
}
function toastImpl(text, type = 'success') {
    // Delete all previous toasts
    document.querySelectorAll('.toast').forEach((t) => t.parentNode?.removeChild(t));
    const d = document.createElement('div');
    d.classList.add(type);
    d.classList.add('toast');
    d.textContent = text;
    document.body.appendChild(d);
    setTimeout(() => {
        try {
            document.body.removeChild(d);
        }
        catch {
            // noop
        }
    }, 7000);
}
export const toast = Object.assign(function toast(text) {
    toastImpl(text, 'success');
}, {
    success: function toastSuccess(text) {
        toastImpl(text, 'success');
    },
    warn: function toastWarn(text) {
        toastImpl(text, 'warn');
    },
    error: function toastError(text) {
        toastImpl(text, 'error');
    },
});
function createButton(type, text, classes, click) {
    const btn = document.createElement('button');
    btn.type = type;
    btn.textContent = text;
    btn.classList.add(...classes);
    if (click) {
        btn.addEventListener('click', click);
    }
    return btn;
}
export const button = {
    delete: ({ click, text = 'Delete', type = 'button' }) => {
        return createButton(type, text, ['btn-small', 'btn-small-outlined-danger'], click);
    },
    edit: ({ click, text = 'Edit', type = 'button' }) => {
        return createButton(type, text, ['btn-small'], click);
    },
    submit: ({ text = 'Save' } = {}) => {
        return createButton('submit', text, ['btn-icon', 'btn-submit']);
    },
    reset: ({ text = 'Reset' } = {}) => {
        return createButton('reset', text, ['btn-icon', 'btn-reset']);
    },
};
//# sourceMappingURL=dom.js.map