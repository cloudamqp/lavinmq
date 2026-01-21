export function editItem(form, item, valueFactories) {
    const factories = valueFactories ?? {};
    const formEl = form instanceof HTMLFormElement ? form : document.querySelector(form);
    if (!formEl)
        return;
    formEl.classList.add('edit-mode');
    const pkfield = formEl.querySelector('[data-primary-key]');
    if (pkfield) {
        pkfield.disabled = true;
    }
    const itemRecord = item;
    formEl.querySelectorAll('input, select, textarea').forEach((input) => {
        let value = itemRecord[input.name];
        const itemValue = itemRecord['value'];
        if (!value && itemValue) {
            value = itemValue[input.name];
        }
        if (factories[input.name]) {
            value = factories[input.name](item);
        }
        if (input instanceof HTMLSelectElement) {
            input.selectedIndex = Array.from(input.options).findIndex((i) => i.value === value);
        }
        else {
            input.value = value ?? '';
        }
        input.dispatchEvent(new Event('change'));
    });
}
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('form').forEach((form) => {
        form.addEventListener('formdata', ((evt) => {
            // Disabled fields wont be included in formdata, so this is a
            // workaround when we edit an entity
            const pkfield = form.querySelector('[data-primary-key]');
            if (pkfield && pkfield.disabled) {
                evt.formData.set(pkfield.name, pkfield.value);
            }
        }));
        form.addEventListener('reset', () => {
            if (form.classList.contains('edit-mode')) {
                form.classList.remove('edit-mode');
                const pkfield = form.querySelector('[data-primary-key]');
                if (pkfield) {
                    pkfield.disabled = false;
                }
            }
        });
    });
});
//# sourceMappingURL=form.js.map