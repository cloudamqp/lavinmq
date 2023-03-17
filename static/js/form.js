function editItem(form, item, valueFactories)  {
  valueFactories = valueFactories ?? {}
  form = form instanceof HTMLFormElement ?  form : document.querySelector(form)
  form.classList.add('edit-mode')
  const pkfield = form.querySelector('[data-primary-key]')
  if (pkfield) {
    pkfield.disabled = true
  }
  form.querySelectorAll('input, select, textarea').forEach(input => {
    let value = item[input.name]
    if (!value && item.value) { value = item.value[input.name] }
    if (valueFactories[input.name]) { value = valueFactories[input.name](item) }
    if (input instanceof HTMLSelectElement) {
      input.selectedIndex = Array.from(input.options).findIndex(i => i.value == value)
    } else {
      input.value = value
    }
  })
}
document.addEventListener('DOMContentLoaded', function() {
  document.querySelectorAll('form').forEach(form => {
    form.addEventListener('formdata', evt => {
      // Disabled fields wont be included in formdata, so this is a
      // workaround when we edit an entity
      const pkfield = form.querySelector('[data-primary-key]')
      if (pkfield && pkfield.disabled) {
        evt.formData.set(pkfield.name, pkfield.value)
      }
    })
    form.addEventListener('reset', _ => {
      if (form.classList.contains('edit-mode')) {
        form.classList.remove("edit-mode")
        form.querySelector('[data-primary-key]').disabled = false
      }
    })
  })
})
export {
  editItem
}
