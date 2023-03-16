function editItem(form, item, valueFactories)  {
  valueFactories = valueFactories ?? {}
  form = form instanceof HTMLFormElement ?  form : document.querySelector(form)
  form.classList.add('edit-mode')
  const pkfield = form.querySelector('[data-primary-key]')
  if (pkfield) { pkfield.setAttribute("readonly", true) }
  form.querySelectorAll('input, select, textarea').forEach(input => {
    let value = item[input.name] || item.value[input.name]
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
    form.addEventListener('reset', _ => {
      if (form.classList.contains('edit-mode')) {
        console.log(form)
        form.classList.remove("edit-mode")
        form.querySelector('[data-primary-key]').removeAttribute("readonly")
      }
    })
  })
})
export {
  editItem
}