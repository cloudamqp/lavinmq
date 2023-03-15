function editItem(form, item, valueFactories)  {
  valueFactories = valueFactories ?? {}
  form = form instanceof HTMLFormElement ?  form : document.querySelector(form)
  form.classList.add('edit')
  const pkfield = form.querySelector('.primary-key')
  console.log(pkfield)
  if (pkfield) { pkfield.setAttribute("readonly", true) }
  form.querySelectorAll('input, select').forEach(input => {
    let value = item[input.name] || item.value[input.name]
    if (valueSelectors[input.name]) { value = valueFactories[input.name](item) }
    if (input instanceof HTMLSelectElement) {
      input.selectedIndex = Array.from(input.options).findIndex(i => i.value == value)
    } else {
      input.value = value
    }
  })
}
document.addEventListener('DOMContentLoaded', function() {
  document.querySelectorAll('form button.cancel').forEach(btn => {
    btn.onclick = _ => {
      btn.form.classList.remove("edit")
      btn.form.querySelector('.primary-key').setAttribute("readonly", false)
    }
  })
})
export {
  editItem
}
