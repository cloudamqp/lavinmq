type ValueFactory<T> = (item: T) => string | undefined

export function editItem<T>(
  form: HTMLFormElement | string,
  item: T,
  valueFactories?: Record<string, ValueFactory<T>>
): void {
  const factories = valueFactories ?? {}
  const formEl = form instanceof HTMLFormElement ? form : document.querySelector<HTMLFormElement>(form)
  if (!formEl) return

  formEl.classList.add('edit-mode')
  const pkfield = formEl.querySelector<HTMLInputElement | HTMLSelectElement>('[data-primary-key]')
  if (pkfield) {
    pkfield.disabled = true
  }
  const itemRecord = item as Record<string, unknown>
  formEl.querySelectorAll<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>('input, select, textarea').forEach((input) => {
    let value: unknown = itemRecord[input.name]
    const itemValue = itemRecord['value'] as Record<string, unknown> | undefined
    if (!value && itemValue) {
      value = itemValue[input.name]
    }
    if (factories[input.name]) {
      value = factories[input.name]!(item)
    }
    if (input instanceof HTMLSelectElement) {
      input.selectedIndex = Array.from(input.options).findIndex((i) => i.value === value)
    } else {
      input.value = value as string ?? ''
    }
    input.dispatchEvent(new Event('change'))
  })
}

document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll<HTMLFormElement>('form').forEach((form) => {
    form.addEventListener('formdata', ((evt: FormDataEvent) => {
      // Disabled fields wont be included in formdata, so this is a
      // workaround when we edit an entity
      const pkfield = form.querySelector<HTMLInputElement | HTMLSelectElement>('[data-primary-key]')
      if (pkfield && pkfield.disabled) {
        evt.formData.set(pkfield.name, pkfield.value)
      }
    }) as EventListener)
    form.addEventListener('reset', () => {
      if (form.classList.contains('edit-mode')) {
        form.classList.remove('edit-mode')
        const pkfield = form.querySelector<HTMLInputElement | HTMLSelectElement>('[data-primary-key]')
        if (pkfield) {
          pkfield.disabled = false
        }
      }
    })
  })
})
