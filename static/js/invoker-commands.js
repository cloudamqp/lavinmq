// Buttons open and close dialogs with the Invoker Commands API
// (command="show-modal" commandfor="id"). Browsers without it (Chrome < 135,
// Firefox < 144, Safari < 26.2) ignore those attributes, so do the same from
// a click handler there.
if (!('commandForElement' in window.HTMLButtonElement.prototype)) {
  document.addEventListener('click', (event) => {
    if (!(event.target instanceof window.Element)) return
    const button = event.target.closest('button[commandfor]')
    if (!button || button.disabled) return
    const dialog = document.getElementById(button.getAttribute('commandfor'))
    if (!(dialog instanceof window.HTMLDialogElement)) return
    switch (button.getAttribute('command')) {
      case 'show-modal':
        if (!dialog.open) dialog.showModal()
        break
      case 'close':
        if (dialog.open) dialog.close(button.value)
        break
      case 'request-close':
        if (!dialog.open) break
        if (dialog.requestClose) dialog.requestClose(button.value)
        else dialog.close(button.value)
        break
    }
  })
}
