// Fallback for the Invoker Commands API (command/commandfor attributes) in
// browsers without it: Chrome/Edge < 135, Firefox < 144, Safari < 26.2.
// Only dialog commands are handled, as those are the ones the views use.
if (!('command' in window.HTMLButtonElement.prototype)) {
  const actions = {
    'show-modal': dialog => { if (!dialog.open) dialog.showModal() },
    close: dialog => { if (dialog.open) dialog.close() },
    'request-close': dialog => {
      if (!dialog.open) return
      if (dialog.requestClose) dialog.requestClose()
      else dialog.close()
    }
  }
  document.addEventListener('click', evt => {
    const button = evt.target.closest?.('button[commandfor][command]')
    if (!button || button.disabled) return
    // Like the native API, buttons that submit or reset a form don't invoke commands
    if (button.form && button.type !== 'button') return
    const action = actions[button.getAttribute('command').toLowerCase()]
    const target = document.getElementById(button.getAttribute('commandfor'))
    if (action && target instanceof window.HTMLDialogElement) action(target)
  })
}
