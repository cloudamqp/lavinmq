function activate (tabs, tab) {
  const name = tab.dataset.tab
  tabs.forEach(t => t.classList.toggle('active', t === tab))
  document.querySelectorAll('[data-tab-content]').forEach(el => {
    el.classList.toggle('hide', el.dataset.tabContent !== name)
  })
  const params = new URLSearchParams(location.hash.substring(1))
  params.set('tab', name)
  history.replaceState(null, '', '#' + params.toString())
}

function initTabs (selector = '.tab-bar') {
  const bar = document.querySelector(selector)
  if (!bar) return
  const tabs = Array.from(bar.querySelectorAll('[data-tab]'))
  if (!tabs.length) return

  const params = new URLSearchParams(location.hash.substring(1))
  const active =
    tabs.find(t => t.dataset.tab === params.get('tab')) ??
    tabs.find(t => t.classList.contains('active')) ??
    tabs[0]

  activate(tabs, active)
  tabs.forEach(tab => tab.addEventListener('click', () => activate(tabs, tab)))

  window.addEventListener('hashchange', () => {
    const p = new URLSearchParams(location.hash.substring(1))
    const tab = tabs.find(t => t.dataset.tab === p.get('tab'))
    if (tab) activate(tabs, tab)
  })
}

export { initTabs }
