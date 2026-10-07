/* global location, history */
function activate (barName, tabs, tab) {
  const tabName = tab.dataset.tab
  tabs.forEach(t => t.classList.toggle('active', t === tab))
  document.querySelectorAll(`[data-tab-bar="${barName}"][data-tab-content]`).forEach(el => {
    el.classList.toggle('hide', el.dataset.tabContent !== tabName)
  })
  const params = new URLSearchParams(location.hash.substring(1))
  params.set(barName, tabName)
  history.replaceState(null, '', '#' + params.toString())
}

function initTabBar (barEl) {
  const barName = barEl.dataset.tabBar
  const tabs = Array.from(barEl.querySelectorAll('[data-tab]'))
  if (!tabs.length) return

  const params = new URLSearchParams(location.hash.substring(1))
  const active =
    tabs.find(t => t.dataset.tab === params.get(barName)) ??
    tabs.find(t => t.classList.contains('active')) ??
    tabs[0]

  activate(barName, tabs, active)
  tabs.forEach(tab => tab.addEventListener('click', () => activate(barName, tabs, tab)))
}

document.querySelectorAll('[data-tab-bar]').forEach(initTabBar)

window.addEventListener('hashchange', () => {
  const params = new URLSearchParams(location.hash.substring(1))
  document.querySelectorAll('[data-tab-bar]').forEach(barEl => {
    const barName = barEl.dataset.tabBar
    const tabs = Array.from(barEl.querySelectorAll('[data-tab]'))
    const tab = tabs.find(t => t.dataset.tab === params.get(barName))
    if (tab) activate(barName, tabs, tab)
  })
})
