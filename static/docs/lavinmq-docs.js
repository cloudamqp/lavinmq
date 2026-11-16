// Theme of the API docs. It shares its state with the management UI: the
// 'system', 'theme-light' and 'theme-dark' classes in localStorage
// 'lmq.stateclasses', see stateClasses in static/js/helpers.js and
// ThemeSwitcher in static/js/layout.js. Loaded in <head> so the theme is set
// before the page is drawn. Elements' CSS follows data-theme on <html>.
(function () {
  const key = 'lmq.stateclasses'
  const mql = window.matchMedia('(prefers-color-scheme: dark)')

  function storedClasses () {
    return (window.localStorage.getItem(key) || '').split(' ').filter(Boolean)
  }

  // 'light', 'dark' or 'system', read like ThemeSwitcher does
  function storedTheme () {
    const classes = storedClasses()
    if (!classes.includes('system')) {
      if (classes.includes('theme-light')) return 'light'
      if (classes.includes('theme-dark')) return 'dark'
    }
    return 'system'
  }

  function color (theme) {
    if (theme !== 'system') return theme
    return mql.matches ? 'dark' : 'light'
  }

  function apply (theme) {
    const html = document.documentElement
    html.classList.toggle('theme-light', color(theme) === 'light')
    html.classList.toggle('theme-dark', color(theme) === 'dark')
    html.setAttribute('data-theme', color(theme))
    // data-mode, not data-theme as in the management UI: Elements' CSS sets
    // its colors on any [data-theme] element, so the buttons would get them
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.classList.toggle('active', button.dataset.mode === theme)
    })
  }

  // Store it the way ThemeSwitcher does: 'system' plus the class of the
  // current system color, or just the chosen class. Other classes are kept.
  function store (theme) {
    const classes = storedClasses().filter(k => !/^(system|theme-.+)$/.test(k))
    if (theme === 'system') classes.push('system')
    classes.push(`theme-${color(theme)}`)
    window.localStorage.setItem(key, classes.join(' '))
  }

  apply(storedTheme())

  mql.addEventListener('change', _ => {
    if (storedTheme() === 'system') {
      store('system')
      apply('system')
    }
  })
  window.addEventListener('storage', e => e.key === key && apply(storedTheme()))
  window.addEventListener('pageshow', e => e.persisted && apply(storedTheme()))

  document.addEventListener('DOMContentLoaded', _ => {
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.addEventListener('click', _ => {
        store(button.dataset.mode)
        apply(button.dataset.mode)
      })
    })
    apply(storedTheme())
  })
})()
