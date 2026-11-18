import * as Helpers from './helpers.js'

class ThemeSwitcher {
  constructor () {
    this.currentTheme = this.#storedTheme()
    this.init()
  }

  #storedTheme () {
    if (!Helpers.stateClasses.has('system')) {
      if (Helpers.stateClasses.has('theme-light')) {
        return 'light'
      } else if (Helpers.stateClasses.has('theme-dark')) {
        return 'dark'
      }
    }
    return 'system'
  }

  refresh () {
    Helpers.stateClasses.sync(/^(system|theme-.+)$/)
    this.currentTheme = this.#storedTheme()
    // Not applyTheme(currentTheme), it can write stale state back to local storage
    this.#setSystemColor(this.mql)
    this.updateActiveButton()
  }

  #setSystemColor (mql) {
    if (mql.matches) {
      this.systemColor = 'dark'
    } else {
      this.systemColor = 'light'
    }
    if (this.currentTheme === 'system') {
      this.applyTheme('system') // make sure right system class is used
    }
  }

  init () {
    this.mql = window.matchMedia('(prefers-color-scheme: dark)')
    this.#setSystemColor(this.mql)
    this.mql.addEventListener('change', mql => this.#setSystemColor(mql))
    // Add event listeners to theme buttons
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.addEventListener('click', _ => this.setTheme(button.dataset.theme))
    })

    window.addEventListener('pageshow', e => e.persisted && this.refresh())
    window.addEventListener('storage', e => e.key === 'lmq.stateclasses' && this.refresh())

    // Set initial active button
    this.updateActiveButton()
  }

  setTheme (theme) {
    this.currentTheme = theme
    this.applyTheme(theme)
    this.updateActiveButton()
  }

  applyTheme (theme) {
    if (theme === 'system') {
      Helpers.stateClasses.add('system')
      theme = this.systemColor
    } else {
      Helpers.stateClasses.remove('system')
    }
    if (theme === 'light') {
      Helpers.stateClasses.add('theme-light')
      Helpers.stateClasses.remove('theme-dark')
    } else {
      Helpers.stateClasses.add('theme-dark')
      Helpers.stateClasses.remove('theme-light')
    }
  }

  updateActiveButton () {
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.classList.remove('active')
    })

    const activeButton = document.querySelector(`#theme-switcher button[data-theme="${this.currentTheme}"]`)
    if (activeButton) {
      activeButton.classList.add('active')
    }
  }
}

window.themeSwitcher = new ThemeSwitcher()
