import * as Auth from './auth.js'
import * as Helpers from './helpers.js'

document.getElementById('username').textContent = Auth.getUsername()

const menuButton = document.getElementById('menu-button')
const menuContent = document.getElementById('menu-content')

menuButton.onclick = (e) => {
  menuButton.classList.toggle('open-menu')
  menuContent.classList.toggle('show-menu')
}

Helpers.addVhostOptions('user-vhost', { addAll: true }).then(() => {
  const vhost = window.sessionStorage.getItem('vhost')
  if (vhost) {
    const opt = document.querySelector('#userMenuVhost option[value="' + vhost + '"]')
    if (opt) {
      document.querySelector('#userMenuVhost').value = vhost
      window.sessionStorage.setItem('vhost', vhost)
    }
  } else {
    window.sessionStorage.setItem('vhost', '_all')
  }
})

document.getElementById('userMenuVhost').onchange = (e) => {
  window.sessionStorage.setItem('vhost', e.target.value)
  window.location.reload()
}

document.getElementById('signoutLink').onclick = () => {
  document.cookie = 'm=; max-age=0'
  window.location.assign('login')
}

const usermenuButton = document.getElementById('usermenu-button')
const usermenuContent = document.getElementById('user-menu')

usermenuButton.onclick = (e) => {
  usermenuButton.classList.toggle('open-menu')
  usermenuContent.classList.toggle('visible')
}

// Theme switcher functionality
class ThemeSwitcher {
  constructor() {
    this.currentTheme = localStorage.getItem('theme') || 'system'
    this.init()
  }

  init() {
    // Set initial theme
    this.applyTheme(this.currentTheme)

    // Add event listeners to theme buttons
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.addEventListener('click', (e) => {
        // Find the button element (in case target is the img inside)
        const buttonElement = e.target.closest('button')
        const theme = buttonElement.dataset.theme
        this.setTheme(theme)
      })
    })

    // Listen for system theme changes
    if (window.matchMedia) {
      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
        if (this.currentTheme === 'system') {
          this.applyTheme('system')
        }
      })
    }

    // Set initial active button
    this.updateActiveButton()
  }

  setTheme(theme) {
    this.currentTheme = theme
    localStorage.setItem('theme', theme)
    this.applyTheme(theme)
    this.updateActiveButton()
  }

  applyTheme(theme) {
    const html = document.documentElement

    // Remove existing theme classes
    html.classList.remove('theme-light', 'theme-dark')

    if (theme === 'light') {
      html.classList.add('theme-light')
      html.style.colorScheme = 'light'
    } else if (theme === 'dark') {
      html.classList.add('theme-dark')
      html.style.colorScheme = 'dark'
    } else { // system
      // Let CSS color-scheme handle it
      html.style.colorScheme = 'light dark'
    }
  }

  updateActiveButton() {
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.classList.remove('active')
    })

    const activeButton = document.querySelector(`#theme-switcher button[data-theme="${this.currentTheme}"]`)
    if (activeButton) {
      activeButton.classList.add('active')
    }
  }
}

// Initialize theme immediately to prevent flash
(function() {
  const savedTheme = localStorage.getItem('theme') || 'system'
  const html = document.documentElement

  if (savedTheme === 'light') {
    html.classList.add('theme-light')
    html.style.colorScheme = 'light'
  } else if (savedTheme === 'dark') {
    html.classList.add('theme-dark')
    html.style.colorScheme = 'dark'
  } else {
    html.style.colorScheme = 'light dark'
  }
})()

// Initialize theme switcher when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  new ThemeSwitcher()
})
