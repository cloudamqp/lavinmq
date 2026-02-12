import * as Auth from './auth.js'
import * as Helpers from './helpers.js'

Auth.whoAmI() // Ensure we have whoami data.

document.getElementById('username').textContent = Auth.getUsername()

const menuButton = document.getElementById('menu-button')
const menuContent = document.getElementById('menu-content')

menuButton.addEventListener('click', (e) => {
  menuButton.classList.toggle('open-menu')
  menuContent.classList.toggle('show-menu')
})

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

document.getElementById('userMenuVhost').addEventListener('change', (e) => {
  window.sessionStorage.setItem('vhost', e.target.value)
  window.location.reload()
})

document.getElementById('signoutLink').addEventListener('click', () => {
  Auth.logout()
  window.location.assign('login')
})

const usermenuButton = document.getElementById('usermenu-button')
const usermenuContent = document.getElementById('user-menu')

usermenuButton.addEventListener('click', (e) => {
  usermenuButton.classList.toggle('open-menu')
  usermenuContent.classList.toggle('visible')
})

// Theme switcher functionality
class ThemeSwitcher {
  constructor () {
    this.currentTheme = 'system'
    if (Helpers.stateClasses.has('theme-light')) {
      this.currentTheme = 'light'
    } else if (Helpers.stateClasses.has('theme-dark')) {
      this.currentTheme = 'dark'
    }
    this.init()
  }

  init () {
    // Add event listeners to theme buttons
    document.querySelectorAll('#theme-switcher button').forEach(button => {
      button.addEventListener('click', _ => this.setTheme(button.dataset.theme))
    })

    // Set initial active button
    this.updateActiveButton()
  }

  setTheme (theme) {
    this.currentTheme = theme
    this.applyTheme(theme)
    this.updateActiveButton()
  }

  applyTheme (theme) {
    if (theme === 'light') {
      Helpers.stateClasses.add('theme-light')
      Helpers.stateClasses.remove('theme-dark')
    } else if (theme === 'dark') {
      Helpers.stateClasses.add('theme-dark')
      Helpers.stateClasses.remove('theme-light')
    } else { // system
      Helpers.stateClasses.remove(/^theme-/)
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

// Initialize theme switcher when DOM is loaded
window.themeSwitcher = new ThemeSwitcher()

// Check if sidebar is collapsed or expanded
document.addEventListener('DOMContentLoaded', () => {
  const sidebarCollapsed = Helpers.stateClasses.has('menu-collapsed')
  const toggleLabel = document.querySelector('.toggle-menu-label')

  if (sidebarCollapsed) {
    toggleLabel.textContent = 'Expand sidebar'
  }
})

document.getElementById('toggle-menu').addEventListener('click', () => {
  const added = Helpers.stateClasses.toggle('menu-collapsed')
  const toggleLabel = document.querySelector('.toggle-menu-label')

  // Save state
  if (added) {
    toggleLabel.textContent = 'Expand sidebar'
    updateMenuTooltips()
  } else {
    toggleLabel.textContent = 'Collapse sidebar'
  }
})

const sidebarMenu = document.getElementById('menu')
const menuItems = document.querySelectorAll('#menu-content li a.menu-tooltip')

function updateMenuTooltips () {
  menuItems.forEach(item => {
    const tooltip = item.querySelector('.menu-tooltip-label')
    if (tooltip) {
      const rect = item.getBoundingClientRect()
      tooltip.style.top = rect.top + (rect.height / 2) + 'px'
    }
  })
}

// Update tooltip positions on scroll
let ticking = false

sidebarMenu.addEventListener('scroll', (e) => {
  if (!ticking) {
    window.requestAnimationFrame(() => {
      updateMenuTooltips()
      ticking = false
    })
    ticking = true
  }
})

updateMenuTooltips()
