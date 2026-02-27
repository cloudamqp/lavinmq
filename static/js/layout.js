import * as Auth from './auth.js'
import * as Helpers from './helpers.js'

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
  document.cookie = 'm=; max-age=0'
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
    this.currentTheme = window.localStorage.getItem('theme') || 'system'
    this.init()
  }

  init () {
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

  setTheme (theme) {
    this.currentTheme = theme
    window.localStorage.setItem('theme', theme)
    this.applyTheme(theme)
    this.updateActiveButton()
  }

  applyTheme (theme) {
    // Uses the global applyTheme() defined in partials/theme.ecr
    applyTheme(theme)
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

// Check if sidebar is collapsed or expanded
document.addEventListener('DOMContentLoaded', () => {
  const sidebarCollapsed = window.localStorage.getItem('menuCollapsed')
  const toggleLabel = document.querySelector('.toggle-menu-label')

  if (sidebarCollapsed) {
    toggleLabel.textContent = 'Expand sidebar'
  }
})

// Initialize theme switcher when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  // Store theme switcher instance on window for debugging
  window.themeSwitcher = new ThemeSwitcher()
})

document.getElementById('toggle-menu').addEventListener('click', () => {
  document.documentElement.classList.toggle('menu-collapsed')
  const toggleLabel = document.querySelector('.toggle-menu-label')

  // Save state
  if (document.documentElement.classList.contains('menu-collapsed')) {
    window.localStorage.setItem('menuCollapsed', 'true')
    toggleLabel.textContent = 'Expand sidebar'
    updateMenuTooltips()
  } else {
    window.localStorage.removeItem('menuCollapsed')
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
