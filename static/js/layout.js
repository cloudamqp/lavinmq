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

// Initialize theme immediately to prevent flash
(function () {
  const savedTheme = window.localStorage.getItem('theme') || 'system'
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
  // Store theme switcher instance on window for debugging
  window.themeSwitcher = new ThemeSwitcher()
})

document.getElementById("toggle-menu").addEventListener("click", () => {
  document.documentElement.classList.toggle("menu-collapsed");

  // Save state
  if (document.documentElement.classList.contains("menu-collapsed")) {
    localStorage.setItem("menuCollapsed", "true");
  } else {
    localStorage.removeItem("menuCollapsed");
  }
});

const sidebar = document.getElementById('menu-content');
const menuItems = document.querySelectorAll('#menu-content li a.menu-tooltip');
console.log(menuItems);

// Position tooltips dynamically
menuItems.forEach(item => {
  const tooltip = item.querySelector('.menu-tooltip-label');
  
  item.addEventListener('mouseenter', function() {
    if (document.documentElement.classList.contains('menu-collapsed')) {
      const rect = this.getBoundingClientRect();
      tooltip.style.left = rect.right + 10 + 'px';
      tooltip.style.top = rect.top + (rect.height / 2) + 'px';
      tooltip.style.transform = 'translateY(-50%)';
    }
  });
});

// Update tooltip positions on scroll
const sidebarMenu = document.getElementById('menu');
sidebarMenu.addEventListener('scroll', function() {
  menuItems.forEach(item => {
    const tooltip = item.querySelector('.menu-tooltip-label');
    const rect = item.getBoundingClientRect();
    tooltip.style.top = rect.top + (rect.height / 2) + 'px';
  });
});
