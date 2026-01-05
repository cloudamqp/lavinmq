import * as Auth from './auth.js'
import * as Helpers from './helpers.js'
import * as HTTP from './http.js'

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

class EntitySearch {
  #searchField
  #resultList
  #ws
  #selectedIndex = -1

  constructor (search) {
    this.#searchField = search.querySelector('input[type=search]')
    this.#resultList = search.querySelector('ul')
    this.#searchField.value = ''

    this.#searchField.addEventListener('keydown', (e) => {
      this.#handleKeydown(e)
    })

    // Remove the listener once init has been called
    const initWsCb = _ => {
      this.#searchField.removeEventListener('input', initWsCb)
      this.#initWs()
    }
    this.#searchField.addEventListener('input', initWsCb)
  }

  #initWs () {
    console.log('init ws')
    this.#ws = new WebSocket('api/entity-search')
    const searchCb = _ => { this.#search() }
    // We remove and disable search on error/close
    this.#ws.addEventListener('error', e => {
      console.error('WS Error', e)
      this.#ws.close
    })
    this.#ws.addEventListener('close', _ => {
      console.warn('WS Closed')
      this.#searchField.value = ''
      this.#searchField.placeholder = 'disabled due to webocket error'
      this.#searchField.disabled = true
      this.#searchField.removeEventListener('input', searchCb)
    })
    // Dont add event listeners until we have a connection
    this.#ws.addEventListener('open', _ => {
      this.#searchField.addEventListener('input', searchCb)
      this.#search()
    })
    this.#ws.addEventListener('message', async e => {
      const response = JSON.parse(await event.data.text())
      this.#showSearchResult(response)
    })
  }

  #searchTimer = null
  #search () {
    clearTimeout(this.#searchTimer)
    this.#searchTimer = setTimeout(function () {
      const value = this.#searchField.value
      if (value.length == 0) {
        this.#resultList.innerHTML = ''
        return
      }
      console.debug('search for', value)
      this.#ws.send(value)
    }.bind(this), 250)
  }

  #handleKeydown (e) {
    const items = this.#resultList.querySelectorAll('li')

    if (items.length === 0) return

    if (e.key === 'ArrowDown') {
      e.preventDefault()
      this.#selectedIndex = Math.min(this.#selectedIndex + 1, items.length - 1)
      this.#updateSelection(items)
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      this.#selectedIndex = Math.max(this.#selectedIndex - 1, -1)
      this.#updateSelection(items)
    } else if (e.key === 'Enter' && this.#selectedIndex >= 0) {
      e.preventDefault()
      const selectedLink = items[this.#selectedIndex]?.querySelector('a')
      if (selectedLink) {
        selectedLink.click()
      }
    }
  }

  #updateSelection (items) {
    items.forEach((item, index) => {
      if (index === this.#selectedIndex) {
        item.classList.add('selected')
      } else {
        item.classList.remove('selected')
      }
    })
  }

  #showSearchResult (response) {
    this.#selectedIndex = -1
    const fragment = document.createDocumentFragment()

    const link = {
      queue: r => HTTP.url`queue#vhost=${r.vhost}&name=${r.name}`,
      exchange: r => HTTP.url`exchange#vhost=${r.vhost}&name=${r.name}`,
      user: r => HTTP.url`user#name=${r.name}`,
      vhost: r => HTTP.url`vhost#name=${r.name}`
    }

    response.result.forEach(result => {
      const li = document.createElement('li')
      const a = document.createElement('a')
      a.href = link[result.type](result)
      a.className = 'search-result-item'

      const nameSpan = document.createElement('span')
      nameSpan.className = 'entity-name'
      nameSpan.textContent = result.name

      const metaRow = document.createElement('span')
      metaRow.className = 'entity-meta'

      const vhostSpan = document.createElement('span')
      vhostSpan.className = 'entity-vhost'
      if (result.vhost) {
        vhostSpan.textContent = `vhost: ${result.vhost}`
      }

      const typeSpan = document.createElement('span')
      typeSpan.className = 'entity-type'
      typeSpan.textContent = result.type

      metaRow.appendChild(vhostSpan)
      metaRow.appendChild(typeSpan)

      a.appendChild(nameSpan)
      a.appendChild(metaRow)
      li.appendChild(a)
      fragment.appendChild(li)
    })

    this.#resultList.innerHTML = ''
    this.#resultList.appendChild(fragment)
  }
}

// Initialize theme switcher when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  // Store theme switcher instance on window for debugging
  window.themeSwitcher = new ThemeSwitcher()
  const searchField = document.querySelector('#entity-search')
  if (searchField) {
    window.entitySearch = new EntitySearch(searchField)
  }
})
