import * as Auth from './auth.js';
import * as Helpers from './helpers.js';
const usernameEl = document.getElementById('username');
if (usernameEl) {
    usernameEl.textContent = Auth.getUsername() ?? '';
}
const menuButton = document.getElementById('menu-button');
const menuContent = document.getElementById('menu-content');
if (menuButton && menuContent) {
    menuButton.addEventListener('click', () => {
        menuButton.classList.toggle('open-menu');
        menuContent.classList.toggle('show-menu');
    });
}
Helpers.addVhostOptions('user-vhost', { addAll: true }).then(() => {
    const vhost = window.sessionStorage.getItem('vhost');
    if (vhost) {
        const opt = document.querySelector('#userMenuVhost option[value="' + vhost + '"]');
        if (opt) {
            const select = document.querySelector('#userMenuVhost');
            if (select) {
                select.value = vhost;
                window.sessionStorage.setItem('vhost', vhost);
            }
        }
    }
    else {
        window.sessionStorage.setItem('vhost', '_all');
    }
});
const userMenuVhost = document.getElementById('userMenuVhost');
if (userMenuVhost) {
    userMenuVhost.addEventListener('change', (e) => {
        const target = e.target;
        window.sessionStorage.setItem('vhost', target.value);
        window.location.reload();
    });
}
const signoutLink = document.getElementById('signoutLink');
if (signoutLink) {
    signoutLink.addEventListener('click', () => {
        document.cookie = 'm=; max-age=0';
        window.location.assign('login');
    });
}
const usermenuButton = document.getElementById('usermenu-button');
const usermenuContent = document.getElementById('user-menu');
if (usermenuButton && usermenuContent) {
    usermenuButton.addEventListener('click', () => {
        usermenuButton.classList.toggle('open-menu');
        usermenuContent.classList.toggle('visible');
    });
}
class ThemeSwitcher {
    currentTheme;
    constructor() {
        this.currentTheme = window.localStorage.getItem('theme') || 'system';
        this.init();
    }
    init() {
        // Set initial theme
        this.applyTheme(this.currentTheme);
        // Add event listeners to theme buttons
        document.querySelectorAll('#theme-switcher button').forEach((button) => {
            button.addEventListener('click', (e) => {
                // Find the button element (in case target is the img inside)
                const buttonElement = e.target.closest('button');
                if (buttonElement) {
                    const theme = buttonElement.dataset['theme'];
                    this.setTheme(theme);
                }
            });
        });
        // Listen for system theme changes
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
                if (this.currentTheme === 'system') {
                    this.applyTheme('system');
                }
            });
        }
        // Set initial active button
        this.updateActiveButton();
    }
    setTheme(theme) {
        this.currentTheme = theme;
        window.localStorage.setItem('theme', theme);
        this.applyTheme(theme);
        this.updateActiveButton();
    }
    applyTheme(theme) {
        const html = document.documentElement;
        // Remove existing theme classes
        html.classList.remove('theme-light', 'theme-dark');
        if (theme === 'light') {
            html.classList.add('theme-light');
            html.style.colorScheme = 'light';
        }
        else if (theme === 'dark') {
            html.classList.add('theme-dark');
            html.style.colorScheme = 'dark';
        }
        else {
            // system
            html.style.colorScheme = 'light dark';
        }
    }
    updateActiveButton() {
        document.querySelectorAll('#theme-switcher button').forEach((button) => {
            button.classList.remove('active');
        });
        const activeButton = document.querySelector(`#theme-switcher button[data-theme="${this.currentTheme}"]`);
        if (activeButton) {
            activeButton.classList.add('active');
        }
    }
}
// Initialize theme immediately to prevent flash
;
(function () {
    const savedTheme = window.localStorage.getItem('theme') || 'system';
    const html = document.documentElement;
    if (savedTheme === 'light') {
        html.classList.add('theme-light');
        html.style.colorScheme = 'light';
    }
    else if (savedTheme === 'dark') {
        html.classList.add('theme-dark');
        html.style.colorScheme = 'dark';
    }
    else {
        html.style.colorScheme = 'light dark';
    }
})();
// Initialize theme switcher when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Store theme switcher instance on window for debugging
    ;
    window.themeSwitcher = new ThemeSwitcher();
});
//# sourceMappingURL=layout.js.map