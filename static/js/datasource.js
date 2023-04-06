import * as HTTP from './http.js'
import EventEmitter from './eventemitter.js'

class DataSource {
  constructor(opts) {
    this._opts = Object.assign(
      {
        autoReloadTimeout: 5000,
        useQueryState: true
      }, opts)
    this._reloadTimer = null
    this._events = new EventEmitter()
    this._items = []
    this._filteredCount = 0
    this._itemCount = 0
    this._pageCount = 0
    this._totalCount = 0
    this._queryState = {
      page: 1,
      pageSize: 100,
      sortKey: '',
      reverseOrder: false,
      name: ''
    }
    let cachedState
    if (cachedState = window.sessionStorage.getItem(this._cacheKey)) {
      try {
        cachedState = JSON.parse(cachedState)
        this._queryState = cachedState
      } catch(e) {
        console.error(`Failed to load cached query state: ${e}`)
      }
    }
    this._lastLoadedUrl = ''
    if (this._opts.useQueryState) {
      const urlParams = new URLSearchParams(window.location.search)
      urlParams.has('name') && (this._queryState.name = urlParams.get('name'))
      urlParams.has('page') && (this._queryState.page = parseInt(urlParams.get('page')))
      urlParams.has('page_size') && (this._queryState.pageSize = parseInt(urlParams.get('page_size')))
      urlParams.has('sort_reverse') && (this._queryState.reverseOrder = (urlParams.get('sort_reverse') === 'true'))
      urlParams.has('sort_key') && (this._queryState.sortKey = urlParams.get('sort_key'))
      window.addEventListener('popstate', evt => {
        if (evt.state) {
          this._queryState = evt.state
          this.reload()
        }
      })
    }
  }

  _setStateFromQuery() {
    const params = new URLSearchParams(window.location.search)
  }

  get page() { return this._queryState.page }
  set page(value) { this._queryState.page = parseInt(value) }
  get pageSize() { return this._queryState.pageSize }
  set pageSize(value) { this._queryState.pageSize = parseInt(value) }
  set sortKey(value) { this._queryState.sortKey = value }
  get sortKey() { return this._queryState.sortKey }
  set reverseOrder(value) { 
    if (typeof value === 'boolean') {
      this._queryState.reverseOrder = value
    } else {
      this._queryState.reverseOrder = value === 'true' || value === 1
    }
  }
  get reverseOrder() { return this._queryState.reverseOrder }
  set searchTerm(value) { this._queryState.name = value }
  get searchTerm() { return this._queryState.name }

  get filteredCount() { return this._filteredCount }
  get itemCount() { return this._itemCount }
  get pageCount() { return this._pageCount }
  get totalCount() { return this._totalCount }
  get items() { return this._items }

  set items(data) {
    if ('items' in data) {
      this._queryState.page = data.page
      this._queryState.pageSize = data.page_size
      this._items = data.items
      this._filteredCount = data.filtered_count
      this._itemCount = data.item_count
      this._pageCount = data.page_count
      this._totalCount = data.total_count
    } else {
      this._items = data
      this._filteredCount = this.items.length
      this._totalCount = this.items.length
      this._pageCount = 1
    }
    this.emit('update')
  }

  queryParams() {
    throw "Not implemented"
  }

  reload() {
    clearTimeout(this._reloadTimer)
    this._reload().then(_ => {
      if (this._opts.autoReloadTimeout === 0) {
        return
      }
      clearTimeout(this._reloadTimer)
      this._reloadTimer = setTimeout(this.reload.bind(this), this._opts.autoReloadTimeout)
    })
  }

  stopAutoReload() {
    clearTimeout(this._reloadTimer)
  }

  emit(eventName, ...args) {
    this._events.emit(eventName, ...args)
  }

  on(eventName, listener) {
    this._events.on(eventName, listener)
  }

  queryParams(params) {
    params ??= new URLSearchParams()
    if (this.page > 0) {
      params.set('page', this.page)
      if (this.pageSize > 0) {
        params.set('page_size', this.pageSize)
      }
    } else {
      params.delete('page')
      params.delete('page_size')
    }
    if (this.searchTerm && this.searchTerm.length > 0) {
      params.set('use_regex', 'true')
      params.set('name', this.searchTerm)
    } else {
      params.delete('use_regex')
      params.delete('name')
    }
    if (this.sortKey && this.sortKey !== '') {
      params.set('sort', this.sortKey)
      params.set('sort_reverse', this.reverseOrder)
    } else {
      params.delete('sort')
      params.delete('sort_reverse')
    }
    params.sort()
    return params
  }

  get _cacheKey() {
    return `${window.location.pathname.split('/').pop()}-queryState`
  }

}

class UrlDataSource extends DataSource {
  constructor(url, opts) {
    super(opts)
    if (url.startsWith('http')) {
      this.url = new URL(url)
    } else {
      this.url = new URL(url, window.location)
    }
  }

  _fullUrl() {
    let url = new URL(this.url)
    url.search = this.queryParams()
    return url
  }

  _reload() {
    const url = this._fullUrl()
    if (this._opts.useQueryState) {
      const documentUrl = new URL(window.location)
      this.queryParams(documentUrl.searchParams)
      if (window.location.search.length == 0 || this._lastLoadedUrl == '') {
        window.history.replaceState(this._queryState, '', documentUrl)
      } else if (this._lastLoadedUrl != url.toString()) {
        window.history.pushState(this._queryState, '', documentUrl)
      }
    }
    window.sessionStorage.setItem(this._cacheKey, JSON.stringify(this._queryState))
    this._lastLoadedUrl = url.toString()
    return HTTP.request('GET', url).then(data => {
      this.items = data
    }).catch(err => {
      console.error(err)
      this.stopAutoReload()
      this.emit('error', err)
    })
  }
}

export { DataSource, UrlDataSource }
