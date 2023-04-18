import * as HTTP from './http.js'

class QueryState {
  constructor() {
    this._state = {
      page: 1,
      page_size: 100,
      sort: '',
      sort_reverse: false,
      name: ''
    }
  }

  get name() { return this._state.name }
  set name(value) { this._state.name = value }
  get page() { return this._state.page }
  set page(value) { this._state.page = parseInt(value) }
  get page_size() { return this._state.page_size }
  set page_size(value) { this._state.page_size = parseInt(value) }
  get sort() { return this._state.sort }
  set sort(value) { this._state.sort = value }
  get sort_reverse() { return this._state.sort_reverse }
  set sort_reverse(value) {
    if (typeof value === 'boolean') {
      this._state.sort_reverse = value
    } else {
      this._state.sort_reverse = (value === 'true' || value === 1)
    }
  }

  update(values) {
    Object.getOwnPropertyNames(values).forEach(key => {
      if (Object.hasOwn(this._state, key)) {
        this[key] = values[key]
      }
    })
  }

  toJSON() {
    return this._state
  }
}

class DataSource {
  static DEFAULT_STATE = {
    page: 1,
    page_size: 100,
    sort: '',
    sort_reverse: false,
    name: ''
  }
  constructor(opts) {
    this._opts = Object.assign(
      {
        autoReloadTimeout: 5000,
        useQueryState: true
      }, opts)
    this._reloadTimer = null
    this._events = new EventTarget()
    this._items = []
    this._filteredCount = 0
    this._itemCount = 0
    this._pageCount = 0
    this._totalCount = 0
    this._setState()
    let cachedState
    this._lastLoadedUrl = ''
    if (this._opts.useQueryState) {
      if (cachedState = window.sessionStorage.getItem(this._cacheKey)) {
        try {
          cachedState = JSON.parse(cachedState)
          this._setState(cachedState)
        } catch(e) {
          console.error(`Failed to load cached query state: ${e}`, cachedState)
        }
      }
      this._setStateFromHash()
      window.addEventListener('hashchange', evt => {
        this._setStateFromHash()
        this.reload({updateState: false})
      })
    }
  }

  _setStateFromHash() {
    const urlParams = Object.fromEntries(new URLSearchParams(window.location.hash.substring(1)).entries())
    this._setState(urlParams)
  }

  _setState(properties = {}) {
    this._queryState = new QueryState()
    this._queryState.update(properties)
  }

  get page() { return this._queryState.page }
  set page(value) { this._queryState.page = value }
  get pageSize() { return this._queryState.page_size }
  set pageSize(value) { this._queryState.page_size = value }
  set sortKey(value) { this._queryState.sort = value }
  get sortKey() { return this._queryState.sort }
  set reverseOrder(value) {  this._queryState.sort_reverse = value }
  get reverseOrder() { return this._queryState.sort_reverse }
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
      this._queryState.page_size = data.page_size
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

  reload(args) {
    clearTimeout(this._reloadTimer)
    return this._reload(args).then(resp => {
      this._enqueueReload()
      this.items = resp
      return resp
    }).catch(err => {
      this._enqueueReload()
      if (err.message) {
        this.emit('error', err.message)
      } else {
        this.emit('error', err)
      }
    })
  }

  reset() {
    this._setState()
  }

  _enqueueReload() {
    if (this._opts.autoReloadTimeout > 0) {
      clearTimeout(this._reloadTimer)
      this._reloadTimer = setTimeout(this.reload.bind(this), this._opts.autoReloadTimeout)
    }
  }

  emit(eventName, args) {
    this._events.dispatchEvent(new CustomEvent(eventName, { detail: args }))
  }

  on(eventName, listener) {
    this._events.addEventListener(eventName, listener)
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

  _fullApiUrl() {
    let url = new URL(this.url)
    url.search = this.queryParams()
    return url
  }

  _reload(opts = {}) {
    const url = this._fullApiUrl()
    if (this._opts.useQueryState) {
      if (opts.updateState !== false) {
        const documentUrl = new URL(window.location)
        const query = new URLSearchParams(documentUrl.hash.substring(1))
        this.queryParams(query)
        documentUrl.hash = query
        if (window.location.hash.length <= 1 || this._lastLoadedUrl == '') {
          window.history.replaceState(null, '', documentUrl)
        } else if (this._lastLoadedUrl != url.toString()) {
          window.history.pushState(null, '', documentUrl)
        }
      }
      window.sessionStorage.setItem(this._cacheKey, JSON.stringify(this._queryState))
    }
    this._lastLoadedUrl = url.toString()
    return HTTP.request('GET', url)
  }
}

export { DataSource, UrlDataSource }
