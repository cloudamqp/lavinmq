import * as HTTP from './http.js'

interface QueryStateData {
  page: number
  page_size: number
  sort: string
  sort_reverse: boolean
  name: string
}

class QueryState {
  private _state: QueryStateData

  constructor() {
    this._state = {
      page: 1,
      page_size: 100,
      sort: '',
      sort_reverse: false,
      name: '',
    }
  }

  get name(): string {
    return this._state.name
  }
  set name(value: string) {
    this._state.name = value
  }
  get page(): number {
    return this._state.page
  }
  set page(value: number | string) {
    this._state.page = typeof value === 'string' ? parseInt(value, 10) : value
  }
  get page_size(): number {
    return this._state.page_size
  }
  set page_size(value: number | string) {
    this._state.page_size = typeof value === 'string' ? parseInt(value, 10) : value
  }
  get sort(): string {
    return this._state.sort
  }
  set sort(value: string) {
    this._state.sort = value
  }
  get sort_reverse(): boolean {
    return this._state.sort_reverse
  }
  set sort_reverse(value: boolean | string | number) {
    if (typeof value === 'boolean') {
      this._state.sort_reverse = value
    } else {
      this._state.sort_reverse = value === 'true' || value === 1
    }
  }

  update(values: Partial<QueryStateData>): void {
    Object.getOwnPropertyNames(values).forEach((key) => {
      if (Object.hasOwn(this._state, key)) {
        const k = key as keyof QueryStateData
        ;(this as unknown as Record<string, unknown>)[k] = values[k]
      }
    })
  }

  toJSON(): QueryStateData {
    return this._state
  }
}

export interface PaginatedResponse<T> {
  items: T[]
  page: number
  page_size: number
  filtered_count: number
  item_count: number
  page_count: number
  total_count: number
}

export interface DataSourceOptions {
  autoReloadTimeout?: number
  useQueryState?: boolean
}

interface ReloadOptions {
  updateState?: boolean
}

export class DataSource<T = unknown> {
  static DEFAULT_STATE: QueryStateData = {
    page: 1,
    page_size: 100,
    sort: '',
    sort_reverse: false,
    name: '',
  }

  protected _opts: Required<DataSourceOptions>
  protected _reloadTimer: ReturnType<typeof setTimeout> | null = null
  protected _events: EventTarget = new EventTarget()
  protected _items: T[] = []
  protected _filteredCount = 0
  protected _itemCount = 0
  protected _pageCount = 0
  protected _totalCount = 0
  protected _queryState!: QueryState
  protected _lastLoadedUrl = ''

  constructor(opts?: DataSourceOptions) {
    this._opts = Object.assign(
      {
        autoReloadTimeout: 5000,
        useQueryState: true,
      },
      opts
    )
    this._setState()
    if (this._opts.useQueryState) {
      const cachedState = window.sessionStorage.getItem(this._cacheKey)
      if (cachedState) {
        try {
          const parsed = JSON.parse(cachedState) as Partial<QueryStateData>
          this._setState(parsed)
        } catch (e) {
          console.error(`Failed to load cached query state: ${e}`, cachedState)
        }
      }
      this._setStateFromHash()
      window.addEventListener('hashchange', () => {
        this._setStateFromHash()
        this.reload({ updateState: false })
      })
    }
  }

  private _setStateFromHash(): void {
    if (window.location.hash.length > 0) {
      const urlParams = Object.fromEntries(
        new URLSearchParams(window.location.hash.substring(1)).entries()
      ) as Partial<QueryStateData>
      this._setState(urlParams)
    }
  }

  protected _setState(properties: Partial<QueryStateData> = {}): void {
    this._queryState = new QueryState()
    this._queryState.update(properties)
  }

  get page(): number {
    return this._queryState.page
  }
  set page(value: number) {
    this._queryState.page = value
  }
  get pageSize(): number {
    return this._queryState.page_size
  }
  set pageSize(value: number) {
    this._queryState.page_size = value
  }
  set sortKey(value: string) {
    this._queryState.sort = value
  }
  get sortKey(): string {
    return this._queryState.sort
  }
  set reverseOrder(value: boolean) {
    this._queryState.sort_reverse = value
  }
  get reverseOrder(): boolean {
    return this._queryState.sort_reverse
  }
  set searchTerm(value: string) {
    this._queryState.name = value
  }
  get searchTerm(): string {
    return this._queryState.name
  }

  get filteredCount(): number {
    return this._filteredCount
  }
  get itemCount(): number {
    return this._itemCount
  }
  get pageCount(): number {
    return this._pageCount
  }
  get totalCount(): number {
    return this._totalCount
  }
  get items(): T[] {
    return this._items
  }

  set items(data: PaginatedResponse<T> | T[]) {
    if ('items' in data) {
      this._queryState.page = data.page
      this._queryState.page_size = data.page_size
      this._items = data.items
      this._filteredCount = data.filtered_count
      this._itemCount = data.item_count
      this._pageCount = data.page_count
      this._totalCount = data.total_count

      // If current page is out of bounds, redirect to the last valid page
      if (this._queryState.page > this._pageCount) {
        const targetPage = this._pageCount === 0 ? 1 : this._pageCount
        // Only reload if we're not already on the target page
        if (this._queryState.page !== targetPage) {
          this._queryState.page = targetPage
          this.reload({ updateState: true })
          return
        }
      }
    } else {
      this._items = data
      this._filteredCount = this.items.length
      this._totalCount = this.items.length
      this._pageCount = 1
    }
    this.emit('update')
  }

  reload(args?: ReloadOptions): Promise<PaginatedResponse<T> | T[] | void> {
    if (this._reloadTimer) clearTimeout(this._reloadTimer)
    return this._reload(args)
      .then((resp) => {
        this._enqueueReload()
        this.items = resp
        return resp
      })
      .catch((err: { status?: number; message?: string }) => {
        this._enqueueReload()
        if (err.status === 401) {
          return
        }
        if (err.message) {
          this.emit('error', err.message)
        } else {
          this.emit('error', err)
        }
      })
  }

  reset(): void {
    this._setState()
  }

  protected _reload(_opts?: ReloadOptions): Promise<PaginatedResponse<T> | T[]> {
    return Promise.resolve([] as T[])
  }

  protected _enqueueReload(): void {
    if (this._opts.autoReloadTimeout > 0) {
      if (this._reloadTimer) clearTimeout(this._reloadTimer)
      this._reloadTimer = setTimeout(this.reload.bind(this), this._opts.autoReloadTimeout)
    }
  }

  emit(eventName: string, args?: unknown): void {
    this._events.dispatchEvent(new CustomEvent(eventName, { detail: args }))
  }

  on(eventName: string, listener: EventListenerOrEventListenerObject): void {
    this._events.addEventListener(eventName, listener)
  }

  queryParams(params?: URLSearchParams): URLSearchParams {
    const p = params ?? new URLSearchParams()
    if (this.page > 0) {
      p.set('page', String(this.page))
      if (this.pageSize > 0) {
        p.set('page_size', String(this.pageSize))
      }
    } else {
      p.delete('page')
      p.delete('page_size')
    }
    if (this.searchTerm && this.searchTerm.length > 0) {
      p.set('use_regex', 'true')
      p.set('name', this.searchTerm)
    } else {
      p.delete('use_regex')
      p.delete('name')
    }
    if (this.sortKey && this.sortKey !== '') {
      p.set('sort', this.sortKey)
      p.set('sort_reverse', String(this.reverseOrder))
    } else {
      p.delete('sort')
      p.delete('sort_reverse')
    }
    p.sort()
    return p
  }

  protected get _cacheKey(): string {
    return `${window.location.pathname.split('/').pop()}-queryState`
  }
}

export class UrlDataSource<T = unknown> extends DataSource<T> {
  url: URL

  constructor(url: string, opts?: DataSourceOptions) {
    super(opts)
    if (url.startsWith('http')) {
      this.url = new URL(url)
    } else {
      this.url = new URL(url, window.location.href)
    }
  }

  private _fullApiUrl(): URL {
    const url = new URL(this.url)
    url.search = this.queryParams().toString()
    return url
  }

  protected override _reload(opts: ReloadOptions = {}): Promise<PaginatedResponse<T> | T[]> {
    const url = this._fullApiUrl()
    if (this._opts.useQueryState) {
      if (opts.updateState !== false) {
        const documentUrl = new URL(window.location.href)
        const query = new URLSearchParams(documentUrl.hash.substring(1))
        this.queryParams(query)
        documentUrl.hash = query.toString()
        if (window.location.hash.length <= 1 || this._lastLoadedUrl === '') {
          window.history.replaceState(null, '', documentUrl)
        } else if (this._lastLoadedUrl !== url.toString()) {
          window.history.pushState(null, '', documentUrl)
        }
      }
      window.sessionStorage.setItem(this._cacheKey, JSON.stringify(this._queryState))
    }
    this._lastLoadedUrl = url.toString()
    return HTTP.request<PaginatedResponse<T> | T[]>('GET', url).then((resp) => resp ?? [])
  }
}
