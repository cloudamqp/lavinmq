import * as HTTP from './http.js';
class QueryState {
    _state;
    constructor() {
        this._state = {
            page: 1,
            page_size: 100,
            sort: '',
            sort_reverse: false,
            name: '',
        };
    }
    get name() {
        return this._state.name;
    }
    set name(value) {
        this._state.name = value;
    }
    get page() {
        return this._state.page;
    }
    set page(value) {
        this._state.page = typeof value === 'string' ? parseInt(value, 10) : value;
    }
    get page_size() {
        return this._state.page_size;
    }
    set page_size(value) {
        this._state.page_size = typeof value === 'string' ? parseInt(value, 10) : value;
    }
    get sort() {
        return this._state.sort;
    }
    set sort(value) {
        this._state.sort = value;
    }
    get sort_reverse() {
        return this._state.sort_reverse;
    }
    set sort_reverse(value) {
        if (typeof value === 'boolean') {
            this._state.sort_reverse = value;
        }
        else {
            this._state.sort_reverse = value === 'true' || value === 1;
        }
    }
    update(values) {
        Object.getOwnPropertyNames(values).forEach((key) => {
            if (Object.hasOwn(this._state, key)) {
                const k = key;
                this[k] = values[k];
            }
        });
    }
    toJSON() {
        return this._state;
    }
}
export class DataSource {
    static DEFAULT_STATE = {
        page: 1,
        page_size: 100,
        sort: '',
        sort_reverse: false,
        name: '',
    };
    _opts;
    _reloadTimer = null;
    _events = new EventTarget();
    _items = [];
    _filteredCount = 0;
    _itemCount = 0;
    _pageCount = 0;
    _totalCount = 0;
    _queryState;
    _lastLoadedUrl = '';
    constructor(opts) {
        this._opts = Object.assign({
            autoReloadTimeout: 5000,
            useQueryState: true,
        }, opts);
        this._setState();
        if (this._opts.useQueryState) {
            const cachedState = window.sessionStorage.getItem(this._cacheKey);
            if (cachedState) {
                try {
                    const parsed = JSON.parse(cachedState);
                    this._setState(parsed);
                }
                catch (e) {
                    console.error(`Failed to load cached query state: ${e}`, cachedState);
                }
            }
            this._setStateFromHash();
            window.addEventListener('hashchange', () => {
                this._setStateFromHash();
                this.reload({ updateState: false });
            });
        }
    }
    _setStateFromHash() {
        if (window.location.hash.length > 0) {
            const urlParams = Object.fromEntries(new URLSearchParams(window.location.hash.substring(1)).entries());
            this._setState(urlParams);
        }
    }
    _setState(properties = {}) {
        this._queryState = new QueryState();
        this._queryState.update(properties);
    }
    get page() {
        return this._queryState.page;
    }
    set page(value) {
        this._queryState.page = value;
    }
    get pageSize() {
        return this._queryState.page_size;
    }
    set pageSize(value) {
        this._queryState.page_size = value;
    }
    set sortKey(value) {
        this._queryState.sort = value;
    }
    get sortKey() {
        return this._queryState.sort;
    }
    set reverseOrder(value) {
        this._queryState.sort_reverse = value;
    }
    get reverseOrder() {
        return this._queryState.sort_reverse;
    }
    set searchTerm(value) {
        this._queryState.name = value;
    }
    get searchTerm() {
        return this._queryState.name;
    }
    get filteredCount() {
        return this._filteredCount;
    }
    get itemCount() {
        return this._itemCount;
    }
    get pageCount() {
        return this._pageCount;
    }
    get totalCount() {
        return this._totalCount;
    }
    get items() {
        return this._items;
    }
    set items(data) {
        if ('items' in data) {
            this._queryState.page = data.page;
            this._queryState.page_size = data.page_size;
            this._items = data.items;
            this._filteredCount = data.filtered_count;
            this._itemCount = data.item_count;
            this._pageCount = data.page_count;
            this._totalCount = data.total_count;
            // If current page is out of bounds, redirect to the last valid page
            if (this._queryState.page > this._pageCount) {
                const targetPage = this._pageCount === 0 ? 1 : this._pageCount;
                // Only reload if we're not already on the target page
                if (this._queryState.page !== targetPage) {
                    this._queryState.page = targetPage;
                    this.reload({ updateState: true });
                    return;
                }
            }
        }
        else {
            this._items = data;
            this._filteredCount = this.items.length;
            this._totalCount = this.items.length;
            this._pageCount = 1;
        }
        this.emit('update');
    }
    reload(args) {
        if (this._reloadTimer)
            clearTimeout(this._reloadTimer);
        return this._reload(args)
            .then((resp) => {
            this._enqueueReload();
            this.items = resp;
            return resp;
        })
            .catch((err) => {
            this._enqueueReload();
            if (err.status === 401) {
                return;
            }
            if (err.message) {
                this.emit('error', err.message);
            }
            else {
                this.emit('error', err);
            }
        });
    }
    reset() {
        this._setState();
    }
    _reload(_opts) {
        return Promise.resolve([]);
    }
    _enqueueReload() {
        if (this._opts.autoReloadTimeout > 0) {
            if (this._reloadTimer)
                clearTimeout(this._reloadTimer);
            this._reloadTimer = setTimeout(this.reload.bind(this), this._opts.autoReloadTimeout);
        }
    }
    emit(eventName, args) {
        this._events.dispatchEvent(new CustomEvent(eventName, { detail: args }));
    }
    on(eventName, listener) {
        this._events.addEventListener(eventName, listener);
    }
    queryParams(params) {
        const p = params ?? new URLSearchParams();
        if (this.page > 0) {
            p.set('page', String(this.page));
            if (this.pageSize > 0) {
                p.set('page_size', String(this.pageSize));
            }
        }
        else {
            p.delete('page');
            p.delete('page_size');
        }
        if (this.searchTerm && this.searchTerm.length > 0) {
            p.set('use_regex', 'true');
            p.set('name', this.searchTerm);
        }
        else {
            p.delete('use_regex');
            p.delete('name');
        }
        if (this.sortKey && this.sortKey !== '') {
            p.set('sort', this.sortKey);
            p.set('sort_reverse', String(this.reverseOrder));
        }
        else {
            p.delete('sort');
            p.delete('sort_reverse');
        }
        p.sort();
        return p;
    }
    get _cacheKey() {
        return `${window.location.pathname.split('/').pop()}-queryState`;
    }
}
export class UrlDataSource extends DataSource {
    url;
    constructor(url, opts) {
        super(opts);
        if (url.startsWith('http')) {
            this.url = new URL(url);
        }
        else {
            this.url = new URL(url, window.location.href);
        }
    }
    _fullApiUrl() {
        const url = new URL(this.url);
        url.search = this.queryParams().toString();
        return url;
    }
    _reload(opts = {}) {
        const url = this._fullApiUrl();
        if (this._opts.useQueryState) {
            if (opts.updateState !== false) {
                const documentUrl = new URL(window.location.href);
                const query = new URLSearchParams(documentUrl.hash.substring(1));
                this.queryParams(query);
                documentUrl.hash = query.toString();
                if (window.location.hash.length <= 1 || this._lastLoadedUrl === '') {
                    window.history.replaceState(null, '', documentUrl);
                }
                else if (this._lastLoadedUrl !== url.toString()) {
                    window.history.pushState(null, '', documentUrl);
                }
            }
            window.sessionStorage.setItem(this._cacheKey, JSON.stringify(this._queryState));
        }
        this._lastLoadedUrl = url.toString();
        return HTTP.request('GET', url).then((resp) => resp ?? []);
    }
}
//# sourceMappingURL=datasource.js.map