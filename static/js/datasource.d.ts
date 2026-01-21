interface QueryStateData {
    page: number;
    page_size: number;
    sort: string;
    sort_reverse: boolean;
    name: string;
}
declare class QueryState {
    private _state;
    constructor();
    get name(): string;
    set name(value: string);
    get page(): number;
    set page(value: number | string);
    get page_size(): number;
    set page_size(value: number | string);
    get sort(): string;
    set sort(value: string);
    get sort_reverse(): boolean;
    set sort_reverse(value: boolean | string | number);
    update(values: Partial<QueryStateData>): void;
    toJSON(): QueryStateData;
}
export interface PaginatedResponse<T> {
    items: T[];
    page: number;
    page_size: number;
    filtered_count: number;
    item_count: number;
    page_count: number;
    total_count: number;
}
export interface DataSourceOptions {
    autoReloadTimeout?: number;
    useQueryState?: boolean;
}
interface ReloadOptions {
    updateState?: boolean;
}
export declare class DataSource<T = unknown> {
    static DEFAULT_STATE: QueryStateData;
    protected _opts: Required<DataSourceOptions>;
    protected _reloadTimer: ReturnType<typeof setTimeout> | null;
    protected _events: EventTarget;
    protected _items: T[];
    protected _filteredCount: number;
    protected _itemCount: number;
    protected _pageCount: number;
    protected _totalCount: number;
    protected _queryState: QueryState;
    protected _lastLoadedUrl: string;
    constructor(opts?: DataSourceOptions);
    private _setStateFromHash;
    protected _setState(properties?: Partial<QueryStateData>): void;
    get page(): number;
    set page(value: number);
    get pageSize(): number;
    set pageSize(value: number);
    set sortKey(value: string);
    get sortKey(): string;
    set reverseOrder(value: boolean);
    get reverseOrder(): boolean;
    set searchTerm(value: string);
    get searchTerm(): string;
    get filteredCount(): number;
    get itemCount(): number;
    get pageCount(): number;
    get totalCount(): number;
    get items(): T[];
    set items(data: PaginatedResponse<T> | T[]);
    reload(args?: ReloadOptions): Promise<PaginatedResponse<T> | T[] | void>;
    reset(): void;
    protected _reload(_opts?: ReloadOptions): Promise<PaginatedResponse<T> | T[]>;
    protected _enqueueReload(): void;
    emit(eventName: string, args?: unknown): void;
    on(eventName: string, listener: EventListenerOrEventListenerObject): void;
    queryParams(params?: URLSearchParams): URLSearchParams;
    protected get _cacheKey(): string;
}
export declare class UrlDataSource<T = unknown> extends DataSource<T> {
    url: URL;
    constructor(url: string, opts?: DataSourceOptions);
    private _fullApiUrl;
    protected _reload(opts?: ReloadOptions): Promise<PaginatedResponse<T> | T[]>;
}
export {};
//# sourceMappingURL=datasource.d.ts.map