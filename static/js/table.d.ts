import { type DataSource } from './datasource.js';
export type RenderRowFn<T> = (tr: HTMLTableRowElement, item: T, isNew: boolean) => void;
export interface RenderTableOptions<T> {
    countId?: string;
    dataSource?: DataSource<T>;
    url?: string;
    keyColumns?: string[];
    columnSelector?: boolean;
    search?: boolean;
    pagination?: boolean;
}
export interface TableController {
    updateTable: () => void;
    reload: () => Promise<unknown>;
    on: (event: string, args: EventListenerOrEventListenerObject) => void;
}
export declare function renderTable<T>(id: string, options: RenderTableOptions<T> | undefined, renderRow: RenderRowFn<T>): TableController;
export declare function renderCell(tr: HTMLTableRowElement, column: number, value: Element | string | number | null | undefined, classList?: string): HTMLTableCellElement | undefined;
export declare function toggleDisplayError(tableID: string, message: string | null): void;
//# sourceMappingURL=table.d.ts.map