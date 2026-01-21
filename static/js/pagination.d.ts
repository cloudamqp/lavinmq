import type { DataSource } from './datasource.js';
declare class Pagination<T> {
    #private;
    constructor(container: HTMLElement, dataSource: DataSource<T>);
    update(): void;
}
export declare function create<T>(container: HTMLElement, dataSource: DataSource<T>): Pagination<T>;
export {};
//# sourceMappingURL=pagination.d.ts.map