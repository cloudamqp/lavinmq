type ValueFactory<T> = (item: T) => string | undefined;
export declare function editItem<T>(form: HTMLFormElement | string, item: T, valueFactories?: Record<string, ValueFactory<T>>): void;
export {};
//# sourceMappingURL=form.d.ts.map