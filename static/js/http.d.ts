export interface RequestOptions {
    body?: unknown;
    headers?: Headers;
}
export interface ErrorResponse {
    status: number;
    reason: string;
    is_error: true;
}
export declare function request<T = unknown>(method: string, path: string | URL, options?: RequestOptions): Promise<T | null>;
declare class NoUrlEscapeString {
    private value;
    constructor(value: string);
    toString(): string;
}
export declare function url(strings: TemplateStringsArray, ...params: (string | NoUrlEscapeString)[]): string;
export declare function noencode(v: string): NoUrlEscapeString;
export {};
//# sourceMappingURL=http.d.ts.map