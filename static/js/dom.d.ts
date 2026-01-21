export declare function parseJSON<T = Record<string, unknown>>(data: string): T;
export declare function jsonToText(obj: unknown): string;
interface Toast {
    (text: string): void;
    success: (text: string) => void;
    warn: (text: string) => void;
    error: (text: string) => void;
}
export declare const toast: Toast;
interface ButtonOptions {
    click?: (e: MouseEvent) => void;
    text?: string;
    type?: 'button' | 'submit' | 'reset';
}
export declare const button: {
    delete: ({ click, text, type }: ButtonOptions) => HTMLButtonElement;
    edit: ({ click, text, type }: ButtonOptions) => HTMLButtonElement;
    submit: ({ text }?: {
        text?: string;
    }) => HTMLButtonElement;
    reset: ({ text }?: {
        text?: string;
    }) => HTMLButtonElement;
};
export {};
//# sourceMappingURL=dom.d.ts.map