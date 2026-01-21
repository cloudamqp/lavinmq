interface WhoAmIResponse {
    tags: string[];
}
declare let shouldAutoScroll: boolean;
declare const evtSource: EventSource;
declare const livelog: HTMLElement | null;
declare const tbody: HTMLElement | null;
declare const btnToTop: HTMLElement | null;
declare const btnToBottom: HTMLElement | null;
declare function forbidden(): void;
declare function setScrollMode(toBottom: boolean): void;
declare const savedMode: string | null;
declare const initialMode: boolean;
declare let lastScrollTop: number;
//# sourceMappingURL=logs.d.ts.map