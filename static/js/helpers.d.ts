export declare function formatNumber(num: number): string;
export declare function nFormatter(num: number | undefined | ''): string;
export declare function duration(seconds: number): string;
export declare function argumentHelper(formID: string, name: string, e: Event): void;
export declare function argumentHelperJSON(formID: string, name: string, e: Event): void;
export declare function formatJSONargument(obj: Record<string, unknown>): string;
export declare function formatTimestamp(timestamp: number | string): string;
interface VhostResponse {
    name: string;
}
/**
 * @param datalistID id of the datalist element linked to input
 * @param type input content, accepts: queues, exchanges, vhosts, users
 */
export declare function autoCompleteDatalist(datalistID: string, type: string, vhost: string | null): void;
interface AddVhostOptionsConfig {
    addAll?: boolean;
}
export declare function addVhostOptions(formId: string, options?: AddVhostOptionsConfig): Promise<VhostResponse[] | null | undefined>;
export {};
//# sourceMappingURL=helpers.d.ts.map