import './lib/chartjs-adapter-luxon.esm.js';
interface DataPoint {
    x: Date;
    y: number | null;
}
interface CustomDataset {
    key: string;
    label: string;
    fill: boolean;
    order: number;
    type: string;
    tension: number;
    pointRadius: number;
    pointStyle: string;
    data: DataPoint[];
    backgroundColor: string;
    borderColor: string;
}
export interface CustomChart {
    data: {
        datasets: CustomDataset[];
        labels: unknown[];
    };
    reverseStack?: boolean;
    update(): void;
}
interface ChartData {
    rate?: number;
    log?: number[];
}
type ChartDataRecord = Record<string, ChartData | number | number[] | undefined>;
export declare function render(id: string, unit: string, fill?: boolean | Record<string, unknown>, stacked?: boolean, reverseStack?: boolean): CustomChart;
export declare function update(chart: CustomChart, data: ChartDataRecord, filled?: boolean): void;
export {};
//# sourceMappingURL=chart.d.ts.map