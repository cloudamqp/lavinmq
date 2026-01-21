declare module './lib/chart.js' {
  export interface ChartDataset<TType extends string = string, TData = unknown[]> {
    type?: TType
    label?: string
    data: TData
    backgroundColor?: string | string[]
    borderColor?: string | string[]
    fill?: boolean | string | number
    order?: number
    tension?: number
    pointRadius?: number
    pointStyle?: string
    hidden?: boolean
    [key: string]: unknown
  }

  export interface ChartOptions<TType extends string = string> {
    responsive?: boolean
    maintainAspectRatio?: boolean
    aspectRatio?: number
    plugins?: {
      legend?: {
        labels?: {
          generateLabels?: (chart: Chart<TType>) => LegendItem[]
        }
      }
      tooltip?: {
        mode?: string
        intersect?: boolean
        position?: string
        callbacks?: {
          label?: (tooltipItem: TooltipItem<TType>) => string
        }
      }
    }
    hover?: {
      mode?: string
      intersect?: boolean
    }
    scales?: {
      x?: ScaleOptions
      y?: ScaleOptions
    }
  }

  export interface ScaleOptions {
    type?: string
    display?: boolean
    position?: string
    grid?: {
      color?: string
      display?: boolean
    }
    border?: {
      dash?: number[]
    }
    time?: {
      unit?: string
      displayFormats?: Record<string, string>
    }
    title?: {
      display?: boolean
      text?: string
    }
    ticks?: {
      callback?: (value: number | string) => string
    }
    stacked?: boolean
    beginAtZero?: boolean
    suggestedMax?: number
    min?: number
    max?: number
  }

  export interface LegendItem {
    text?: string
    fillStyle?: string
    strokeStyle?: string
    hidden?: boolean
    [key: string]: unknown
  }

  export interface TooltipItem<TType extends string = string> {
    dataset: ChartDataset<TType>
    parsed: { x: number; y: number }
    dataIndex: number
    [key: string]: unknown
  }

  export interface ChartConfiguration<TType extends string = string, TData = unknown[], TLabel = unknown> {
    type: TType
    data: {
      datasets: ChartDataset<TType, TData>[]
      labels?: TLabel[]
    }
    options?: ChartOptions<TType>
  }

  export interface ChartData<TType extends string = string, TData = unknown[], TLabel = unknown> {
    datasets: ChartDataset<TType, TData>[]
    labels?: TLabel[]
  }

  export class Chart<TType extends string = string, TData = unknown[], TLabel = unknown> {
    data: ChartData<TType, TData, TLabel>
    options: ChartOptions<TType>

    constructor(
      ctx: HTMLCanvasElement | CanvasRenderingContext2D,
      config: ChartConfiguration<TType, TData, TLabel>
    )

    update(mode?: string): void
    destroy(): void

    static register(...components: unknown[]): void
    static defaults: {
      plugins: {
        legend: {
          labels: {
            generateLabels?: <T extends string>(chart: Chart<T>) => LegendItem[]
          }
        }
      }
    }
  }

  export class TimeScale {}
  export class LinearScale {}
  export class LineController {}
  export class PointElement {}
  export class LineElement {}
  export class Legend {}
  export class Tooltip {}
  export class Title {}
  export class Filler {}
}

declare module './lib/chartjs-adapter-luxon.esm.js' {
  // Side-effect only import
}
