export function parseJSON<T = Record<string, unknown>>(data: string): T {
  try {
    if (data.length) {
      return JSON.parse(data) as T
    }
    return {} as T
  } catch (e) {
    if (e instanceof SyntaxError) {
      window.alert('Input must be JSON')
    }
    throw e
  }
}

export function jsonToText(obj: unknown): string {
  if (obj == null) return ''
  return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '').trim()
}

type ToastType = 'success' | 'warn' | 'error'

function toastImpl(text: string, type: ToastType = 'success'): void {
  // Delete all previous toasts
  document.querySelectorAll('.toast').forEach((t) => t.parentNode?.removeChild(t))

  const d = document.createElement('div')
  d.classList.add(type)
  d.classList.add('toast')
  d.textContent = text
  document.body.appendChild(d)
  setTimeout(() => {
    try {
      document.body.removeChild(d)
    } catch {
      // noop
    }
  }, 7000)
}

interface Toast {
  (text: string): void
  success: (text: string) => void
  warn: (text: string) => void
  error: (text: string) => void
}

export const toast: Toast = Object.assign(
  function toast(text: string): void {
    toastImpl(text, 'success')
  },
  {
    success: function toastSuccess(text: string): void {
      toastImpl(text, 'success')
    },
    warn: function toastWarn(text: string): void {
      toastImpl(text, 'warn')
    },
    error: function toastError(text: string): void {
      toastImpl(text, 'error')
    },
  }
)

function createButton(
  type: 'button' | 'submit' | 'reset',
  text: string,
  classes: string[],
  click?: (e: MouseEvent) => void
): HTMLButtonElement {
  const btn = document.createElement('button')
  btn.type = type
  btn.textContent = text
  btn.classList.add(...classes)
  if (click) {
    btn.addEventListener('click', click)
  }
  return btn
}

interface ButtonOptions {
  click?: (e: MouseEvent) => void
  text?: string
  type?: 'button' | 'submit' | 'reset'
}

export const button = {
  delete: ({ click, text = 'Delete', type = 'button' }: ButtonOptions): HTMLButtonElement => {
    return createButton(type, text, ['btn-small', 'btn-small-outlined-danger'], click)
  },
  edit: ({ click, text = 'Edit', type = 'button' }: ButtonOptions): HTMLButtonElement => {
    return createButton(type, text, ['btn-small'], click)
  },
  submit: ({ text = 'Save' }: { text?: string } = {}): HTMLButtonElement => {
    return createButton('submit', text, ['btn-icon', 'btn-submit'])
  },
  reset: ({ text = 'Reset' }: { text?: string } = {}): HTMLButtonElement => {
    return createButton('reset', text, ['btn-icon', 'btn-reset'])
  },
}
