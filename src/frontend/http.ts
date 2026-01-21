export interface RequestOptions {
  body?: unknown
  headers?: Headers
}

export interface ErrorResponse {
  status: number
  reason: string
  is_error: true
}

function alertErrorHandler(e: { body?: string; message?: string; reason?: string }): void {
  window.alert(e.body || e.message || e.reason)
}

function standardErrorHandler(e: ErrorResponse & { message?: string; body?: string }): never {
  if (e.status === 404) {
    console.warn(`Not found: ${e.message}`)
  } else if (e.status === 401) {
    window.location.assign('login')
  } else if (e.body || e.message || e.reason) {
    alertErrorHandler(e)
  } else {
    console.error(e)
  }
  throw e
}

export function request<T = unknown>(
  method: string,
  path: string | URL,
  options: RequestOptions = {}
): Promise<T | null> {
  const body = options.body
  const headers = options.headers || new Headers()
  const opts: RequestInit = {
    method,
    headers,
  }
  if (body instanceof FormData) {
    headers.delete('Content-Type') // browser will set to multipart with boundary
    opts.body = body
  } else if (body) {
    headers.append('Content-Type', 'application/json')
    opts.body = JSON.stringify(body)
  }
  return fetch(path.toString(), opts).then((response) => {
    if (!response.ok) {
      const error: ErrorResponse = { status: response.status, reason: response.statusText, is_error: true }
      return response
        .json()
        .then((json: { reason?: string }) => {
          if (json.reason) error.reason = json.reason
          return error
        })
        .catch(() => error)
        .then((err) => {
          standardErrorHandler(err as ErrorResponse & { message?: string })
        })
    } else {
      return response.json().catch(() => null) as Promise<T | null>
    }
  })
}

class NoUrlEscapeString {
  private value: string

  constructor(value: string) {
    this.value = value
  }

  toString(): string {
    return this.value
  }
}

export function url(strings: TemplateStringsArray, ...params: (string | NoUrlEscapeString)[]): string {
  return params.reduce<string>((res, param, i) => {
    const suffix = strings[i + 1] ?? ''
    if (param instanceof NoUrlEscapeString) {
      return res + param.toString() + suffix
    } else {
      return res + encodeURIComponent(param) + suffix
    }
  }, strings[0] ?? '')
}

export function noencode(v: string): NoUrlEscapeString {
  return new NoUrlEscapeString(v)
}
