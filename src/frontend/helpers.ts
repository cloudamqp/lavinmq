import * as HTTP from './http.js'

export function formatNumber(num: number): string {
  if (typeof num.toLocaleString === 'function') {
    return num.toLocaleString('en', { style: 'decimal', minimumFractionDigits: 0, maximumFractionDigits: 1 })
  }

  return String(num)
}

export function nFormatter(num: number | undefined | ''): string {
  let suffix = ''
  if (typeof num === 'undefined') {
    return ''
  }

  if (num === '') {
    return num
  }

  let value = num
  if (value >= 1000000000) {
    suffix = 'G'
    value = value / 1000000000
  }
  if (value >= 1000000) {
    suffix = 'M'
    value = value / 1000000
  }
  if (value >= 1000) {
    suffix = 'K'
    value = value / 1000
  }

  return formatNumber(value) + suffix
}

export function duration(seconds: number): string {
  let res = ''
  const days = Math.floor(seconds / (24 * 3600))
  if (days > 0) {
    res += days + 'd, '
  }
  const daysRest = seconds % (24 * 3600)
  const hours = Math.floor(daysRest / 3600)
  if (hours > 0) {
    res += hours + 'h, '
  }
  const hoursRest = daysRest % 3600
  const minutes = Math.floor(hoursRest / 60)
  res += minutes + 'm '
  if (days === 0) {
    res += Math.ceil(hoursRest % 60) + 's'
  }
  return res
}

export function argumentHelper(formID: string, name: string, e: Event): void {
  const target = e.target as HTMLElement
  const key = target.dataset['tag']
  const form = document.getElementById(formID) as HTMLFormElement | null
  if (!form) return
  const input = form.elements.namedItem(name) as HTMLInputElement | null
  if (!input) return
  const currentValue = input.value
    .split(/,+\s*/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
  if (key && !currentValue.includes(key)) {
    currentValue.push(key)
    input.value = currentValue.join(', ')
  } else if (key === '') {
    const targetInput = target as HTMLInputElement
    const defaultValue = targetInput.list?.dataset['value'] ?? ''
    input.value = defaultValue
  }
}

export function argumentHelperJSON(formID: string, name: string, e: Event): void {
  const target = e.target as HTMLElement
  const key = target.dataset['tag']
  let value: unknown
  try {
    value = JSON.parse(target.dataset['value'] || '""')
  } catch {
    value = target.dataset['value']
  }
  const form = document.getElementById(formID) as HTMLFormElement | null
  if (!form) return
  const input = form.elements.namedItem(name) as HTMLInputElement | null
  if (!input) return
  try {
    let currentValue = input.value.trim()
    if (currentValue.length === 0) {
      currentValue = '{}'
    }
    const parsed = JSON.parse(currentValue) as Record<string, unknown>
    if (parsed[key ?? ''] || !key) {
      return
    }
    parsed[key] = value
    input.value = formatJSONargument(parsed)
  } catch {
    input.value += `\n"${key}": ${value}`
  }
}

export function formatJSONargument(obj: Record<string, unknown>): string {
  const values = Object.keys(obj)
    .map((key) => `"${key}": ${JSON.stringify(obj[key])}`)
    .join(',\n')
  return `{ ${values} }`
}

export function formatTimestamp(timestamp: number | string): string {
  const date = new Date(timestamp).toISOString().split('T')

  return `${date[0]} ${date[1]?.split('.')[0] ?? ''}`
}

interface VhostResponse {
  name: string
}

/**
 * @param datalistID id of the datalist element linked to input
 * @param type input content, accepts: queues, exchanges, vhosts, users
 */
export function autoCompleteDatalist(datalistID: string, type: string, vhost: string | null): void {
  HTTP.request<VhostResponse[]>('GET', HTTP.url`api/${type}/${vhost ?? ''}?columns=name`).then((res) => {
    const datalist = document.getElementById(datalistID)
    if (!datalist) return
    while (datalist.firstChild) {
      datalist.removeChild(datalist.lastChild!)
    }
    const values = (res ?? []).map((val) => val.name).sort()
    values.forEach((val) => {
      const option = document.createElement('option')
      option.value = val
      datalist.appendChild(option)
    })
  })
}

let loadedVhosts: Promise<VhostResponse[] | null> | null = null

function fetch(): Promise<VhostResponse[] | null> {
  const vhost = window.sessionStorage.getItem('vhost')
  const url = 'api/vhosts?columns=name'
  if (!loadedVhosts) {
    loadedVhosts = HTTP.request<VhostResponse[]>('GET', url)
      .then(function (vhosts) {
        if (vhost !== '_all' && vhosts && !vhosts.some((vh) => vh.name === vhost)) {
          window.sessionStorage.removeItem('vhost')
        }
        return vhosts
      })
      .catch(function (e) {
        console.error(e)
        return null
      })
  }
  return loadedVhosts
}

interface AddVhostOptionsConfig {
  addAll?: boolean
}

export function addVhostOptions(
  formId: string,
  options?: AddVhostOptionsConfig
): Promise<VhostResponse[] | null | undefined> {
  const opts = options ?? {}
  const addAllOpt = opts.addAll ?? false
  return fetch().then((vhosts) => {
    const form = document.forms.namedItem(formId)
    if (!form) return
    const select = form.elements.namedItem('vhost') as HTMLSelectElement | null
    if (!select) return
    while (select.options.length) {
      select.remove(0)
    }

    if (!vhosts) {
      if (formId === 'user-vhost') {
        return
      }
      const err = document.createElement('span')
      err.id = 'error-msg'
      err.textContent = 'Error fetching data: Please try to refresh the page!'
      select.parentElement?.insertAdjacentElement('beforebegin', err)
      return
    }

    const selectedVhost = window.sessionStorage.getItem('vhost')
    if (addAllOpt) {
      select.add(new Option('All', '_all', true, false))
    }
    const collator = new Intl.Collator()
    vhosts.sort((a, b) => collator.compare(a.name, b.name))
    vhosts.forEach((vhost) => {
      select.add(new Option(vhost.name, vhost.name, false, vhost.name === selectedVhost))
    })
    return vhosts
  })
}
