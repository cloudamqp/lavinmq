import * as HTTP from './http.js'

function formatNumber (num) {
  if (typeof num.toLocaleString === 'function') {
    return num.toLocaleString('en', { style: 'decimal', minimumFractionDigits: 0, maximumFractionDigits: 1 })
  }

  return num
}

function nFormatter (num) {
  let suffix = ''

  if (num === '') {
    return num
  }

  if (num >= 1000000000) {
    suffix = 'G'
    num = (num / 1000000000)
  }
  if (num >= 1000000) {
    suffix = 'M'
    num = (num / 1000000)
  }
  if (num >= 1000) {
    suffix = 'K'
    num = (num / 1000)
  }

  return formatNumber(num) + suffix
}

function duration (seconds) {
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

function argumentHelper (formID, name, e) {
  const key = e.target.dataset.tag
  const form = document.getElementById(formID)
  const currentValue = form.elements[name].value.split(/,+\s*/).map(s => s.trim()).filter(s => s.length > 0)
  if (key && !currentValue.includes(key)) {
    currentValue.push(key)
    form.elements[name].value = currentValue.join(', ')
  } else if (key === '') {
    const defaultValue = e.target.datalist ? e.target.datalist.value : ''
    form.elements[name].value = defaultValue
  }
}

function argumentHelperJSON (formID, name, e) {
  const key = e.target.getAttribute('data-tag')
  const value = e.target.getAttribute('value') || 'value'
  const form = document.getElementById(formID)
  try {
    let currentValue = form.elements[name].value.trim()
    if (currentValue.length == 0) {
      currentValue = '{}'
    }
    currentValue = JSON.parse(currentValue)
    if (currentValue[key] || !key) { return }
    currentValue[key] = value
    form.elements[name].value = formatJSONargument(currentValue)
  } catch {
    form.elements[name].value += `\n"${key}": ${value}`
  }
}

function formatJSONargument(obj) {
  const values = Object.keys(obj).map(key => `"${key}": ${JSON.stringify(obj[key])}`).join(',\n')
  return `{ ${values} }`
}

function formatTimestamp(timestamp) {
  const date = new Date(timestamp).toISOString().split("T");

  return `${date[0]} ${date[1].split(".")[0]}`;
}

/**
 * @param datalistID id of the datalist element linked to input
 * @param type input content, accepts: queues, exchanges, vhosts, users
 */
function autoCompleteDatalist(datalistID, type, vhost) {
  HTTP.request('GET',`api/${type}/${vhost}?columns=name`).then(res => {
    const datalist = document.getElementById(datalistID);
    while (datalist.firstChild) {
      datalist.removeChild(datalist.lastChild);
    }
    const values = res.map(val => val.name).sort()
    values.forEach(val => {
      const option = document.createElement("option")
      option.value = val
      datalist.appendChild(option)
    });
  })
}

let loadedVhosts

function fetch() {
  const vhost = window.sessionStorage.getItem('vhost')
  const url = 'api/vhosts?columns=name'
  if (!loadedVhosts) {
    loadedVhosts = HTTP.request('GET', url).then(function (vhosts) {
      if (vhost !== '_all' && !vhosts.some(vh => vh.name === vhost)) {
        window.sessionStorage.removeItem('vhost')
      }
      return vhosts
    }).catch(function (e) {
      console.error(e)
      return null
    })
  }
  return loadedVhosts
}

function addVhostOptions (formId, options) {
  options = options ?? {}
  const addAllOpt = options["addAll"] ?? false
  return fetch().then(vhosts => {
    const select = document.forms[formId].elements.vhost
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
      select.parentElement.insertAdjacentElement('beforebegin',err)
      return
    }

    const selectedVhost = window.sessionStorage.getItem('vhost')
    if (addAllOpt) {
      select.add(new Option('All', '_all', true, false))
    }
    const collator = new Intl.Collator()
    vhosts.sort((a, b) => collator.compare(a.name, b.name))
    vhosts.forEach(vhost => {
      select.add(new Option(vhost.name, vhost.name, false, vhost.name === selectedVhost))
    })
    return vhosts
  })
}

addVhostOptions('user-vhost', {addAll: true}).then(() => {
  const vhost = window.sessionStorage.getItem('vhost')
  if (vhost) {
    const opt = document.querySelector('#userMenuVhost option[value="' + vhost + '"]')
    if (opt) {
      document.querySelector('#userMenuVhost').value = vhost
      window.sessionStorage.setItem('vhost', vhost)
    }
  } else {
    window.sessionStorage.setItem('vhost', '_all')
  }
})

export {
  addVhostOptions,
  formatNumber,
  nFormatter,
  duration,
  argumentHelper,
  argumentHelperJSON,
  formatJSONargument,
  autoCompleteDatalist,
  formatTimestamp
}
