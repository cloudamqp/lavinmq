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
  const key = e.target.getAttribute('data-tag')
  const form = document.getElementById(formID)
  const currentValue = form.elements[name].value
  if (key && !currentValue.includes(key)) {
    form.elements[name].value = currentValue ? currentValue + ', ' + key : key
  } else if (key === '') {
    form.elements[name].value = ''
  }
}

function argumentHelperJSON (formID, name, e) {
  const key = e.target.getAttribute('data-tag')
  const value = e.target.getAttribute('value') || 'value'
  const form = document.getElementById(formID)
  try {
    const currentValue = JSON.parse(form.elements[name].value ?? '{}')
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
function autoCompleteDatalist(datalistID, type) {
  HTTP.request('GET',`api/${type}`).then(res => {
    const datalist = document.getElementById(datalistID);
    while (datalist.firstChild) {
      datalist.removeChild(datalist.lastChild);
    }
    const values = res.map(val => val.name)
    const uniqValues = [...new Set(values)];
    uniqValues.sort().forEach(val => {
      const option = document.createElement("option")
      option.value = val
      datalist.appendChild(option)
    });
  })
}

export {
  formatNumber,
  nFormatter,
  duration,
  argumentHelper,
  argumentHelperJSON,
  formatJSONargument,
  autoCompleteDatalist,
  formatTimestamp
}
