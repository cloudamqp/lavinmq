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

function argumentHelper (className, e) {
  const val = e.target.getAttribute('data-tag')
  if (val) {
    const currentVal = document.querySelector(`[name=${className}]`).value
    if (currentVal.includes(val)) { return }
    document.querySelector(`[name=${className}]`).value = currentVal ? currentVal + ', ' + val : val
  } else if (val === "") {
    document.querySelector(`[name=${className}]`).value = ""
  }
}

function argumentHelperJSON (className, e) {
  const val = e.target.getAttribute('data-tag')
  let value = e.target.getAttribute('value')
  if (value === null) {
    value = 'value'
  }
  const currentVal = document.querySelector(`[name=${className}]`).value
  if (currentVal === "" && val) {
    document.querySelector(`[name=${className}]`).value = `{"${val}": ${value}}`
  } else if (currentVal[currentVal.length - 1] === "}" && val) {
    document.querySelector(`[name=${className}]`).value = currentVal.substr(0, currentVal.length - 1) + `,\n"${val}": ${value}}`
  }
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
  HTTP.request('GET',`/api/${type}`).then(res => {
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
  autoCompleteDatalist,
  formatTimestamp
}
