(function () {
  window.avalanchemq = window.avalanchemq || {}
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

    return window.avalanchemq.helpers.formatNumber(num) + suffix
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

  Object.assign(window.avalanchemq, {
    helpers: {
      formatNumber,
      nFormatter,
      duration
    }
  })
})()
