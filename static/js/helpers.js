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

  Object.assign(window.avalanchemq, {
    helpers: {
      formatNumber,
      nFormatter
    }
  })
})()
