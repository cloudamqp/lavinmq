(function () {
  window.avalanchemq = window.avalanchemq || {}
  function formatNumber (num) {
    if (typeof num.toLocaleString === "function") {
      return num.toLocaleString('en', { style: 'decimal', minimumFractionDigits: 0, maximumFractionDigits: 1 })
    }

    return num
  }

  Object.assign(window.avalanchemq, {
    helpers: {
      formatNumber
    }
  })
})()
