// uPlot formats numbers with navigator.language when its module loads, and an
// invalid tag (e.g. 'en-US@posix' from Chromium on Linux without LANG) throws a
// RangeError there. Imported before uPlot so it sees a valid tag.
function isValidLocale (locale) {
  try {
    Intl.getCanonicalLocales(locale)
    return true
  } catch {
    return false
  }
}

if (!isValidLocale(navigator.language)) {
  Object.defineProperty(navigator, 'language', { value: 'en-US', configurable: true })
}
