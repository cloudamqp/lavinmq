import uPlot from './lib/uPlot.esm.js';

function render (id, unit, options = {}, stacked = false) {
  // --- Helper Functions (Time Formatting) ---
  // Helper for formatting time (using browser's local timezone)
  const localTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const timeFormatter = new Intl.DateTimeFormat(undefined, {
    timeZone: localTimezone,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const tzDate = ts => uPlot.tzDate(new Date(ts * 1000), localTimezone);
  const opts = {
    width: "800",
    height: "500",
    scales: {
      x: { auto: true, time: true },
      y: { auto: true },
    },
    legend: {
      live: true
    },
    cursor: {
      show: true,
    },
    series: [
      {} // x-axis
    ],
    axes: [
      {
        scale: "x",
        stroke: "#FFFFFF",
        ticks: {
          stroke: "#FFFFFF",
        },
        grid: {
          stroke: "rgba(255,255,255,0.1)",
        },
      },
      {
        scale: "y",
        label: unit,
        stroke: "#FFFFFF",
        ticks: {
          stroke: "#FFFFFF",
        },
        grid: {
          stroke: "rgba(255,255,255,0.1)",
        },
      }
    ]
  }
  return new uPlot(opts, [], document.getElementById(id))
}

function update (chart, data) {
  const nowSec = Math.floor(Date.now() / 1000); // Get current time in seconds
  const values = [
    Array.from({length: 120}, (_, i) => nowSec - 600 + i * 5), // 10min time window, 5s interval
  ]
  let keys = Object.keys(data)
  const hasDetails = keys.find(key => key.match(/_details$/))
  if (hasDetails) { keys = keys.filter(key => key.match(/_details$/)) }
  for (const key in data) {
    if (key.match(/_log$/)) continue
    let log = data[`${key}_log`] || data[key].log || []
    if (log.length > 0 && log[0].rate) log = log.map(v => v.rate)
    values.push(padArray(log))
    // Add series for each metric
    const label = formatLabel(key)
    if (!chart.series.some(s => s.label === label)) {
      chart.addSeries({
        label: label,
        stroke: chartColors[chart.series.length - 1 % chartColors.length],
        width: 1,
        min: 0,
        points: { show: true, size: 5, one: true }
      })
    }
  }
  chart.setData(values)
}

function padArray(array) {
  // Check if the array already has more than 120 elements
  if (array.length > 120) {
    // If so, return only the last 120 elements
    return array.slice(array.length - 120);
  }

  // Calculate how many zeros we need to add
  const nullsNeeded = 120 - array.length;

  // Create an array of null and concatenate with the original array
  const zeros = Array(nullsNeeded).fill(null);
  // Return the padded array
  return zeros.concat(array)
}

const chartColors = ['#54be7e', '#4589ff', '#d12771', '#d2a106', '#08bdba', '#bae6ff', '#ba4e00', '#d4bbff', '#8a3ffc', '#33b1ff', '#007d79']

function formatLabel (key) {
  const label = key.replace(/_/g, ' ').replace(/(rate|details|messages)/ig, '').trim()
    .replace(/^\w/, c => c.toUpperCase())
  return label || 'Total'
}

export {
  render, update
}
