import * as HTTP from './http.js'
import * as Table from './table.js'
import * as DOM from './dom.js'

// Poll interval in milliseconds
const POLL_INTERVAL = 5000

// Tab management
const tabs = document.querySelectorAll('.tab-btn')
const tabContents = document.querySelectorAll('.tab-content')

tabs.forEach(tab => {
  tab.addEventListener('click', () => {
    const tabName = tab.dataset.tab

    // Remove active class from all tabs and contents
    tabs.forEach(t => t.classList.remove('active'))
    tabContents.forEach(c => c.classList.remove('active'))

    // Add active class to clicked tab and corresponding content
    tab.classList.add('active')
    document.getElementById(`tab-${tabName}`).classList.add('active')
  })
})

// Format timestamp for display
function formatTimestamp (timestamp) {
  if (!timestamp) return '-'
  const date = new Date(timestamp * 1000)
  return date.toLocaleString()
}

// Format duration in seconds to human readable
function formatDuration (seconds) {
  if (!seconds) return '-'
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}

// Truncate string with ellipsis
function truncate (str, maxLength = 50) {
  if (!str) return ''
  if (str.length <= maxLength) return str
  return str.substring(0, maxLength) + '...'
}

// Load dashboard stats
function loadDashboard () {
  HTTP.request('GET', 'api/job-queues/dashboard')
    .then(data => {
      document.getElementById('processed-today').textContent = data.processed_today
      document.getElementById('failed-today').textContent = data.failed_today
      document.getElementById('scheduled-count').textContent = data.scheduled_count
      document.getElementById('retry-count').textContent = data.retry_count
      document.getElementById('dead-count').textContent = data.dead_count
      document.getElementById('active-processes').textContent = data.active_processes
      document.getElementById('busy-workers').textContent = data.busy_workers
      document.getElementById('total-workers').textContent = data.total_workers
    })
    .catch(err => {
      console.error('Failed to load dashboard:', err)
    })
}

// Load scheduled jobs
function loadScheduledJobs () {
  const tbody = document.querySelector('#scheduled-table tbody')
  tbody.innerHTML = ''

  HTTP.request('GET', 'api/job-queues/scheduled-jobs')
    .then(jobs => {
      jobs.forEach(job => {
        const tr = document.createElement('tr')

        Table.renderCell(tr, 0, truncate(job.jid, 20))
        Table.renderCell(tr, 1, job.job.job_class)
        Table.renderCell(tr, 2, job.job.queue)
        Table.renderCell(tr, 3, formatTimestamp(job.scheduled_at))

        // Arguments with tooltip
        const argsCell = document.createElement('td')
        const argsText = JSON.stringify(job.job.args)
        if (argsText.length > 30) {
          const tooltip = document.createElement('span')
          tooltip.classList.add('arg-tooltip')
          tooltip.textContent = truncate(argsText, 30)
          const tooltipText = document.createElement('span')
          tooltipText.classList.add('tooltiptext')
          tooltipText.textContent = argsText
          tooltip.appendChild(tooltipText)
          argsCell.appendChild(tooltip)
        } else {
          argsCell.textContent = argsText
        }
        tr.appendChild(argsCell)

        // Actions
        const btnsCell = document.createElement('td')
        btnsCell.classList.add('right')
        const btns = document.createElement('div')
        btns.classList.add('buttons')

        const deleteBtn = DOM.button.delete({
          click: function () {
            if (window.confirm(`Delete scheduled job ${job.jid}?`)) {
              HTTP.request('DELETE', `api/job-queues/scheduled-jobs/${job.jid}`)
                .then(() => {
                  loadScheduledJobs()
                  loadDashboard()
                  DOM.toast('Scheduled job deleted')
                })
                .catch(err => {
                  DOM.toast('Failed to delete job', 'error')
                })
            }
          }
        })

        btns.appendChild(deleteBtn)
        btnsCell.appendChild(btns)
        tr.appendChild(btnsCell)

        tbody.appendChild(tr)
      })

      if (jobs.length === 0) {
        const tr = document.createElement('tr')
        const td = document.createElement('td')
        td.colSpan = 6
        td.textContent = 'No scheduled jobs'
        td.style.textAlign = 'center'
        tr.appendChild(td)
        tbody.appendChild(tr)
      }
    })
    .catch(err => {
      console.error('Failed to load scheduled jobs:', err)
      document.getElementById('scheduled-error').textContent = 'Failed to load scheduled jobs'
    })
}

// Load retry jobs
function loadRetries () {
  const tbody = document.querySelector('#retries-table tbody')
  tbody.innerHTML = ''

  HTTP.request('GET', 'api/job-queues/retries')
    .then(jobs => {
      jobs.forEach(job => {
        const tr = document.createElement('tr')

        Table.renderCell(tr, 0, truncate(job.jid, 20))
        Table.renderCell(tr, 1, job.job.job_class)
        Table.renderCell(tr, 2, job.job.queue)
        Table.renderCell(tr, 3, formatTimestamp(job.retry_at))
        Table.renderCell(tr, 4, job.retry_count)
        Table.renderCell(tr, 5, formatTimestamp(job.failed_at))
        Table.renderCell(tr, 6, truncate(job.error_message, 40))

        // Actions
        const btnsCell = document.createElement('td')
        btnsCell.classList.add('right')
        const btns = document.createElement('div')
        btns.classList.add('buttons')

        const retryBtn = DOM.button.edit({
          click: function () {
            if (window.confirm(`Retry job ${job.jid} now?`)) {
              HTTP.request('POST', `api/job-queues/retries/${job.jid}/retry`)
                .then(() => {
                  loadRetries()
                  loadDashboard()
                  DOM.toast('Job queued for retry')
                })
                .catch(err => {
                  DOM.toast('Failed to retry job', 'error')
                })
            }
          },
          text: 'Retry Now'
        })

        const killBtn = DOM.button.delete({
          click: function () {
            if (window.confirm(`Move job ${job.jid} to dead jobs?`)) {
              HTTP.request('POST', `api/job-queues/retries/${job.jid}/kill`)
                .then(() => {
                  loadRetries()
                  loadDashboard()
                  DOM.toast('Job moved to dead jobs')
                })
                .catch(err => {
                  DOM.toast('Failed to kill job', 'error')
                })
            }
          },
          text: 'Kill'
        })

        btns.append(retryBtn, killBtn)
        btnsCell.appendChild(btns)
        tr.appendChild(btnsCell)

        tbody.appendChild(tr)
      })

      if (jobs.length === 0) {
        const tr = document.createElement('tr')
        const td = document.createElement('td')
        td.colSpan = 8
        td.textContent = 'No retry jobs'
        td.style.textAlign = 'center'
        tr.appendChild(td)
        tbody.appendChild(tr)
      }
    })
    .catch(err => {
      console.error('Failed to load retries:', err)
      document.getElementById('retries-error').textContent = 'Failed to load retries'
    })
}

// Load dead jobs
function loadDeadJobs () {
  const tbody = document.querySelector('#dead-table tbody')
  tbody.innerHTML = ''

  HTTP.request('GET', 'api/job-queues/dead-jobs')
    .then(jobs => {
      jobs.forEach(job => {
        const tr = document.createElement('tr')

        Table.renderCell(tr, 0, truncate(job.jid, 20))
        Table.renderCell(tr, 1, job.job.job_class)
        Table.renderCell(tr, 2, job.job.queue)
        Table.renderCell(tr, 3, formatTimestamp(job.died_at))
        Table.renderCell(tr, 4, job.retry_count)
        Table.renderCell(tr, 5, truncate(job.error_message, 40))

        // Actions
        const btnsCell = document.createElement('td')
        btnsCell.classList.add('right')
        const btns = document.createElement('div')
        btns.classList.add('buttons')

        const retryBtn = DOM.button.edit({
          click: function () {
            if (window.confirm(`Retry dead job ${job.jid}?`)) {
              HTTP.request('POST', `api/job-queues/dead-jobs/${job.jid}/retry`)
                .then(() => {
                  loadDeadJobs()
                  loadDashboard()
                  DOM.toast('Job queued for execution')
                })
                .catch(err => {
                  DOM.toast('Failed to retry job', 'error')
                })
            }
          },
          text: 'Retry'
        })

        const deleteBtn = DOM.button.delete({
          click: function () {
            if (window.confirm(`Permanently delete job ${job.jid}?`)) {
              HTTP.request('DELETE', `api/job-queues/dead-jobs/${job.jid}`)
                .then(() => {
                  loadDeadJobs()
                  loadDashboard()
                  DOM.toast('Dead job deleted')
                })
                .catch(err => {
                  DOM.toast('Failed to delete job', 'error')
                })
            }
          }
        })

        btns.append(retryBtn, deleteBtn)
        btnsCell.appendChild(btns)
        tr.appendChild(btnsCell)

        tbody.appendChild(tr)
      })

      if (jobs.length === 0) {
        const tr = document.createElement('tr')
        const td = document.createElement('td')
        td.colSpan = 7
        td.textContent = 'No dead jobs'
        td.style.textAlign = 'center'
        tr.appendChild(td)
        tbody.appendChild(tr)
      }
    })
    .catch(err => {
      console.error('Failed to load dead jobs:', err)
      document.getElementById('dead-error').textContent = 'Failed to load dead jobs'
    })
}

// Load worker processes
function loadProcesses () {
  const tbody = document.querySelector('#processes-table tbody')
  tbody.innerHTML = ''

  HTTP.request('GET', 'api/job-queues/processes')
    .then(processes => {
      processes.forEach(proc => {
        const tr = document.createElement('tr')

        Table.renderCell(tr, 0, truncate(proc.identity, 30))
        Table.renderCell(tr, 1, proc.hostname)
        Table.renderCell(tr, 2, proc.pid)
        Table.renderCell(tr, 3, proc.concurrency)
        Table.renderCell(tr, 4, proc.busy)
        Table.renderCell(tr, 5, formatTimestamp(proc.beat))
        Table.renderCell(tr, 6, proc.queues.join(', '))

        tbody.appendChild(tr)
      })

      if (processes.length === 0) {
        const tr = document.createElement('tr')
        const td = document.createElement('td')
        td.colSpan = 7
        td.textContent = 'No active worker processes'
        td.style.textAlign = 'center'
        tr.appendChild(td)
        tbody.appendChild(tr)
      }
    })
    .catch(err => {
      console.error('Failed to load processes:', err)
      document.getElementById('processes-error').textContent = 'Failed to load processes'
    })
}

// Load metrics
function loadMetrics () {
  const tbody = document.querySelector('#metrics-table tbody')
  tbody.innerHTML = ''

  HTTP.request('GET', 'api/job-queues/metrics')
    .then(metrics => {
      metrics.forEach(metric => {
        const tr = document.createElement('tr')

        Table.renderCell(tr, 0, metric.job_class)
        Table.renderCell(tr, 1, metric.count)
        Table.renderCell(tr, 2, metric.success)
        Table.renderCell(tr, 3, metric.failed)
        const avgDuration = metric.count > 0 ? (metric.total_ms / metric.count).toFixed(2) : 0
        Table.renderCell(tr, 4, avgDuration)

        tbody.appendChild(tr)
      })

      if (metrics.length === 0) {
        const tr = document.createElement('tr')
        const td = document.createElement('td')
        td.colSpan = 5
        td.textContent = 'No metrics data'
        td.style.textAlign = 'center'
        tr.appendChild(td)
        tbody.appendChild(tr)
      }
    })
    .catch(err => {
      console.error('Failed to load metrics:', err)
      document.getElementById('metrics-error').textContent = 'Failed to load metrics'
    })
}

// Load all data
function loadAll () {
  loadDashboard()
  loadScheduledJobs()
  loadRetries()
  loadDeadJobs()
  loadProcesses()
  loadMetrics()
}

// Initial load
loadAll()

// Poll for updates
setInterval(loadAll, POLL_INTERVAL)
