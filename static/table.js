(function () {
  function renderTable(url, keyColumns, renderRow) {
    var sortColumns = keyColumns;
    var reverseOrder = false;
    makeHeadersSortable();
    var raw = localStorage.getItem(url);
    if (raw) {
      updateTable(raw);
    }
    fetchAndUpdate();
    var updateTimer = setInterval(fetchAndUpdate, 5000);

    function makeHeadersSortable() {
      document.querySelectorAll("#table th[sort-columns]").forEach(function (cell) {
        cell.addEventListener("click", function (e) {
          // var column = e.target.cellIndex;
          var newSortColumns = e.target.getAttribute("sort-columns").split(",");
          // var newSortColumn = e.target.textContent.toLowerCase();
          if (sortColumns.toString() === newSortColumns.toString()) {
            reverseOrder = !reverseOrder;
          } else {
            sortColumns = newSortColumns;
          }
          clearInterval(updateTimer);
          var t = document.getElementById("table").tBodies[0];
          while (t.rows.length) {
            t.deleteRow(0);
          }
          var raw = localStorage.getItem(url);
          updateTable(raw);
        });
      });
    }

    function fetchAndUpdate() {
      var xhr = new XMLHttpRequest();
      xhr.open('GET', url);
      xhr.onload = function () {
        document.getElementById("xhr-error").textContent = "";
        localStorage.setItem(url, this.response);
        updateTable(this.response);
      };
      xhr.onerror = function (e) {
        document.getElementById("xhr-error").textContent = "Error fetching data";
      };
      xhr.ontimeout = function (e) {
        document.getElementById("xhr-error").textContent = "Error fetching data";
      };
      xhr.timeout = 5000;
      xhr.send();
    }

    function updateTable(raw) {
      var data = JSON.parse(raw);
      data.sort(function (a, b) {
        for (var i = 0; i < sortColumns.length; i++) {
          if (a[sortColumns[i]] > b[sortColumns[i]]) {
            return 1 * (reverseOrder ? -1 : 1);
          }
          if (a[sortColumns[i]] < b[sortColumns[i]]) {
            return -1 * (reverseOrder ? -1 : 1);
          }
        }
        return 0;
      });
      document.getElementById("table-count").textContent = data.length;
      var t = document.getElementById("table").tBodies[0];
      for (var i = 0; i < data.length; i++) {
        var item = data[i];
        var foundIndex = findIndex(t.rows, i, item);
        if (foundIndex !== -1) {
          var d = foundIndex - i;
          while (d--) t.deleteRow(i);
          renderRow(t.rows[i], item);
        } else {
          var tr = t.insertRow(i);
          setKeyAttributes(tr, item);
          renderRow(tr, item);
        }
      }

      var rowsToDelete = t.rows.length - data.length;
      while (0 < rowsToDelete--) {
        t.deleteRow(t.rows.length - 1);
      }
    }

    function findIndex(rows, start, item) {
      for (var i = start; i < rows.length; i++) {
        for (var k = 0; k < keyColumns.length; k++) {
          if (rows[i].getAttribute(keyColumns[k]) !== item[keyColumns[k]]) {
            break;
          } else if (k === keyColumns.length - 1) {
            return i;
          }
        }
      }
      return -1;
    }

    function setKeyAttributes(tr, item) {
      keyColumns.forEach(function (key) {
        tr.setAttribute(key, item[key]);
      });
    }
  }

  function renderCell(tr, column, value) {
    var cell = tr.cells[column] || tr.insertCell(-1);
    var text = value.toString();
    if (cell.textContent !== text) {
      cell.textContent = text;
    }
  }

  window.avalanchemq = window.avalanchemq || {};
  Object.assign(window.avalanchemq, {
    table: {
      renderCell: renderCell,
      renderTable: renderTable
    }
  });
})();
