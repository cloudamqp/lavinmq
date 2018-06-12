
(function () {
  window.avalanchemq = window.avalanchemq || {};
  let avalanchemq = window.avalanchemq;

  let url = "/api/overview";
  let raw = localStorage.getItem(url);
  let updateTimer = null;

  if (raw) {
    try {
      let data = JSON.parse(raw);
      if (data) {
        render(data);
      }
    } catch(e) {
      localStorage.removeItem(url);
      console.log("Error parsing data from localStorage");
      console.error(e);
    }
  }

  function update() {
    avalanchemq.http.request("GET", url).then(function (response) {
      try {
        localStorage.setItem("/api/overview", JSON.stringify(response));
      } catch (e) {
        console.error("Saving localStorage", e);
      }
      render(response);
    });
  }

  function render(data) {
    document.querySelector("#version").innerText = data.avalanchemq_version;
    let table = document.querySelector("#overview");
    if (table) {
      Object.keys(data.object_totals).forEach(function (key) {
        table.querySelector("." + key).innerText = data.object_totals[key];
      });
    }
  }

  function start() {
    update();
    updateTimer = setInterval(update, 5000);
  }

  function stop() {
    if (updateTimer) {
      clearInterval(updateTimer);
    }
  }

  Object.assign(window.avalanchemq, {
    overview: {
      update, start, stop, render
    }
  });
}) ();
