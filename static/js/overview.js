
(function () {
  window.avalanchemq = window.avalanchemq || {};
  let avalanchemq = window.avalanchemq;

  let url = "/api/overview";
  let raw = localStorage.getItem(url);
  let updateTimer = null;

  if (raw) {
    render(JSON.parse(raw));
  }

  function update() {
    avalanchemq.http.request("GET", url).then(function (response) {
      render(response);
    });
  }

  function render(data) {
    console.log(data);
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
