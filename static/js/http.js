(function () {
  window.avalanchemq = window.avalanchemq || {};
  let avalanchemq = window.avalanchemq;

  function testLoggedIn() {
    var hash = location.hash;
    if (hash.startsWith("#/login")) {
      var arr = hash.split("/");
      avalanchemq.auth.setAuth(arr[2] + ":" + arr[3]);
      location.hash = "";
      window.location.assign("/");
    }
    if (location.pathname !== "/login") {
      request("GET", "/api/whoami").then(function (response) {
        avalanchemq.auth.setUsername();
      }).catch(function () {
        // not logged in
      });
      request("GET", "/api/overview").then(function (response) {
        try {
          localStorage.setItem("/api/overview", JSON.stringify(response));
        } catch (e) {
          console.error("Saving localStorage", e);
        }
      });
    }
  }

  function redirectToLogin() {
    window.location.assign("/login");
  }

  function request(method, path, body) {
    let request = new Request(path);
    let headers = new Headers();
    if (!avalanchemq.auth && window.location.pathname !== "/login") {
      redirectToLogin();
    }
    var hdr = avalanchemq.auth.header();
    headers.append('Authorization', hdr);
    headers.append('Content-Type', 'application/json');
    let opts = {
      method: method,
      headers: headers,
      credentials: "include",
      mode : "cors",
      redirect: "follow"
    };
    if (body) {
      opts.body = JSON.stringify(body);
    }
    return fetch(request, opts)
    .then(function (response) {
      if (response.status === 401) {
        redirectToLogin();
      } else if (!(response.status >= 200 && response.status < 400)) {
        // not ok
      }
      return response;
    })
    .then(function (response) {
      return response.json();
    });
  }

  testLoggedIn();

  Object.assign(window.avalanchemq, {
    http: {
      request, redirectToLogin
    }
  });
}) ();
