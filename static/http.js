(function () {
  window.avalanchemq = window.avalanchemq || {};
  let avalanchemq = window.avalanchemq;

  function redirectToLogin() {
    window.location.assign("/login");
  }

  function request(method, path, body) {
    let request = new Request(path);
    let headers = new Headers();
    var auth = avalanchemq.auth.header();
    if (!auth && window.location.pathname !== "/login") {
      redirectToLogin();
    }
    headers.append('Authorization', auth);
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

  Object.assign(window.avalanchemq, {
    http: {
      request: request,
      redirectToLogin: redirectToLogin
    }
  });

  avalanchemq.auth.testLoggedIn();
})();
