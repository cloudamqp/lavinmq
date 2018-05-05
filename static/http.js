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
    let opts = {
      method: method,
      headers: headers,
      credentials: "include",
      mode : "cors",
      body : body,
      redirect: "follow"
    };
    return fetch(request, opts).then(function (response) {
      if (response.status === 401) {
        redirectToLogin();
      } else if (!(response.status >= 200 && response.status < 400)) {
        console.log(response.body);
      }
      return response;
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
