(function () {
  window.avalanchemq = window.avalanchemq || {};
  let avalanchemq = window.avalanchemq;

  function testLoggedIn() {
    var hash = location.hash;
    if (hash.startsWith("#/login")) {
      var arr = hash.split("/");
      set_auth(arr[2] + ":" + arr[3]);
      location.hash = "";
      window.location.assign("/");
    }
    if (location.pathname !== "/login") {
      avalanchemq.http.request("GET", "/api/whoami").then(function (response) {
        setUsername();
      }).catch(function () {
        // not logged in
      });
    }
  }

  function setUsername() {
    document.querySelector("#username").innerText = get_cookie_value("username");
  }

  function auth_header() {
    if (get_cookie_value('auth')) {
      return "Basic " + decodeURIComponent(get_cookie_value('auth'));
    } else {
      return null;
    }
  }

  function signout() {
    clear_cookie_value('auth');
    clear_cookie_value('username');
    avalanchemq.http.redirectToLogin();
  }

  function set_auth(userinfo) {
    clear_cookie_value('auth');
    clear_cookie_value('username');

    var b64 = window.btoa(userinfo);
    store_cookie({ 'auth': encodeURIComponent(b64) });
    store_cookie({ 'username': userinfo.split(':')[0] });
  }

  function store_cookie(dict) {
    var date = new Date();
    date.setHours(date.getHours() + 8);
    Object.assign(dict, parse_cookie());
    store_cookie_with_expiration(dict, date);
  }

  function store_cookie_with_expiration(dict, expiration_date) {
    var enc = [];
    for (var k in dict) {
      enc.push(k + ':' + escape(dict[k]));
    }
    document.cookie = 'm=' + enc.join('|') + '; expires=' + expiration_date.toUTCString();
  }

  function clear_cookie_value(k) {
    var d = parse_cookie();
    delete d[k];
    var date = new Date();
    date.setHours(date.getHours() + 8);
    store_cookie_with_expiration(d, date);
  }

  function get_cookie_value(k) {
    return parse_cookie()[k];
  }

  function parse_cookie() {
    var c = get_cookie('m');
    var items = c.length == 0 ? [] : c.split('|');

    var dict = {};
    for (var i in items) {
      var kv = items[i].split(':');
      dict[kv[0]] = unescape(kv[1]);
    }
    return dict;
  }

  function get_cookie(key) {
    var cookies = document.cookie.split(';');
    for (var i in cookies) {
      var kv = cookies[i].trim().split('=');
      if (kv[0] == key) {
        return kv[1];
      }
    }
    return '';
  }

  Object.assign(window.avalanchemq, {
    auth: {
      header: auth_header,
      testLoggedIn: testLoggedIn,
      set_auth: set_auth,
      store_cookie: store_cookie
    }
  });
})();
