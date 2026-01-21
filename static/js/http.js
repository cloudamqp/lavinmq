function alertErrorHandler(e) {
    window.alert(e.body || e.message || e.reason);
}
function standardErrorHandler(e) {
    if (e.status === 404) {
        console.warn(`Not found: ${e.message}`);
    }
    else if (e.status === 401) {
        window.location.assign('login');
    }
    else if (e.body || e.message || e.reason) {
        alertErrorHandler(e);
    }
    else {
        console.error(e);
    }
    throw e;
}
export function request(method, path, options = {}) {
    const body = options.body;
    const headers = options.headers || new Headers();
    const opts = {
        method,
        headers,
    };
    if (body instanceof FormData) {
        headers.delete('Content-Type'); // browser will set to multipart with boundary
        opts.body = body;
    }
    else if (body) {
        headers.append('Content-Type', 'application/json');
        opts.body = JSON.stringify(body);
    }
    return fetch(path.toString(), opts).then((response) => {
        if (!response.ok) {
            const error = { status: response.status, reason: response.statusText, is_error: true };
            return response
                .json()
                .then((json) => {
                if (json.reason)
                    error.reason = json.reason;
                return error;
            })
                .catch(() => error)
                .then((err) => {
                standardErrorHandler(err);
            });
        }
        else {
            return response.json().catch(() => null);
        }
    });
}
class NoUrlEscapeString {
    value;
    constructor(value) {
        this.value = value;
    }
    toString() {
        return this.value;
    }
}
export function url(strings, ...params) {
    return params.reduce((res, param, i) => {
        const suffix = strings[i + 1] ?? '';
        if (param instanceof NoUrlEscapeString) {
            return res + param.toString() + suffix;
        }
        else {
            return res + encodeURIComponent(param) + suffix;
        }
    }, strings[0] ?? '');
}
export function noencode(v) {
    return new NoUrlEscapeString(v);
}
//# sourceMappingURL=http.js.map