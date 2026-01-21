export function getUsername() {
    const auth = getAuth();
    if (!auth)
        return undefined;
    return window.atob(auth).split(':')[0];
}
export function getPassword() {
    const auth = getAuth();
    if (!auth)
        return undefined;
    return window.atob(auth).split(':')[1];
}
function getAuth() {
    const m = getCookie('m');
    if (!m)
        return undefined;
    const idx = m.lastIndexOf(':');
    return decodeURIComponent(m.substring(idx + 1));
}
function getCookie(key) {
    return document.cookie
        .split('; ')
        .find((c) => c.startsWith(`${key}=`))
        ?.split('=')[1];
}
//# sourceMappingURL=auth.js.map