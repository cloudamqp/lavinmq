import * as Auth from './auth.js'
import * as Vhosts from './vhosts.js'
import * as Overview from './overview.js'

document.getElementById("userMenuVhost").onchange = (e) => Auth.selectVhost(e)
document.getElementById("signoutLink").onclick = (e) => Auth.signOut(e)
