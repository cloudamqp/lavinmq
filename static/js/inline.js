(function () {
  try {
    var collapsed = window.localStorage.getItem("menuCollapsed") === "true";
    document.documentElement.classList.toggle("menu-collapsed", collapsed);
  } catch (e) {}
})();
