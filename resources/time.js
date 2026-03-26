(function () {
  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  function formatClock(d) {
    let h = d.getHours();
    const m = pad2(d.getMinutes());
    const suffix = h >= 12 ? "pm" : "am";
    h = h % 12;
    if (h === 0) h = 12;
    return `${h}:${m}${suffix}`;
  }

  function formatYMD(d) {
    return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
  }

  function startOfDay(d) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }

  function formatRelative(epochSec) {
    const dt = new Date(epochSec * 1000);
    const now = new Date();

    const dayMs = 24 * 60 * 60 * 1000;
    const diffDays = Math.round((startOfDay(now) - startOfDay(dt)) / dayMs);

    if (diffDays === 0) return `Today ${formatClock(dt)}`;
    if (diffDays === 1) return `Yesterday ${formatClock(dt)}`;
    return `${formatYMD(dt)} ${formatClock(dt)}`;
  }

  function updateMtimeCells() {
    document.querySelectorAll("#index-body tr[data-mtime]").forEach((row) => {
      if (row.dataset.parent === "1") return;

      const cell = row.querySelector(".col-mtime");
      if (!cell) return;

      const epochSec = Number(row.dataset.mtime);
      if (!Number.isFinite(epochSec)) return;

      const dt = new Date(epochSec * 1000);
      cell.textContent = formatRelative(epochSec);
      cell.title = dt.toLocaleString();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", updateMtimeCells);
  } else {
    updateMtimeCells();
  }

  setInterval(updateMtimeCells, 60 * 1000);
})();
