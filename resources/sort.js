(() => {
  const tbody = document.getElementById("index-body");
  if (!tbody) return;

  const fixedRows = Array.from(tbody.querySelectorAll("tr[data-parent='1']"));
  const sortableRows = Array.from(tbody.querySelectorAll("tr[data-original-index]"));
  const buttons = Array.from(document.querySelectorAll(".sort-btn"));

  let sortKey = null;
  let sortDir = 0; // 0 = default/original, 1 = asc, -1 = desc

  function num(row, key) {
    return Number(row.dataset[key]);
  }

  function str(row, key) {
    return row.dataset[key] || "";
  }

  function isDir(row) {
    return row.dataset.isDir === "1";
  }

  function compareText(a, b, key, dir) {
    return dir * str(a, key).localeCompare(
      str(b, key),
      undefined,
      { numeric: true, sensitivity: "base" }
    );
  }

  function compareNum(a, b, key, dir) {
    return dir * (num(a, key) - num(b, key));
  }

  function compareWithinGroup(a, b, key, dir) {
    if (key === "name") {
      const c = compareText(a, b, "name", dir);
      if (c !== 0) return c;
      return num(a, "originalIndex") - num(b, "originalIndex");
    }

    if (key === "ext") {
      if (isDir(a) && isDir(b)) {
        return num(a, "originalIndex") - num(b, "originalIndex");
      }
      const c = compareText(a, b, "ext", dir);
      if (c !== 0) return c;
      const byName = compareText(a, b, "name", 1);
      if (byName !== 0) return byName;
      return num(a, "originalIndex") - num(b, "originalIndex");
    }

    if (key === "size" || key === "mtime") {
      const c = compareNum(a, b, key, dir);
      if (c !== 0) return c;
      const byName = compareText(a, b, "name", 1);
      if (byName !== 0) return byName;
      return num(a, "originalIndex") - num(b, "originalIndex");
    }

    return num(a, "originalIndex") - num(b, "originalIndex");
  }

  function compareRows(a, b, key, dir) {
    const aDir = isDir(a);
    const bDir = isDir(b);

    if (aDir !== bDir) {
      return aDir ? -1 : 1;
    }

    return compareWithinGroup(a, b, key, dir);
  }

  function updateIndicators() {
    for (const btn of buttons) {
      const indicator = btn.querySelector(".sort-indicator");
      const key = btn.dataset.sortKey;
      if (key === sortKey && sortDir === 1) {
        indicator.textContent = "▲";
      } else if (key === sortKey && sortDir === -1) {
        indicator.textContent = "▼";
      } else {
        indicator.textContent = "";
      }
    }
  }

  function render() {
    const rows = [...sortableRows];

    if (sortKey && sortDir !== 0) {
      rows.sort((a, b) => compareRows(a, b, sortKey, sortDir));
    } else {
      rows.sort(
        (a, b) =>
          Number(a.dataset.originalIndex) - Number(b.dataset.originalIndex)
      );
    }

    tbody.replaceChildren(...fixedRows, ...rows);
    updateIndicators();
  }

  for (const btn of buttons) {
    btn.addEventListener("click", () => {
      const key = btn.dataset.sortKey;

      if (sortKey !== key) {
        sortKey = key;
        sortDir = 1;
      } else if (sortDir === 1) {
        sortDir = -1;
      } else {
        sortKey = null;
        sortDir = 0;
      }

      render();
    });
  }

  render();
})();
