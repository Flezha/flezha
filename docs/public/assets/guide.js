/*
 * `mermaid` fences are drawn with the library mix.exs loads before this file.
 * Picking a tab picks the same-labelled tab in every tabset on the page and
 * on later pages. Tabs switch without ExDoc's click, which focuses the tab and
 * would scroll to it; the scroll correction keeps the clicked tab in place
 * while tabsets above it change height.
 * Links that leave the docs open in a new tab.
 * A table column is no narrower than its average cell, up to 14em, so a prose
 * column is not squeezed to one word a line; a table that grows too wide scrolls.
 */

let flezhaMermaidReady = false;

window.addEventListener("exdoc:loaded", () => {
  if (!flezhaMermaidReady) {
    mermaid.initialize({
      startOnLoad: false,
      theme: document.body.className.includes("dark") ? "dark" : "default"
    });
    flezhaMermaidReady = true;
  }

  let id = 0;

  for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
    const preEl = codeEl.parentElement;
    const graphEl = document.createElement("div");

    mermaid.render("mermaid-graph-" + id++, codeEl.textContent).then(({svg, bindFunctions}) => {
      graphEl.innerHTML = svg;
      bindFunctions?.(graphEl);
      preEl.insertAdjacentElement("afterend", graphEl);
      preEl.remove();
    });
  }
});

{
  const storageKey = "flezha-tab";

  const selectTab = (label, except) => {
    for (const tab of document.querySelectorAll(".tabset-tab")) {
      if (tab === except || tab.textContent.trim() !== label) continue;

      for (const other of tab.parentNode.children) {
        const selected = other === tab;
        const panel = document.getElementById(other.getAttribute("aria-controls"));
        other.setAttribute("aria-selected", selected);
        other.tabIndex = selected ? 0 : -1;
        panel.hidden = !selected;
        panel.tabIndex = selected ? 0 : -1;
      }
    }
  };

  const syncTabs = (event) => {
    const tablist = event.target.closest?.(".tabset-tablist");
    if (!tablist) return;

    const selected = tablist.querySelector("[aria-selected=true]");
    const label = selected.textContent.trim();
    const top = selected.getBoundingClientRect().top;
    selectTab(label, selected);
    window.scrollBy(0, selected.getBoundingClientRect().top - top);
    try { localStorage.setItem(storageKey, label); } catch {}
  };

  document.addEventListener("click", syncTabs);
  document.addEventListener("keydown", syncTabs);

  // ExDoc builds the tabsets in its own `exdoc:loaded` listener, which runs after this one.
  window.addEventListener("exdoc:loaded", () => setTimeout(() => {
    let label = null;
    try { label = localStorage.getItem(storageKey); } catch {}
    if (label) selectTab(label);
  }));
}

window.addEventListener("exdoc:loaded", () => {
  for (const link of document.querySelectorAll("a[href^='http']")) {
    if (link.host === location.host) continue;
    link.target = "_blank";
    link.relList.add("noopener", "noreferrer");
  }
});

window.addEventListener("exdoc:loaded", () => {
  for (const table of document.querySelectorAll(".content-inner table")) {
    const [head, ...rows] = table.rows;

    for (const [i, cell] of [...head.cells].entries()) {
      const lengths = rows.map(row => row.cells[i]?.textContent.trim().length ?? 0);
      const average = lengths.reduce((sum, length) => sum + length, 0) / lengths.length;
      cell.style.minWidth = `min(14em, ${Math.round(average)}ch)`;
    }
  }
});
