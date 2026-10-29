// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

"use strict";

const state = {
  online: false,
  test: null,
  runs: [],
  run: null, // { run, events }
  constraints: [], // [{ before: HitId, after: HitId, post: bool }]
  pending: null, // { hit: HitId, post: bool }
  selected: null, // event
  filters: { comm: false, idle: true, markers: true, text: "" },
};

const $ = (id) => document.getElementById(id);

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

function hitId(e) {
  return { actor: e.actor, point: e.point, hit: e.hit };
}

function sameHit(a, b) {
  return a.actor === b.actor && a.point === b.point && a.hit === b.hit;
}

function fmtHit(h) {
  return `${h.actor}:${h.point}#${h.hit}`;
}

function toast(msg) {
  const t = $("toast");
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { t.hidden = true; }, 3500);
}

async function api(path, body) {
  const opts = body === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
  const resp = await fetch(path, opts);
  const json = await resp.json();
  if (!resp.ok) throw new Error(json.error || resp.statusText);
  return json;
}

// Data model.

function laneKey(e) {
  return `${e.process}\u0000${e.actor}`;
}

function analyze(events) {
  // Communication points: hits by different actors on the same key, at least
  // one of them not a read.
  const byKey = new Map();
  for (const e of events) {
    if (e.event !== "hit" || e.key === undefined) continue;
    if (!byKey.has(e.key)) byKey.set(e.key, []);
    byKey.get(e.key).push(e);
  }
  const comm = new Set();
  for (const hits of byKey.values()) {
    for (const a of hits) {
      for (const b of hits) {
        if (a.actor !== b.actor && !(a.access === "read" && b.access === "read")) {
          comm.add(a);
          comm.add(b);
        }
      }
    }
  }
  return { comm };
}

// Rendering.

function render() {
  renderRuns();
  renderConstraints();
  renderSummary();
  renderLanes();
  renderDetails();
  $("download").disabled = !state.run;
  $("run").textContent = state.constraints.length ? `Run with ${state.constraints.length} constraint(s)` : "Run baseline";
}

function renderRuns() {
  const ul = $("runs");
  ul.innerHTML = "";
  for (const r of state.runs) {
    const li = document.createElement("li");
    if (state.run && state.run.run.id === r.id) li.classList.add("selected");
    const bad = /violation:|: (?!ok)/.test(r.signature);
    li.innerHTML = `
      <div><b>${esc(r.id)}</b> <span class="status ${esc(r.status)}">${esc(r.status)}</span>
      ${bad ? '<span class="status error">error</span>' : ""}</div>
      <div class="label">${esc(r.label || "manual")}</div>`;
    li.title = r.signature;
    li.onclick = () => loadRun(r.id);
    ul.appendChild(li);
  }
}

function renderConstraints() {
  const ul = $("constraints");
  ul.innerHTML = "";
  state.constraints.forEach((c, i) => {
    const li = document.createElement("li");
    const status = state.run?.run?.constraints?.[i];
    const matches = state.run && JSON.stringify(state.run.run.orders[i]) === JSON.stringify(c);
    let badge = "";
    if (status && matches) {
      badge = status.timed_out ? '<span class="status wait-timeout">timeout</span>'
        : (status.waited && status.resumed) ? '<span class="status ok">enforced</span>'
        : '<span class="status injection-failed">not hit</span>';
    }
    li.innerHTML = `<span class="num">${i + 1}</span>
      <span class="text">${esc(fmtHit(c.before))}<br>→ ${esc(fmtHit(c.after))}</span>
      ${badge}`;
    const mode = document.createElement("button");
    mode.className = "secondary small";
    mode.textContent = c.post ? "post" : "pre";
    mode.title = "post: fires when the point's guard drops (after the action). pre: fires on reaching the point.";
    mode.onclick = () => { c.post = !c.post; render(); };
    const del = document.createElement("button");
    del.className = "secondary small";
    del.textContent = "×";
    del.onclick = () => { state.constraints.splice(i, 1); render(); };
    li.append(mode, del);
    ul.appendChild(li);
  });
  const p = $("pending");
  p.hidden = !state.pending;
  if (state.pending) {
    p.innerHTML = `Pending: <b>${esc(fmtHit(state.pending.hit))}</b> (${state.pending.post ? "post" : "pre"}) before … click the hit that must wait. <a href="#" id="cancel-pending">cancel</a>`;
    $("cancel-pending").onclick = (ev) => { ev.preventDefault(); state.pending = null; render(); };
  }
}

function renderSummary() {
  const el = $("run-summary");
  if (!state.run) {
    el.innerHTML = '<p class="hint">No run selected. Press <b>Run baseline</b> to record the test once.</p>';
    return;
  }
  const r = state.run.run;
  const rows = Object.entries(r.outcomes).map(([actor, o]) => {
    const text = o.ok ? `ok ${o.result === null || o.result === undefined ? "" : JSON.stringify(o.result)}` : o.error;
    return `<div class="outcome ${o.ok ? "good" : "bad"}" title="${esc(o.traceback || "")}"><b>${esc(actor)}</b>: ${esc(text)}</div>`;
  });
  for (const v of r.violations) rows.push(`<div class="outcome bad">violation: ${esc(v)}</div>`);
  el.innerHTML = `<div><b>${esc(r.id)}</b> <span class="status ${esc(r.status)}">${esc(r.status)}</span>
    <span class="hint">${esc(r.label || "")} · ${r.duration_s.toFixed(2)} s · ${state.run.events.length} events</span></div>${rows.join("")}`;
}

function visibleEvents() {
  const { comm } = state.analysis;
  const text = state.filters.text.toLowerCase();
  return state.run.events.filter((e) => {
    if (e.event !== "hit") {
      if (!state.filters.markers) return false;
    } else if (state.filters.comm && !comm.has(e)) {
      return false;
    }
    if (text) {
      const hay = `${e.actor} ${e.point} ${e.key ?? ""} ${e.note ?? ""} ${e.span ?? ""}`.toLowerCase();
      if (!hay.includes(text)) return false;
    }
    return true;
  });
}

function renderLanes() {
  const lanes = $("lanes");
  lanes.innerHTML = "";
  $("arrows").innerHTML = "";
  if (!state.run) return;

  const events = visibleEvents();
  const laneOrder = [];
  const laneInfo = new Map();
  for (const e of state.run.events) {
    const k = laneKey(e);
    if (!laneInfo.has(k)) {
      laneInfo.set(k, { process: e.process, actor: e.actor, visible: 0 });
      laneOrder.push(k);
    }
  }
  for (const e of events) laneInfo.get(laneKey(e)).visible++;
  const shown = laneOrder.filter((k) => !state.filters.idle || laneInfo.get(k).visible > 0);
  const col = new Map(shown.map((k, i) => [k, i + 1]));
  lanes.style.gridTemplateColumns = `repeat(${Math.max(shown.length, 1)}, minmax(200px, 1fr))`;

  for (const k of shown) {
    const info = laneInfo.get(k);
    const head = document.createElement("div");
    head.className = "lane-head";
    head.style.gridColumn = col.get(k);
    head.style.gridRow = 1;
    head.innerHTML = `<span class="process">${esc(info.process)}</span>${esc(info.actor)}`;
    lanes.appendChild(head);
  }

  const { comm } = state.analysis;
  const selectedKey = state.selected?.key;
  state.cards = new Map();
  let row = 2;
  for (const e of events) {
    const c = col.get(laneKey(e));
    if (!c) continue;
    const el = document.createElement("div");
    el.style.gridColumn = c;
    el.style.gridRow = row++;
    if (e.event === "hit") {
      el.className = `hit ${e.access ?? ""}`;
      if (selectedKey !== undefined && e.key === selectedKey && e !== state.selected) el.classList.add("shared");
      if (state.selected === e) el.classList.add("selected");
      if (state.pending && sameHit(state.pending.hit, e)) el.classList.add("pending");
      el.innerHTML = `<span class="name">${esc(e.point)}</span>#${e.hit}
        ${e.access ? `<span class="access ${esc(e.access)}">${esc(e.access[0].toUpperCase())}</span>` : ""}
        ${comm.has(e) ? '<span class="comm" title="communication point">⇄</span>' : ""}
        ${e.key !== undefined ? `<span class="key">${esc(e.key)}</span>` : ""}
        ${e.note ? `<span class="note" title="${esc(e.note)}">${esc(e.note)}</span>` : ""}`;
      el.onmouseenter = () => highlightKey(e.key, true);
      el.onmouseleave = () => highlightKey(e.key, false);
      el.onclick = () => clickHit(e);
      el.oncontextmenu = (ev) => { ev.preventDefault(); rightClickHit(e); };
      el.dataset.key = e.key ?? "";
      state.cards.set(`${e.actor}\u0000${e.point}\u0000${e.hit}`, el);
    } else {
      el.className = `marker ${e.event}`;
      const n = e.constraint !== undefined ? ` ${e.constraint + 1}` : "";
      const label = { wait: "⏸ wait", resume: "▶ resume", timeout: "⏱ TIMEOUT", wake: "⚑ wake" }[e.event] ?? e.event;
      el.textContent = `${label}${n} (${e.point}#${e.hit})`;
    }
    lanes.appendChild(el);
  }
  requestAnimationFrame(drawArrows);
}

function highlightKey(key, on) {
  if (key === undefined) return;
  for (const el of $("lanes").querySelectorAll(".hit")) {
    if (el.dataset.key === key) el.classList.toggle("shared", on || (state.selected && state.selected.key === key));
  }
}

function drawArrows() {
  const svg = $("arrows");
  const wrap = $("lanes-wrap").getBoundingClientRect();
  svg.setAttribute("width", wrap.width);
  svg.setAttribute("height", wrap.height);
  let html = `<defs><marker id="head" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--arrow)"/></marker></defs>`;
  state.constraints.forEach((c, i) => {
    const a = state.cards?.get(`${c.before.actor}\u0000${c.before.point}\u0000${c.before.hit}`);
    const b = state.cards?.get(`${c.after.actor}\u0000${c.after.point}\u0000${c.after.hit}`);
    if (!a || !b) return;
    const ra = a.getBoundingClientRect();
    const rb = b.getBoundingClientRect();
    const x1 = ra.left + ra.width / 2 - wrap.left;
    const y1 = (c.post ? ra.bottom : ra.top + 4) - wrap.top;
    const x2 = rb.left + rb.width / 2 - wrap.left;
    const y2 = rb.top - wrap.top;
    const mx = (x1 + x2) / 2;
    html += `<path d="M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}" fill="none" stroke="var(--arrow)" stroke-width="2"
      stroke-dasharray="${c.post ? "" : "5 3"}" marker-end="url(#head)"/>
      <text x="${mx + 4}" y="${(y1 + y2) / 2}" fill="var(--arrow)" font-size="11" font-weight="bold">${i + 1}</text>`;
  });
  svg.innerHTML = html;
}

function clickHit(e) {
  if (state.pending) {
    addConstraint(state.pending.hit, hitId(e), state.pending.post);
    state.pending = null;
  }
  state.selected = e;
  render();
  loadSource(e);
}

function rightClickHit(e) {
  if (state.pending) {
    addConstraint(state.pending.hit, hitId(e), state.pending.post);
    state.pending = null;
  } else {
    state.pending = { hit: hitId(e), post: true };
  }
  render();
}

function addConstraint(before, after, post) {
  if (sameHit(before, after)) {
    toast("A hit cannot wait for itself.");
    return;
  }
  if (before.actor === after.actor) {
    toast("Both hits belong to one actor, so the constraint either holds already or deadlocks until the timeout.");
  }
  state.constraints.push({ before, after, post });
}

function renderDetails() {
  const el = $("hit-details");
  const e = state.selected;
  if (!e) {
    el.innerHTML = '<p class="hint">Select a hit to see its details and source.</p>';
    $("source").innerHTML = "";
    return;
  }
  const rows = [
    ["actor", e.actor], ["process", `${e.process} (pid ${e.pid})`], ["point", `${e.point} #${e.hit}`],
    ["key", e.key], ["access", e.access], ["span", e.span], ["note", e.note],
    ["location", `${e.file}:${e.line}`],
  ].filter(([, v]) => v !== undefined && v !== "");
  el.innerHTML = `<table>${rows.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(v)}</td></tr>`).join("")}</table>
    <div class="buttons server-only-ish">
      <button id="order-post" class="small">Order before… (after it)</button>
      <button id="order-pre" class="small secondary">Order before… (on reach)</button>
    </div>`;
  $("order-post").onclick = () => { state.pending = { hit: hitId(e), post: true }; render(); };
  $("order-pre").onclick = () => { state.pending = { hit: hitId(e), post: false }; render(); };
}

async function loadSource(e) {
  const pre = $("source");
  pre.innerHTML = "";
  if (!state.online || !e.file) return;
  try {
    const src = await api(`/api/source?file=${encodeURIComponent(e.file)}&line=${e.line}`);
    pre.innerHTML = src.lines.map((l, i) => {
      const n = src.start + i;
      return `<span class="line ${n === src.line ? "current" : ""}"><span class="ln">${n}</span>${esc(l)}</span>`;
    }).join("");
    pre.querySelector(".current")?.scrollIntoView({ block: "center" });
  } catch (err) {
    pre.textContent = `source unavailable: ${err.message}`;
  }
}

// Actions.

async function refreshRuns() {
  if (!state.online) return;
  state.runs = await api("/api/runs");
}

async function loadRun(id) {
  const data = await api(`/api/runs/${encodeURIComponent(id)}`);
  setRun(data);
}

function setRun(data) {
  state.run = data;
  state.analysis = analyze(data.events);
  state.constraints = data.run.orders.map((o) => JSON.parse(JSON.stringify(o)));
  state.selected = null;
  state.pending = null;
  render();
}

async function runNow() {
  const btn = $("run");
  btn.disabled = true;
  btn.textContent = "Running…";
  try {
    const label = state.constraints.map((c) => `${fmtHit(c.before)} → ${fmtHit(c.after)}`).join(", ");
    const { id } = await api("/api/runs", { orders: state.constraints, label: label || "baseline" });
    await refreshRuns();
    await loadRun(id);
  } catch (err) {
    toast(`Run failed: ${err.message}`);
  } finally {
    btn.disabled = false;
    render();
  }
}

async function exploreNow() {
  if (!state.run) {
    toast("Select a baseline run first.");
    return;
  }
  try {
    await api("/api/explore", { base: state.run.run.id });
    pollExplore();
  } catch (err) {
    toast(`Exploration failed: ${err.message}`);
  }
}

async function pollExplore() {
  const s = await api("/api/explore");
  const section = $("explore-section");
  section.hidden = s.total === undefined;
  if (s.total !== undefined) {
    $("explore-progress").innerHTML = `${s.done} / ${s.total} schedules${s.running ? " · running" : ""}${s.error ? ` · <span class="status error">${esc(s.error)}</span>` : ""}`;
    const groups = new Map();
    for (const r of s.runs) {
      if (!groups.has(r.signature)) groups.set(r.signature, []);
      groups.get(r.signature).push(r);
    }
    const ul = $("outcomes");
    ul.innerHTML = "";
    for (const [sig, runs] of groups) {
      const li = document.createElement("li");
      const statuses = {};
      for (const r of runs) statuses[r.status] = (statuses[r.status] ?? 0) + 1;
      li.innerHTML = `<div class="signature">${esc(sig)}</div>
        <div class="label">${runs.length} run(s): ${Object.entries(statuses).map(([k, v]) => `${esc(k)}:${v}`).join(" ")}</div>`;
      li.onclick = () => loadRun(runs.find((r) => r.status === "reordered")?.id ?? runs[0].id);
      ul.appendChild(li);
    }
  }
  if (s.running) {
    setTimeout(pollExplore, 1000);
  }
  await refreshRuns();
  renderRuns();
}

function download() {
  const blob = new Blob([JSON.stringify(state.run, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `race-${state.run.run.test}-${state.run.run.id}.json`;
  a.click();
  URL.revokeObjectURL(a.href);
}

async function openFile(file) {
  try {
    const data = JSON.parse(await file.text());
    if (!data.run || !Array.isArray(data.events)) throw new Error("not a race run file");
    setRun(data);
  } catch (err) {
    toast(`Cannot open ${file.name}: ${err.message}`);
  }
}

async function init() {
  $("run").onclick = runNow;
  $("explore").onclick = exploreNow;
  $("download").onclick = download;
  $("open-file").onchange = (ev) => ev.target.files[0] && openFile(ev.target.files[0]);
  $("filter-comm").onchange = (ev) => { state.filters.comm = ev.target.checked; renderLanes(); };
  $("filter-idle").onchange = (ev) => { state.filters.idle = ev.target.checked; renderLanes(); };
  $("filter-markers").onchange = (ev) => { state.filters.markers = ev.target.checked; renderLanes(); };
  $("filter-text").oninput = (ev) => { state.filters.text = ev.target.value; renderLanes(); };
  window.addEventListener("resize", () => requestAnimationFrame(drawArrows));

  try {
    state.test = await api("/api/test");
    state.online = true;
    $("test-name").textContent = state.test.name;
    document.title = `Race Viewer: ${state.test.name}`;
    await refreshRuns();
    if (state.runs.length) await loadRun(state.runs[0].id);
    pollExplore();
  } catch {
    document.body.classList.add("offline");
    $("test-name").textContent = "offline: open a run file";
  }
  render();
}

init();
