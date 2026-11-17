"use strict";

const $ = (id) => document.getElementById(id);
const SERVER_NODE = "i=2253";

let state = null;            // last /api/state response
let tags = [];               // [{node_id, description, display_name}]
let settingsLoaded = false;
let tagSignature = "";
let selected = null;         // {item, parentId}
const treeNodes = new Map(); // node_id -> {item, parentId, li, children, expanded, valueEl, checkbox}
const expanded = new Set();  // node_ids of expanded nodes (survives reloads)
let treeLoadedFor = null;    // OPC UA url the tree was loaded for

// ------------------------------------------------------------------ helpers

async function api(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: body !== undefined ? { "Content-Type": "application/json" } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || res.statusText);
  return data;
}

let toastTimer;
function toast(text, isError = false) {
  const el = $("toast");
  el.textContent = text;
  el.className = "toast show" + (isError ? " err" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.className = "toast"), isError ? 5000 : 2500);
}

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") el.className = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null && v !== false) el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) if (c !== null && c !== undefined) el.append(c instanceof Node ? c : String(c));
  return el;
}

function fmt(value) {
  if (value === null || value === undefined) return "–";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return value === "" ? '""' : String(value);
}

function ago(iso) {
  if (!iso) return "–";
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 2) return "just now";
  if (s < 60) return `${Math.floor(s)} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return new Date(iso).toLocaleTimeString();
}

function snake(name) {
  return name
    .replace(/^\d+:/, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .toLowerCase();
}

const ICON = { Object: "O", Variable: "V", Method: "M" };

// --------------------------------------------------------------- state poll

async function poll() {
  try {
    state = await api("GET", "/api/state");
    render();
  } catch (e) {
    $("chip-opcua").className = "chip bad";
  }
}

function render() {
  const { opcua, mqtt } = state;
  $("chip-opcua").className = "chip " + (opcua.connected ? "ok" : opcua.error ? "bad" : "");
  $("chip-opcua").title = opcua.connected ? opcua.url : opcua.error || "disconnected";
  $("chip-mqtt").className = "chip " + (mqtt.connected ? "ok" : mqtt.error ? "bad" : "warn");
  $("chip-mqtt").title = (mqtt.connected ? "connected to " : "not connected: ") + mqtt.target + (mqtt.error ? ` (${mqtt.error})` : "");
  const fwd = state.forwarding;
  $("chip-fwd").className = "chip " + (fwd && state.subscribed ? "ok" : fwd ? "warn" : "");
  $("published").textContent = state.published.toLocaleString();

  const oe = $("opcua-error");
  oe.textContent = opcua.connected ? `Connected to ${opcua.url}` : opcua.error ? `⚠ ${opcua.error} – retrying…` : "Not connected";
  oe.className = "hint" + (opcua.error && !opcua.connected ? " error" : "");
  const me = $("mqtt-error");
  me.textContent = mqtt.connected ? `Connected to ${mqtt.target}` : `⚠ Broker ${mqtt.target} not reachable${mqtt.error ? ` (${mqtt.error})` : ""} – retrying…`;
  me.className = "hint" + (mqtt.connected ? "" : " error");
  $("status-topic").textContent = `LWT: ${mqtt.status_topic}`;

  if (!settingsLoaded) fillSettings(state.settings);

  const btn = $("toggle-fwd");
  btn.textContent = fwd ? "Stop forwarding" : "Start forwarding";
  btn.className = fwd ? "danger" : "primary";

  renderTags();
  renderMessages();

  if (opcua.connected && treeLoadedFor !== opcua.url) loadTree();
  if (!opcua.connected && (treeLoadedFor || !$("tree").childElementCount)) {
    treeLoadedFor = null;
    $("tree").replaceChildren(h("p", { class: "empty" }, "Connect to an OPC UA server to browse its address space."));
  }
}

function fillSettings(s) {
  $("opcua-url").value = s.opcua_url;
  $("mqtt-host").value = s.mqtt_host;
  $("mqtt-port").value = s.mqtt_port;
  $("topic-root").value = s.topic_root;
  $("machine-name").value = s.machine_name;
  $("interval").value = s.publishing_interval_ms;
  $("qos").value = String(s.qos);
  $("retain").checked = s.retain;
  settingsLoaded = true;
}

// --------------------------------------------------------------------- tags

function renderTags() {
  tags = state.tags.map(({ node_id, description, display_name }) => ({ node_id, description, display_name }));
  $("tag-count").textContent = tags.length;
  $("tags-empty").style.display = tags.length ? "none" : "";

  const sig = state.tags.map((t) => t.node_id + "=" + t.description + "@" + t.topic).join("|");
  const body = $("tag-rows");
  if (sig !== tagSignature) {
    tagSignature = sig;
    body.replaceChildren(...state.tags.map(tagRow));
  }
  state.tags.forEach((t, i) => {
    const tr = body.children[i];
    if (!tr) return;
    const live = t.live;
    const valueCell = tr.querySelector(".num");
    const text = live ? fmt(live.value) : "–";
    if (valueCell.textContent !== text) {
      valueCell.textContent = text;
      tr.classList.remove("flash");
      void tr.offsetWidth;
      tr.classList.add("flash");
    }
    tr.querySelector(".updated").textContent = live ? `${ago(live.timestamp)} · ${live.count}×` : state.forwarding ? "waiting…" : "–";
    tr.querySelector(".tag-error").textContent = t.error ? `⚠ ${t.error}` : "";
  });

  // keep tree checkboxes in sync
  const ids = new Set(tags.map((t) => t.node_id));
  for (const [id, n] of treeNodes) if (n.checkbox) n.checkbox.checked = ids.has(id);
}

function tagRow(t) {
  const input = h("input", {
    class: "tag-name", value: t.description, title: "MQTT topic level – edit to rename", spellcheck: "false",
    onchange: (e) => renameTag(t.node_id, e.target.value),
    onkeydown: (e) => { if (e.key === "Enter") e.target.blur(); },
  });
  return h("tr", {},
    h("td", {}, input, h("div", { class: "topic", title: t.node_id }, t.topic), h("div", { class: "tag-error" })),
    h("td", { class: "num" }, "–"),
    h("td", { class: "updated" }, "–"),
    h("td", {}, h("button", { class: "ghost", title: "Stop forwarding this tag", onclick: () => removeTag(t.node_id) }, "✕")),
  );
}

async function saveTags(next) {
  try {
    state = await api("PUT", "/api/tags", next);
    render();
  } catch (e) {
    toast(e.message, true);
  }
}

function tagFor(item) {
  return { node_id: item.node_id, description: snake(item.browse_name || item.display_name), display_name: item.display_name };
}

function addTags(items) {
  const have = new Set(tags.map((t) => t.node_id));
  const fresh = items.filter((i) => !have.has(i.node_id)).map(tagFor);
  if (!fresh.length) return toast("All of these tags are already forwarded");
  saveTags([...tags, ...fresh]);
  toast(`Added ${fresh.length} tag${fresh.length > 1 ? "s" : ""}`);
}

function removeTag(nodeId) {
  saveTags(tags.filter((t) => t.node_id !== nodeId));
}

function renameTag(nodeId, name) {
  saveTags(tags.map((t) => (t.node_id === nodeId ? { ...t, description: name } : t)));
}

// ----------------------------------------------------------------- messages

let lastMessageAt = null;
function renderMessages() {
  const msgs = state.messages;
  if (!msgs.length) {
    $("messages").replaceChildren(h("p", { class: "empty" }, "Nothing published yet."));
    lastMessageAt = null;
    return;
  }
  if (msgs[0].at === lastMessageAt) return;
  lastMessageAt = msgs[0].at;
  $("messages").replaceChildren(...msgs.map((m) =>
    h("div", { class: "msg" + (m.ok ? "" : " err"), title: m.ok ? m.payload : "not published – broker offline" },
      h("span", { class: "t" }, new Date(m.at).toLocaleTimeString()),
      h("span", { class: "topic-l" }, m.topic),
      h("span", { class: "payload" }, m.payload))));
}

// --------------------------------------------------------------------- tree

async function loadTree() {
  treeLoadedFor = state.opcua.url;
  treeNodes.clear();
  const root = h("ul", { role: "group" });
  $("tree").replaceChildren(root);
  try {
    const items = await api("GET", "/api/browse");
    root.replaceChildren(...items.map((i) => treeItem(i, null)));
    // re-open what was open before (e.g. after a reconnect)
    for (const id of [...expanded]) if (treeNodes.has(id)) await expand(id);
  } catch (e) {
    root.replaceChildren(h("p", { class: "empty" }, e.message));
    treeLoadedFor = null;
  }
}

function treeItem(item, parentId) {
  const isVar = item.node_class === "Variable";
  const caret = h("span", { class: "caret" }, item.has_children ? "▶" : "");
  const checkbox = isVar
    ? h("input", {
        type: "checkbox", title: "Forward to MQTT",
        onclick: (e) => {
          e.stopPropagation();
          e.target.checked ? addTags([item]) : removeTag(item.node_id);
        },
      })
    : h("span", { class: "pad" });
  checkbox.checked = isVar && tags.some((t) => t.node_id === item.node_id);
  const valueEl = isVar ? h("span", { class: "node-value", title: item.data_type || "" }, fmt(item.value)) : null;
  const addAll = item.node_class === "Object" && item.node_id !== SERVER_NODE
    ? h("button", { class: "add-all", title: "Forward all variables below this node", onclick: (e) => { e.stopPropagation(); addAllBelow(item); } }, "+ all")
    : null;

  const row = h("div", {
    class: "node-row", role: "treeitem", title: item.node_id,
    onclick: () => {
      if (item.has_children) toggle(item.node_id);
      selectNode(item, parentId);
    },
  },
    caret, checkbox,
    h("span", { class: `icon ${ICON[item.node_class] ? item.node_class : "Other"}` }, ICON[item.node_class] || "·"),
    h("span", { class: "node-name" }, item.display_name),
    valueEl, addAll);

  const li = h("li", {}, row);
  treeNodes.set(item.node_id, { item, parentId, li, row, caret, valueEl, checkbox: isVar ? checkbox : null, childList: null });
  return li;
}

async function toggle(id) {
  const n = treeNodes.get(id);
  if (n.childList) {
    n.childList.remove();
    n.childList = null;
    n.caret.classList.remove("open");
    expanded.delete(id);
  } else {
    await expand(id);
  }
}

async function expand(id) {
  const n = treeNodes.get(id);
  if (!n || n.childList) return;
  expanded.add(id);
  n.caret.classList.add("open");
  const ul = h("ul", { role: "group" }, h("li", { class: "loading" }, "loading…"));
  n.childList = ul;
  n.li.append(ul);
  try {
    const items = await api("GET", `/api/browse?node_id=${encodeURIComponent(id)}`);
    ul.replaceChildren(...items.map((i) => treeItem(i, id)));
    for (const child of items) if (expanded.has(child.node_id)) await expand(child.node_id);
  } catch (e) {
    ul.replaceChildren(h("li", { class: "loading" }, e.message));
  }
}

async function refreshTreeValues() {
  if (!state?.opcua.connected || !treeLoadedFor || document.hidden) return;
  for (const id of expanded) {
    const n = treeNodes.get(id);
    if (!n?.childList || id === SERVER_NODE) continue;
    try {
      const items = await api("GET", `/api/browse?node_id=${encodeURIComponent(id)}`);
      for (const i of items) {
        const child = treeNodes.get(i.node_id);
        if (child?.valueEl) child.valueEl.textContent = fmt(i.value);
      }
    } catch { /* ignore – next round */ }
  }
}

async function collectVariables(item, depth = 0) {
  if (depth > 6) return [];
  const children = await api("GET", `/api/browse?node_id=${encodeURIComponent(item.node_id)}`);
  const out = [];
  for (const c of children) {
    if (c.node_class === "Variable") out.push(c);
    else if (c.node_class === "Object" && c.has_children) out.push(...(await collectVariables(c, depth + 1)));
  }
  return out;
}

async function addAllBelow(item) {
  try {
    const vars = await collectVariables(item);
    if (!vars.length) return toast("No variables below this node");
    addTags(vars);
  } catch (e) {
    toast(e.message, true);
  }
}

// ------------------------------------------------------------------ details

async function selectNode(item, parentId) {
  selected = { item, parentId };
  for (const n of treeNodes.values()) n.row.classList.toggle("active", n.item.node_id === item.node_id);
  const box = $("details");
  box.replaceChildren(h("p", { class: "empty" }, "loading…"));
  try {
    const d = await api("GET", `/api/node?node_id=${encodeURIComponent(item.node_id)}`);
    if (selected?.item.node_id !== item.node_id) return;
    box.replaceChildren(...detailsView(d, parentId));
  } catch (e) {
    box.replaceChildren(h("p", { class: "empty" }, e.message));
  }
}

function detailsView(d, parentId) {
  const out = [
    h("h3", {}, d.display_name),
    h("p", { class: "desc" }, d.description || d.node_class),
  ];

  if (d.node_class === "Variable") {
    out.push(h("div", { class: "big-value", id: "detail-value" }, fmt(d.value)));
    const isTag = tags.some((t) => t.node_id === d.node_id);
    out.push(h("p", {},
      isTag
        ? h("button", { onclick: () => { removeTag(d.node_id); setTimeout(() => selectNode(selected.item, selected.parentId), 300); } }, "✕ Stop forwarding")
        : h("button", { class: "primary", onclick: () => { addTags([{ ...selected.item, display_name: d.display_name }]); setTimeout(() => selectNode(selected.item, selected.parentId), 300); } }, "＋ Forward to MQTT")));
  }

  const kv = [["NodeId", d.node_id], ["BrowseName", d.browse_name], ["NodeClass", d.node_class]];
  if (d.node_class === "Variable") {
    kv.push(["DataType", d.data_type], ["Status", d.status], ["Source time", d.source_timestamp || "–"], ["Writable", d.writable ? "yes" : "no"]);
  }
  out.push(h("dl", { class: "kv" }, ...kv.flatMap(([k, v]) => [h("dt", {}, k), h("dd", { id: k === "Source time" ? "detail-ts" : undefined }, v)])));

  if (d.node_class === "Variable" && d.writable && d.writable_type_supported) {
    const input = h("input", { class: "mono", value: d.value ?? "" });
    const result = h("div", { class: "result" });
    out.push(h("form", {
      class: "panel",
      onsubmit: async (e) => {
        e.preventDefault();
        try {
          await api("POST", "/api/write", { node_id: d.node_id, value: input.value });
          result.className = "result ok";
          result.textContent = `✓ written ${input.value}`;
        } catch (err) {
          result.className = "result err";
          result.textContent = err.message;
        }
      },
    }, h("h4", {}, "Write value"), h("div", { class: "row" }, input, h("button", { type: "submit", class: "primary" }, "Write")), result));
  }

  if (d.node_class === "Method") {
    const inputs = d.inputs.map((a) => {
      const input = h("input", {
        class: "mono",
        placeholder: a.data_type,
        type: /Int|Byte|Double|Float/.test(a.data_type) ? "number" : "text",
        step: /Double|Float/.test(a.data_type) ? "any" : undefined,
      });
      return { a, input };
    });
    const result = h("div", { class: "result" });
    out.push(h("form", {
      class: "panel",
      onsubmit: async (e) => {
        e.preventDefault();
        result.className = "result";
        result.textContent = "calling…";
        try {
          const r = await api("POST", "/api/method", {
            object_id: parentId, method_id: d.node_id, args: inputs.map(({ input }) => input.value),
          });
          result.className = "result ok";
          result.textContent = `✓ ${d.display_name}() → ${r.result === null || (Array.isArray(r.result) && !r.result.length) ? "ok" : fmt(r.result)}`;
        } catch (err) {
          result.className = "result err";
          result.textContent = `✕ ${err.message}`;
        }
      },
    },
      h("h4", {}, `Call ${d.display_name}()`),
      ...inputs.map(({ a, input }) => h("label", {}, `${a.name} (${a.data_type})${a.description ? " – " + a.description : ""}`, input)),
      h("button", { type: "submit", class: "primary" }, "Call method"),
      result));
  }
  return out;
}

async function refreshDetails() {
  if (!selected || selected.item.node_class !== "Variable" || !state?.opcua.connected || document.hidden) return;
  try {
    const d = await api("GET", `/api/node?node_id=${encodeURIComponent(selected.item.node_id)}`);
    const v = $("detail-value");
    if (v && selected.item.node_id === d.node_id) {
      v.textContent = fmt(d.value);
      const ts = $("detail-ts");
      if (ts) ts.textContent = d.source_timestamp || "–";
    }
  } catch { /* ignore */ }
}

// -------------------------------------------------------------------- forms

$("opcua-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const url = $("opcua-url").value.trim();
  $("opcua-error").textContent = `Connecting to ${url}…`;
  try {
    state = await api("POST", "/api/opcua/connect", { url });
    treeLoadedFor = null;
    render();
    toast(state.opcua.connected ? "Connected" : `Connection failed: ${state.opcua.error}`, !state.opcua.connected);
  } catch (err) {
    toast(err.message, true);
  }
});

$("disconnect").addEventListener("click", async () => {
  state = await api("POST", "/api/opcua/disconnect");
  render();
});

$("mqtt-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    state = await api("PUT", "/api/settings", {
      mqtt_host: $("mqtt-host").value.trim(),
      mqtt_port: Number($("mqtt-port").value),
      topic_root: $("topic-root").value.trim(),
      machine_name: $("machine-name").value.trim(),
      publishing_interval_ms: Number($("interval").value),
      qos: Number($("qos").value),
      retain: $("retain").checked,
    });
    fillSettings(state.settings);
    render();
    toast("Settings saved");
  } catch (err) {
    toast(err.message, true);
  }
});

$("toggle-fwd").addEventListener("click", async () => {
  try {
    state = await api("POST", state.forwarding ? "/api/forwarding/stop" : "/api/forwarding/start");
    render();
  } catch (err) {
    toast(err.message, true);
  }
});

$("clear-tags").addEventListener("click", () => {
  if (tags.length && confirm(`Stop forwarding all ${tags.length} tags?`)) saveTags([]);
});

$("reload-tree").addEventListener("click", () => {
  treeLoadedFor = null;
  if (state?.opcua.connected) loadTree();
});

// --------------------------------------------------------------------- boot

poll();
setInterval(poll, 1000);
setInterval(refreshTreeValues, 3000);
setInterval(refreshDetails, 2000);
