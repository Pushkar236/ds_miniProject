// TicketRush dashboard: polls REST for state, listens on a WebSocket for live events.
const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const audits = {};

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  return { status: res.status, data, handledBy: res.headers.get("X-Handled-By") };
}

// ---------- events ----------
function renderEvents(events) {
  $("#events").innerHTML = events.map((e) => {
    const pct = (n) => `${(100 * n / e.capacity).toFixed(2)}%`;
    const a = audits[e.id];
    return `<article class="event">
      <div><h4>${esc(e.name)}</h4>
        <div class="meta">${esc(e.venue)} &middot; ₹${(e.price_cents / 100).toLocaleString("en-IN")} &middot; #${e.id}</div></div>
      <div class="bar" title="green: confirmed, amber: pending payment">
        <span class="c" style="width:${pct(e.confirmed)}"></span><span class="p" style="width:${pct(e.pending)}"></span>
      </div>
      <div class="counts">
        <div><b>${e.remaining ?? "?"}</b><span class="k">left</span></div>
        <div><b>${e.pending}</b><span class="k">pending</span></div>
        <div><b>${e.confirmed}</b><span class="k">sold</span></div>
        <div><b>${e.capacity}</b><span class="k">capacity</span></div>
      </div>
      <div class="actions">
        <button class="small" data-book="${e.id}">Book 1</button>
        <button class="small ghost" data-audit="${e.id}">Audit</button>
        <button class="small ghost" data-reset="${e.id}">Reset</button>
      </div>
      ${a ? `<div class="audit ${a.consistent && !a.oversold ? "good" : "badc"}">
        ${a.oversold ? "OVERSOLD!" : a.consistent ? "Consistent" : "Inconsistent (traffic in flight?)"}:
        ${a.redis_remaining} left + ${a.confirmed} sold + ${a.pending} pending = ${a.redis_remaining + a.confirmed + a.pending} / ${a.capacity}</div>` : ""}
    </article>`;
  }).join("");

  const sel = $("#sale-event");
  const current = sel.value;
  sel.innerHTML = events.map((e) => `<option value="${e.id}">${esc(e.name)}</option>`).join("");
  if (current) sel.value = current;
}

async function refreshEvents() {
  const { data } = await api("GET", "/api/events");
  if (Array.isArray(data)) renderEvents(data);
}

$("#events").addEventListener("click", async (ev) => {
  const b = ev.target.closest("button");
  if (!b) return;
  if (b.dataset.book) {
    const r = await api("POST", "/api/bookings", { event_id: +b.dataset.book, user_id: `web-${crypto.randomUUID().slice(0, 8)}`, qty: 1 });
    addFeed({ type: r.status === 202 ? "YOU_BOOKED" : `HTTP_${r.status}`, instance: r.handledBy, ...(r.data || {}) });
  } else if (b.dataset.audit) {
    audits[b.dataset.audit] = (await api("GET", `/api/events/${b.dataset.audit}/audit`)).data;
  } else if (b.dataset.reset) {
    delete audits[b.dataset.reset];
    await api("POST", `/api/events/${b.dataset.reset}/reset`);
  }
  refreshEvents();
});

$("#new-event").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  await api("POST", "/api/events", { name: f.get("name"), venue: f.get("venue"), capacity: +f.get("capacity"), price_cents: 99900 });
  ev.target.reset();
  refreshEvents();
});

// ---------- flash sale ----------
$("#sale-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  const eventId = +f.get("event"), total = +f.get("requests"), parallel = +f.get("parallel");
  const btn = $("#sale-btn");
  btn.disabled = true;
  const statuses = {}, byReplica = {};
  let sent = 0, done = 0;
  const started = performance.now();
  const render = () => {
    const secs = (performance.now() - started) / 1000;
    $("#sale-result").innerHTML = `
      <div class="row"><span class="pill">${done}/${total} done</span><span class="pill">${(done / secs).toFixed(0)} req/s</span>
        ${Object.entries(statuses).map(([k, v]) => `<span class="pill">${k === "202" ? "202 reserved" : k === "409" ? "409 sold out" : k}: ${v}</span>`).join("")}</div>
      <div class="row">${Object.entries(byReplica).sort().map(([k, v]) => `<span class="pill">${esc(k)}: ${v}</span>`).join("")}</div>`;
  };
  async function lane() {
    while (sent < total) {
      const i = sent++;
      const r = await api("POST", "/api/bookings", { event_id: eventId, user_id: `rush-${Date.now()}-${i}`, qty: 1 }).catch(() => ({ status: "error" }));
      statuses[r.status] = (statuses[r.status] || 0) + 1;
      if (r.handledBy) byReplica[r.handledBy] = (byReplica[r.handledBy] || 0) + 1;
      done++;
      if (done % 10 === 0) render();
    }
  }
  await Promise.all(Array.from({ length: parallel }, lane));
  render();
  btn.disabled = false;
  refreshEvents();
});

// ---------- cluster ----------
function renderReplicas(el, rows, label) {
  if (!rows.length) { el.innerHTML = `<div class="empty">no ${label} seen yet</div>`; return; }
  const max = Math.max(1, ...rows.map((r) => r.handled));
  el.innerHTML = rows.map((r) => `<div class="replica ${r.alive ? "" : "dead"}">
      <span class="dot ${r.alive ? "on" : "off"}"></span>
      <span class="name"><code>${esc(r.instance)}</code></span>
      <span>${r.handled}${r.rejected !== undefined ? ` <span class="muted">(${r.rejected} sold-out)</span>` : ""}</span>
      <span class="share"><i style="width:${(100 * r.handled / max).toFixed(1)}%"></i></span>
    </div>`).join("");
}

async function refreshCluster() {
  const { data } = await api("GET", "/api/cluster");
  if (!data) return;
  $("#gateway-id").textContent = data.gateway;
  const q = data.queue;
  $("#queue").innerHTML = `
    <div class="stat"><b>${q.lag}</b><span>waiting in stream</span></div>
    <div class="stat"><b>${q.pending}</b><span>in flight (unacked)</span></div>
    <div class="stat"><b>${q.consumers}</b><span>consumers</span></div>`;
  renderReplicas($("#booking-replicas"), data.booking, "booking replicas");
  renderReplicas($("#payment-replicas"), data.payment, "payment workers");
}

$("#reset-stats").addEventListener("click", async () => { await api("POST", "/api/cluster/reset-stats"); refreshCluster(); });

// ---------- live feed ----------
const feedQueue = [];
function addFeed(m) { feedQueue.push(m); }
function describe(m) {
  switch (m.type) {
    case "ORDER_RESERVED": return `${m.order_id} reserved ${m.qty} seat(s) on event #${m.event_id} (${m.remaining} left)`;
    case "ORDER_CONFIRMED": return `${m.order_id} payment confirmed`;
    case "ORDER_FAILED": return `${m.order_id} payment failed: ${m.reason}, seats released`;
    case "ORDER_EXPIRED": return `${m.order_id} hold expired, seats released`;
    case "ORDER_RECLAIMED": return `${m.order_id} taken over from a crashed worker`;
    case "EVENT_RESET": return `event #${m.event_id} reset`;
    case "EVENT_CREATED": return `event #${m.event_id} "${m.name}" created`;
    case "YOU_BOOKED": return `your booking ${m.order_id} is pending payment`;
    default: return `${m.type} ${m.detail || ""}`;
  }
}
setInterval(() => {
  if (!feedQueue.length) return;
  const items = feedQueue.splice(0, feedQueue.length).slice(-80).reverse();
  const html = items.map((m) => {
    const t = new Date((m.ts || Date.now() / 1000) * 1000).toLocaleTimeString();
    return `<li><span class="t">${t}</span><span class="who">${esc(m.instance || "")}</span><span class="k-${esc(m.type)}">${esc(describe(m))}</span></li>`;
  }).join("");
  const feed = $("#feed");
  feed.insertAdjacentHTML("afterbegin", html);
  while (feed.children.length > 120) feed.lastElementChild.remove();
}, 250);

function connect() {
  const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
  ws.onopen = () => { $("#ws-dot").className = "dot on"; $("#ws-label").textContent = "live"; };
  ws.onclose = () => { $("#ws-dot").className = "dot off"; $("#ws-label").textContent = "reconnecting…"; setTimeout(connect, 2000); };
  ws.onmessage = (e) => addFeed(JSON.parse(e.data));
}

connect();
refreshEvents();
refreshCluster();
setInterval(refreshEvents, 1500);
setInterval(refreshCluster, 1500);
