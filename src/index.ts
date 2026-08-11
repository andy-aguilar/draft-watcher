import { DurableObject } from "cloudflare:workers";

type DraftSummary = {
  draftId: string;
  label: string | null;
  createdAt: string;
  updatedAt: string;
};

type WatcherStatus = {
  draftId: string;
  label: string | null;
  isPolling: boolean;
  pollIntervalSeconds: number;
  lastPollAt: string | null;
  nextPollAt: string | null;
  lastError: string | null;
  lastPickCount: number;
  lastKnownPickNumber: number | null;
  lastEventAt: string | null;
  picks: DraftPick[];
};

type WatcherEvent = {
  id: number;
  at: string;
  type: string;
  message: string;
  details: unknown;
};

type DraftPick = {
  pickNo: number | null;
  round: number | null;
  draftSlot: number | null;
  playerId: string | null;
  pickedBy: string | null;
  rosterId: number | null;
  firstName: string | null;
  lastName: string | null;
  fullName: string;
  position: string | null;
  team: string | null;
};

type SleeperPick = {
  pick_no?: number;
  round?: number;
  draft_slot?: number;
  player_id?: string;
  picked_by?: string;
  roster_id?: number;
  metadata?: Record<string, unknown>;
};

type StartOptions = {
  label?: string | null;
  pollIntervalSeconds?: number;
};

const DEFAULT_POLL_INTERVAL_SECONDS = 15;
const MIN_POLL_INTERVAL_SECONDS = 10;
const MAX_POLL_INTERVAL_SECONDS = 300;
const SLEEPER_API_BASE = "https://api.sleeper.app/v1";

export class Registry extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS drafts (
          draft_id TEXT PRIMARY KEY,
          label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      `);
    });
  }

  registerDraft(draftId: string, label: string | null): DraftSummary {
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(
      `INSERT INTO drafts (draft_id, label, created_at, updated_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(draft_id) DO UPDATE SET
         label = COALESCE(excluded.label, drafts.label),
         updated_at = excluded.updated_at`,
      draftId,
      label,
      now,
      now,
    );

    return this.getDraft(draftId);
  }

  getDraft(draftId: string): DraftSummary {
    return this.ctx.storage.sql
      .exec<{
        draft_id: string;
        label: string | null;
        created_at: string;
        updated_at: string;
      }>(
        "SELECT draft_id, label, created_at, updated_at FROM drafts WHERE draft_id = ?",
        draftId,
      )
      .toArray()
      .map(rowToDraftSummary)[0];
  }

  listDrafts(): DraftSummary[] {
    return this.ctx.storage.sql
      .exec<{
        draft_id: string;
        label: string | null;
        created_at: string;
        updated_at: string;
      }>("SELECT draft_id, label, created_at, updated_at FROM drafts ORDER BY updated_at DESC")
      .toArray()
      .map(rowToDraftSummary);
  }
}

export class DraftWatcher extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          at TEXT NOT NULL,
          type TEXT NOT NULL,
          message TEXT NOT NULL,
          details TEXT
        )
      `);
    });
  }

  async start(draftId: string, options: StartOptions = {}): Promise<WatcherStatus> {
    const current = await this.getStatus(draftId);
    const pollIntervalSeconds = clampPollInterval(options.pollIntervalSeconds);
    const status: WatcherStatus = {
      ...current,
      draftId,
      label: normalizeLabel(options.label) ?? current.label,
      isPolling: true,
      pollIntervalSeconds,
      nextPollAt: new Date().toISOString(),
      lastError: null,
    };

    await this.saveStatus(status);
    await this.ctx.storage.setAlarm(Date.now());
    this.appendEvent("watcher.started", "Polling started", {
      draftId,
      pollIntervalSeconds,
      label: status.label,
    });
    return status;
  }

  async stop(draftId: string): Promise<WatcherStatus> {
    const status = await this.getStatus(draftId);
    const stopped: WatcherStatus = {
      ...status,
      draftId,
      isPolling: false,
      nextPollAt: null,
    };

    await this.saveStatus(stopped);
    await this.ctx.storage.deleteAlarm();
    this.appendEvent("watcher.stopped", "Polling stopped", { draftId });
    return stopped;
  }

  async status(draftId: string): Promise<WatcherStatus> {
    return this.getStatus(draftId);
  }

  async events(limit = 50): Promise<WatcherEvent[]> {
    const boundedLimit = Math.max(1, Math.min(limit, 100));
    return this.ctx.storage.sql
      .exec<{ id: number; at: string; type: string; message: string; details: string | null }>(
        "SELECT id, at, type, message, details FROM events ORDER BY id DESC LIMIT ?",
        boundedLimit,
      )
      .toArray()
      .map((row) => ({
        id: row.id,
        at: row.at,
        type: row.type,
        message: row.message,
        details: row.details ? JSON.parse(row.details) : null,
      }));
  }

  async pollNow(draftId: string): Promise<WatcherStatus> {
    return this.poll(draftId, false);
  }

  async alarm(): Promise<void> {
    const status = await this.ctx.storage.get<WatcherStatus>("status");
    if (!status?.isPolling) {
      return;
    }

    await this.poll(status.draftId, true);
  }

  private async poll(draftId: string, reschedule: boolean): Promise<WatcherStatus> {
    const before = await this.getStatus(draftId);
    const now = new Date();

    try {
      const sleeperPicks = await fetchSleeperPicks(draftId);
      const picks = normalizeSleeperPicks(sleeperPicks);
      const lastPickNumber = getLastPickNumber(picks);
      const pickCount = picks.length;
      const changed =
        pickCount !== before.lastPickCount || lastPickNumber !== before.lastKnownPickNumber;
      const nextPollAt =
        before.isPolling || reschedule
          ? new Date(now.getTime() + before.pollIntervalSeconds * 1000).toISOString()
          : null;
      const status: WatcherStatus = {
        ...before,
        draftId,
        lastPollAt: now.toISOString(),
        nextPollAt,
        lastError: null,
        lastPickCount: pickCount,
        lastKnownPickNumber: lastPickNumber,
        lastEventAt: changed ? now.toISOString() : before.lastEventAt,
        picks,
      };

      await this.saveStatus(status);
      this.appendEvent(changed ? "poll.changed" : "poll.ok", changed ? "Draft picks changed" : "Poll completed", {
        draftId,
        pickCount,
        lastPickNumber,
      });

      if (status.isPolling && reschedule) {
        await this.ctx.storage.setAlarm(Date.parse(status.nextPollAt ?? now.toISOString()));
      }

      return status;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown polling error";
      const nextPollAt =
        before.isPolling || reschedule
          ? new Date(now.getTime() + before.pollIntervalSeconds * 1000).toISOString()
          : null;
      const status: WatcherStatus = {
        ...before,
        draftId,
        lastPollAt: now.toISOString(),
        nextPollAt,
        lastError: message,
      };

      await this.saveStatus(status);
      this.appendEvent("poll.error", "Poll failed", { draftId, error: message });

      if (status.isPolling && reschedule) {
        await this.ctx.storage.setAlarm(Date.parse(status.nextPollAt ?? now.toISOString()));
      }

      return status;
    }
  }

  private async getStatus(draftId: string): Promise<WatcherStatus> {
    const stored = await this.ctx.storage.get<WatcherStatus>("status");
    if (stored) {
      return { ...stored, draftId, picks: stored.picks ?? [] };
    }

    return {
      draftId,
      label: null,
      isPolling: false,
      pollIntervalSeconds: DEFAULT_POLL_INTERVAL_SECONDS,
      lastPollAt: null,
      nextPollAt: null,
      lastError: null,
      lastPickCount: 0,
      lastKnownPickNumber: null,
      lastEventAt: null,
      picks: [],
    };
  }

  private async saveStatus(status: WatcherStatus): Promise<void> {
    await this.ctx.storage.put("status", status);
  }

  private appendEvent(type: string, message: string, details: Record<string, unknown>): void {
    this.ctx.storage.sql.exec(
      "INSERT INTO events (at, type, message, details) VALUES (?, ?, ?, ?)",
      new Date().toISOString(),
      type,
      message,
      JSON.stringify(details),
    );
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    try {
      if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/dashboard")) {
        return html(renderDashboard());
      }

      if (request.method === "GET" && url.pathname === "/api/status") {
        const registry = env.REGISTRY.getByName("registry");
        const drafts = await registry.listDrafts();
        const statuses = await Promise.all(
          drafts.map((draft) => env.DRAFT_WATCHER.getByName(draft.draftId).status(draft.draftId)),
        );
        return json({ service: "draft-watcher", drafts: statuses });
      }

      const route = matchDraftRoute(url.pathname);
      if (!route) {
        return json({ error: "Not found" }, 404);
      }

      const watcher = env.DRAFT_WATCHER.getByName(route.draftId);

      if (request.method === "GET" && route.action === "status") {
        return json(await watcher.status(route.draftId));
      }

      if (request.method === "GET" && route.action === "events") {
        const limit = Number(url.searchParams.get("limit") ?? "50");
        return json({ events: await watcher.events(Number.isFinite(limit) ? limit : 50) });
      }

      if (request.method === "POST" && route.action === "start") {
        const body = await readJson<StartOptions>(request);
        const label = normalizeLabel(body.label);
        const registry = env.REGISTRY.getByName("registry");
        await registry.registerDraft(route.draftId, label);
        return json(await watcher.start(route.draftId, { ...body, label }));
      }

      if (request.method === "POST" && route.action === "stop") {
        return json(await watcher.stop(route.draftId));
      }

      if (request.method === "POST" && route.action === "poll") {
        const registry = env.REGISTRY.getByName("registry");
        await registry.registerDraft(route.draftId, null);
        return json(await watcher.pollNow(route.draftId));
      }

      return json({ error: "Method not allowed" }, 405);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unexpected error";
      console.error(JSON.stringify({ level: "error", message, path: url.pathname }));
      return json({ error: message }, 500);
    }
  },
};

function rowToDraftSummary(row: {
  draft_id: string;
  label: string | null;
  created_at: string;
  updated_at: string;
}): DraftSummary {
  return {
    draftId: row.draft_id,
    label: row.label,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function fetchSleeperPicks(draftId: string): Promise<SleeperPick[]> {
  const response = await fetch(`${SLEEPER_API_BASE}/draft/${encodeURIComponent(draftId)}/picks`, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok) {
    throw new Error(`Sleeper returned ${response.status}`);
  }

  const body = await response.json();
  if (!Array.isArray(body)) {
    throw new Error("Sleeper returned an unexpected picks payload");
  }

  return body as SleeperPick[];
}

function normalizeSleeperPicks(picks: SleeperPick[]): DraftPick[] {
  return picks
    .map((pick) => {
      const firstName = stringFromMetadata(pick.metadata, "first_name");
      const lastName = stringFromMetadata(pick.metadata, "last_name");
      const position = stringFromMetadata(pick.metadata, "position");
      const team = stringFromMetadata(pick.metadata, "team");
      const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();

      return {
        pickNo: numberOrNull(pick.pick_no),
        round: numberOrNull(pick.round),
        draftSlot: numberOrNull(pick.draft_slot),
        playerId: typeof pick.player_id === "string" ? pick.player_id : null,
        pickedBy: typeof pick.picked_by === "string" ? pick.picked_by : null,
        rosterId: numberOrNull(pick.roster_id),
        firstName,
        lastName,
        fullName: fullName || "Unknown player",
        position,
        team,
      };
    })
    .sort((a, b) => (a.pickNo ?? Number.MAX_SAFE_INTEGER) - (b.pickNo ?? Number.MAX_SAFE_INTEGER));
}

function stringFromMetadata(metadata: Record<string, unknown> | undefined, key: string): string | null {
  const value = metadata?.[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function numberOrNull(value: number | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getLastPickNumber(picks: DraftPick[]): number | null {
  const pickNumbers = picks
    .map((pick) => pick.pickNo)
    .filter((pickNo): pickNo is number => typeof pickNo === "number");
  return pickNumbers.length > 0 ? Math.max(...pickNumbers) : null;
}

function matchDraftRoute(pathname: string): { draftId: string; action: string } | null {
  const match = /^\/api\/drafts\/([^/]+)\/(status|events|start|stop|poll)$/.exec(pathname);
  if (!match) {
    return null;
  }

  return {
    draftId: decodeURIComponent(match[1]),
    action: match[2],
  };
}

async function readJson<T>(request: Request): Promise<T> {
  if (!request.headers.get("content-type")?.includes("application/json")) {
    return {} as T;
  }

  return (await request.json()) as T;
}

function clampPollInterval(value: number | undefined): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return DEFAULT_POLL_INTERVAL_SECONDS;
  }

  return Math.max(MIN_POLL_INTERVAL_SECONDS, Math.min(MAX_POLL_INTERVAL_SECONDS, Math.floor(value)));
}

function normalizeLabel(label: string | undefined | null): string | null {
  const trimmed = label?.trim();
  return trimmed ? trimmed.slice(0, 80) : null;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function html(body: string): Response {
  return new Response(body, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function renderDashboard(): string {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>draft-watcher</title>
    <style>
      :root {
        color-scheme: light dark;
        --bg: #f8f7f2;
        --panel: #ffffff;
        --text: #22221f;
        --muted: #696a63;
        --border: #ddd8cc;
        --accent: #1f7a6d;
        --accent-strong: #14544b;
        --danger: #b73434;
        --ok: #17803d;
      }
      @media (prefers-color-scheme: dark) {
        :root {
          --bg: #121412;
          --panel: #1c1f1c;
          --text: #f2f0e8;
          --muted: #aaa69a;
          --border: #363931;
          --accent: #57b5a7;
          --accent-strong: #8fd4c9;
          --danger: #ff8a8a;
          --ok: #77d391;
        }
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      main {
        width: min(1120px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 28px 0 40px;
      }
      header {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 22px;
      }
      h1 {
        margin: 0;
        font-size: clamp(2rem, 5vw, 4.2rem);
        line-height: 0.95;
        letter-spacing: 0;
      }
      p { margin: 0; color: var(--muted); }
      button, input {
        border: 1px solid var(--border);
        border-radius: 6px;
        font: inherit;
      }
      button {
        cursor: pointer;
        min-height: 40px;
        padding: 0 12px;
        color: #fff;
        background: var(--accent);
        border-color: var(--accent);
        font-weight: 650;
      }
      button.secondary {
        color: var(--text);
        background: transparent;
      }
      button.danger {
        background: var(--danger);
        border-color: var(--danger);
      }
      input {
        min-height: 40px;
        padding: 0 10px;
        background: var(--panel);
        color: var(--text);
      }
      .panel {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 16px;
      }
      .toolbar {
        display: grid;
        grid-template-columns: minmax(180px, 1fr) minmax(140px, 240px) 100px auto auto;
        gap: 10px;
        margin-bottom: 18px;
      }
      .summary {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 18px;
      }
      .metric {
        min-height: 86px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
      }
      .metric span {
        color: var(--muted);
        font-size: 0.82rem;
      }
      .metric strong {
        font-size: 1.45rem;
      }
      .drafts {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 12px;
      }
      .draft {
        display: grid;
        gap: 12px;
      }
      .draft h2 {
        margin: 0;
        font-size: 1rem;
      }
      .status-line {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
      }
      .badge {
        display: inline-flex;
        align-items: center;
        min-height: 28px;
        border-radius: 999px;
        padding: 0 10px;
        border: 1px solid var(--border);
        color: var(--muted);
        font-size: 0.78rem;
        font-weight: 700;
      }
      .badge.ok { color: var(--ok); }
      .badge.err { color: var(--danger); }
      dl {
        display: grid;
        grid-template-columns: 112px 1fr;
        gap: 6px 10px;
        margin: 0;
      }
      dt { color: var(--muted); }
      dd { margin: 0; overflow-wrap: anywhere; }
      pre {
        max-height: 220px;
        overflow: auto;
        margin: 0;
        padding: 12px;
        border-radius: 6px;
        background: color-mix(in srgb, var(--panel) 88%, #000 12%);
        border: 1px solid var(--border);
        font-size: 0.78rem;
      }
      details {
        border: 1px solid var(--border);
        border-radius: 6px;
        overflow: hidden;
      }
      summary {
        cursor: pointer;
        min-height: 40px;
        padding: 10px 12px;
        color: var(--accent-strong);
        font-weight: 750;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.86rem;
      }
      th, td {
        border-top: 1px solid var(--border);
        padding: 8px 10px;
        text-align: left;
        vertical-align: top;
      }
      th {
        color: var(--muted);
        font-size: 0.72rem;
        text-transform: uppercase;
      }
      @media (max-width: 780px) {
        header { align-items: flex-start; flex-direction: column; }
        .toolbar { grid-template-columns: 1fr; }
        .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        table, thead, tbody, tr, th, td { display: block; }
        thead { display: none; }
        td { border-top: 0; padding: 4px 10px; }
        td:first-child { border-top: 1px solid var(--border); padding-top: 10px; font-weight: 750; }
        td:last-child { padding-bottom: 10px; }
      }
    </style>
  </head>
  <body>
    <main>
      <header>
        <div>
          <h1>draft-watcher</h1>
          <p>Live polling status for Sleeper drafts.</p>
        </div>
        <button class="secondary" id="refresh">Refresh</button>
      </header>

      <section class="panel">
        <form class="toolbar" id="start-form">
          <input id="draft-id" name="draftId" placeholder="Sleeper draft ID" autocomplete="off" required />
          <input id="label" name="label" placeholder="Label" autocomplete="off" />
          <input id="interval" name="interval" type="number" min="10" max="300" value="15" aria-label="Polling interval seconds" />
          <button type="submit">Start</button>
          <button type="button" class="secondary" id="poll-now">Poll</button>
        </form>
        <div class="summary" id="summary"></div>
        <div class="drafts" id="drafts"></div>
      </section>
    </main>

    <script>
      const summary = document.getElementById("summary");
      const drafts = document.getElementById("drafts");
      const form = document.getElementById("start-form");
      const draftId = document.getElementById("draft-id");
      const label = document.getElementById("label");
      const interval = document.getElementById("interval");

      async function api(path, options) {
        const res = await fetch(path, options);
        if (!res.ok) throw new Error(await res.text());
        return res.json();
      }

      function fmt(value) {
        if (!value) return "never";
        return new Date(value).toLocaleString();
      }

      function esc(value) {
        return String(value ?? "").replace(/[&<>"']/g, (char) => ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[char]);
      }

      function renderPicks(picks) {
        if (!picks || picks.length === 0) {
          return '<p style="padding: 0 12px 12px;">No picks recorded yet. Click Poll after entering a draft ID.</p>';
        }

        return '<table><thead><tr><th>Pick</th><th>Player</th><th>Pos</th><th>Team</th><th>Round</th><th>Slot</th></tr></thead><tbody>'
          + picks.map((pick) => '<tr>'
            + '<td>' + esc(pick.pickNo ?? "") + '</td>'
            + '<td>' + esc(pick.fullName) + '</td>'
            + '<td>' + esc(pick.position || "") + '</td>'
            + '<td>' + esc(pick.team || "") + '</td>'
            + '<td>' + esc(pick.round ?? "") + '</td>'
            + '<td>' + esc(pick.draftSlot ?? "") + '</td>'
          + '</tr>').join("")
          + '</tbody></table>';
      }

      function renderStatus(data) {
        const items = data.drafts || [];
        const active = items.filter((draft) => draft.isPolling).length;
        const errors = items.filter((draft) => draft.lastError).length;
        summary.innerHTML = [
          ["Registered", items.length],
          ["Polling", active],
          ["Errors", errors],
          ["Last refresh", new Date().toLocaleTimeString()],
        ].map(([k, v]) => '<div class="panel metric"><span>' + k + '</span><strong>' + v + '</strong></div>').join("");

        drafts.innerHTML = items.map((draft) => '<article class="panel draft">'
          + '<div class="status-line"><h2>' + esc(draft.label || draft.draftId) + '</h2>'
          + '<span class="badge ' + (draft.lastError ? 'err' : draft.isPolling ? 'ok' : '') + '">' + (draft.lastError ? 'error' : draft.isPolling ? 'polling' : 'stopped') + '</span></div>'
          + '<dl>'
          + '<dt>Draft ID</dt><dd>' + esc(draft.draftId) + '</dd>'
          + '<dt>Last poll</dt><dd>' + esc(fmt(draft.lastPollAt)) + '</dd>'
          + '<dt>Next poll</dt><dd>' + esc(fmt(draft.nextPollAt)) + '</dd>'
          + '<dt>Picks</dt><dd>' + esc(draft.lastPickCount) + '</dd>'
          + '<dt>Last pick</dt><dd>' + esc(draft.lastKnownPickNumber || 'none') + '</dd>'
          + '<dt>Error</dt><dd>' + esc(draft.lastError || 'none') + '</dd>'
          + '</dl>'
          + '<div class="status-line">'
          + '<button data-poll="' + esc(draft.draftId) + '" class="secondary">Poll</button>'
          + '<button data-stop="' + esc(draft.draftId) + '" class="danger">Stop</button>'
          + '</div>'
          + '<details><summary>Picks / players</summary>' + renderPicks(draft.picks) + '</details>'
          + '<details><summary>Recent events</summary><pre id="events-' + esc(draft.draftId) + '">Loading events...</pre></details>'
          + '</article>').join("") || '<p>No drafts registered yet.</p>';

        for (const draft of items) loadEvents(draft.draftId);
      }

      async function load() {
        renderStatus(await api("/api/status"));
      }

      async function loadEvents(id) {
        const el = document.getElementById("events-" + id);
        if (!el) return;
        const data = await api("/api/drafts/" + encodeURIComponent(id) + "/events?limit=8");
        el.textContent = data.events.map((event) => event.at + "  " + event.type + "  " + event.message).join("\\n") || "No events yet.";
      }

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        await api("/api/drafts/" + encodeURIComponent(draftId.value) + "/start", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ label: label.value, pollIntervalSeconds: Number(interval.value) }),
        });
        await load();
      });

      document.getElementById("poll-now").addEventListener("click", async () => {
        if (!draftId.value) return;
        await api("/api/drafts/" + encodeURIComponent(draftId.value) + "/poll", { method: "POST" });
        await load();
      });

      document.getElementById("refresh").addEventListener("click", load);
      drafts.addEventListener("click", async (event) => {
        const target = event.target;
        if (!(target instanceof HTMLButtonElement)) return;
        if (target.dataset.poll) {
          await api("/api/drafts/" + encodeURIComponent(target.dataset.poll) + "/poll", { method: "POST" });
        }
        if (target.dataset.stop) {
          await api("/api/drafts/" + encodeURIComponent(target.dataset.stop) + "/stop", { method: "POST" });
        }
        await load();
      });

      load();
      setInterval(load, 10000);
    </script>
  </body>
</html>`;
}
