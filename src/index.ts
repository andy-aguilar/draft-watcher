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

type HookAttempt = {
  id: number;
  eventId: string;
  hook: string;
  attemptedAt: string;
  accepted: boolean;
  status: number | null;
  error: string | null;
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

type SleeperDraft = {
  draft_id?: string;
  type?: string;
  status?: string;
  draft_order?: Record<string, number>;
  slot_to_roster_id?: Record<string, number>;
  settings?: {
    teams?: number;
    rounds?: number;
  };
};

type StartOptions = {
  label?: string | null;
  pollIntervalSeconds?: number;
};

type RuntimeEnv = Env & {
  DRAFT_WATCHER_PUBLIC_BASE_URL?: string;
  OPENCLAW_BASE_URL?: string;
  WEBHOOK_TOKEN?: string;
  CHANDLER_ROSTER_ID?: string;
};

type ManualHookName =
  | "draft-pick-announce"
  | "round-summary"
  | "chandler-pick"
  | "chandler-fallback";

type PendingChandlerFallback = {
  draftId: string;
  pickSequence: string | number;
  turnStartPickNumber: number;
  turnEndPickNumber: number;
  rosterId: string;
  clockStartedAt: string;
  deadline: string;
  thresholdReachedAt: string;
  sourceVersion: string;
  strategyVersion: string;
  strategySnapshot: string;
};

const DEFAULT_POLL_INTERVAL_SECONDS = 15;
const MIN_POLL_INTERVAL_SECONDS = 10;
const MAX_POLL_INTERVAL_SECONDS = 300;
const SLEEPER_API_BASE = "https://api.sleeper.app/v1";
const CHANDLER_STRATEGY_VERSION = "synthetic-test-v1";
const CHANDLER_ADVICE_STRATEGY =
  "Synthetic test only. Prefer value, maintain positional balance, and plan two rounds ahead. Do not use Chandler private data.";
const CHANDLER_FALLBACK_STRATEGY =
  "Synthetic test only. Prefer value, maintain positional balance, and do not use Chandler private data.";

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

  unregisterDraft(draftId: string): { removed: boolean } {
    const existing = this.getDraft(draftId);
    this.ctx.storage.sql.exec("DELETE FROM drafts WHERE draft_id = ?", draftId);
    return { removed: Boolean(existing) };
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
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS delivered_hooks (
          event_id TEXT PRIMARY KEY,
          hook TEXT NOT NULL,
          delivered_at TEXT NOT NULL,
          status INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS hook_attempts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL,
          hook TEXT NOT NULL,
          attempted_at TEXT NOT NULL,
          accepted INTEGER NOT NULL,
          status INTEGER,
          error TEXT
        )
      `);
    });
  }

  async start(draftId: string, options: StartOptions = {}): Promise<WatcherStatus> {
    const current = await this.getStatus(draftId);
    const [sleeperDraft, sleeperPicks] = await Promise.all([
      fetchSleeperDraft(draftId),
      fetchSleeperPicks(draftId),
    ]);
    const picks = normalizeSleeperPicks(sleeperPicks);
    const lastPickNumber = getLastPickNumber(picks);
    const pollIntervalSeconds = clampPollInterval(options.pollIntervalSeconds);
    const now = new Date().toISOString();
    const nextPollAt = new Date(Date.parse(now) + pollIntervalSeconds * 1000).toISOString();
    const status: WatcherStatus = {
      ...current,
      draftId,
      label: normalizeLabel(options.label) ?? current.label,
      isPolling: true,
      pollIntervalSeconds,
      lastPollAt: now,
      nextPollAt,
      lastError: null,
      lastPickCount: picks.length,
      lastKnownPickNumber: lastPickNumber,
      lastEventAt:
        picks.length !== current.lastPickCount || lastPickNumber !== current.lastKnownPickNumber
          ? now
          : current.lastEventAt,
      picks,
    };

    await this.saveStatus(status);
    await this.scheduleNextAlarm(status);
    this.appendEvent("watcher.started", "Polling started", {
      draftId,
      pollIntervalSeconds,
      label: status.label,
      pickCount: status.lastPickCount,
      lastPickNumber: status.lastKnownPickNumber,
    });
    await this.updateChandlerAutomation(draftId, sleeperDraft, status, now);
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
    await this.ctx.storage.delete("pendingChandlerFallback");
    await this.scheduleNextAlarm(stopped);
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

  async hooks(limit = 50): Promise<HookAttempt[]> {
    const boundedLimit = Math.max(1, Math.min(limit, 100));
    return this.ctx.storage.sql
      .exec<{
        id: number;
        event_id: string;
        hook: string;
        attempted_at: string;
        accepted: number;
        status: number | null;
        error: string | null;
      }>(
        `SELECT id, event_id, hook, attempted_at, accepted, status, error
         FROM hook_attempts ORDER BY id DESC LIMIT ?`,
        boundedLimit,
      )
      .toArray()
      .map((row) => ({
        id: row.id,
        eventId: row.event_id,
        hook: row.hook,
        attemptedAt: row.attempted_at,
        accepted: row.accepted === 1,
        status: row.status,
        error: row.error,
      }));
  }

  async remove(draftId: string): Promise<{ removed: boolean }> {
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.delete("status");
    await this.ctx.storage.delete("pendingChandlerFallback");
    this.appendEvent("watcher.removed", "Watcher removed", { draftId });
    return { removed: true };
  }

  async pollNow(draftId: string): Promise<WatcherStatus> {
    return this.poll(draftId, false);
  }

  async alarm(): Promise<void> {
    const status = await this.ctx.storage.get<WatcherStatus>("status");
    if (!status?.isPolling) {
      await this.ctx.storage.deleteAlarm();
      return;
    }

    const pendingFallback = await this.ctx.storage.get<PendingChandlerFallback>(
      "pendingChandlerFallback",
    );
    const now = Date.now();

    if (pendingFallback && Date.parse(pendingFallback.thresholdReachedAt) <= now) {
      await this.checkChandlerFallback(pendingFallback);
    }

    const refreshed = await this.getStatus(status.draftId);
    if (refreshed.isPolling && (!refreshed.nextPollAt || Date.parse(refreshed.nextPollAt) <= now)) {
      await this.poll(refreshed.draftId, true);
      return;
    }

    await this.scheduleNextAlarm(await this.getStatus(status.draftId));
  }

  private async poll(draftId: string, reschedule: boolean): Promise<WatcherStatus> {
    const before = await this.getStatus(draftId);
    const now = new Date();

    try {
      const [sleeperDraft, sleeperPicks] = await Promise.all([
        fetchSleeperDraft(draftId),
        fetchSleeperPicks(draftId),
      ]);
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

      if (changed) {
        await this.deliverDraftHooks(draftId, sleeperDraft, before, status, now.toISOString());
      }

      if (status.isPolling && reschedule) {
        await this.scheduleNextAlarm(status);
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
        await this.scheduleNextAlarm(status);
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

  private async deliverDraftHooks(
    draftId: string,
    sleeperDraft: SleeperDraft,
    before: WatcherStatus,
    after: WatcherStatus,
    occurredAt: string,
  ): Promise<void> {
    const newPicks = after.picks.filter((pick) => {
      if (typeof pick.pickNo !== "number") return false;
      if (typeof pick.playerId !== "string") return false;
      if (typeof pick.rosterId !== "number") return false;
      return before.lastKnownPickNumber === null || pick.pickNo > before.lastKnownPickNumber;
    });

    for (const pick of newPicks) {
      await this.deliverOpenClawHook("draft-pick-announce", {
        eventId: `draft:${draftId}:pick:${pick.pickNo}`,
        eventType: "PickMade",
        draftId,
        pickNumber: pick.pickNo,
        playerId: pick.playerId,
        rosterId: pick.rosterId,
        sourceVersion: "draft-watcher-v1",
        observedAt: occurredAt,
      });
    }

    await this.deliverCompletedRoundHooks(draftId, sleeperDraft, before.picks, after.picks, occurredAt);
    await this.updateChandlerAutomation(draftId, sleeperDraft, after, occurredAt);
  }

  private async deliverCompletedRoundHooks(
    draftId: string,
    sleeperDraft: SleeperDraft,
    beforePicks: DraftPick[],
    afterPicks: DraftPick[],
    completedAt: string,
  ): Promise<void> {
    const beforeRounds = completedRounds(sleeperDraft, beforePicks);
    const afterRounds = completedRounds(sleeperDraft, afterPicks);

    for (const round of afterRounds) {
      if (beforeRounds.some((before) => before.round === round.round)) continue;

      await this.deliverOpenClawHook("round-summary", {
        eventId: `draft:${draftId}:round:${round.round}:completed`,
        eventType: "RoundCompleted",
        draftId,
        round: round.round,
        firstPickNumber: round.pickStart,
        lastPickNumber: round.pickEnd,
        completedAt,
        sourceVersion: "draft-watcher-v1",
      });
    }
  }

  private async updateChandlerAutomation(
    draftId: string,
    sleeperDraft: SleeperDraft,
    status: WatcherStatus,
    clockStartedAt: string,
  ): Promise<void> {
    const chandlerRosterId = parseOptionalInteger((this.env as RuntimeEnv).CHANDLER_ROSTER_ID);
    const pending = await this.ctx.storage.get<PendingChandlerFallback>("pendingChandlerFallback");
    const pendingTurnEndPickNumber = pending ? pendingFallbackTurnEndPickNumber(pending) : null;

    if (
      pending &&
      pendingTurnEndPickNumber !== null &&
      status.lastKnownPickNumber !== null &&
      status.lastKnownPickNumber >= pendingTurnEndPickNumber
    ) {
      await this.ctx.storage.delete("pendingChandlerFallback");
      this.appendEvent("chandler.fallback.cleared", "Chandler fallback cleared after pick", {
        draftId,
        pickSequence: pending.pickSequence,
        turnEndPickNumber: pendingTurnEndPickNumber,
      });
    }

    if (chandlerRosterId === null) {
      return;
    }

    const currentTurn = chandlerDraftTurn(sleeperDraft, status.picks, chandlerRosterId);
    if (!currentTurn) return;

    const pickSequence = chandlerTurnPickSequence(currentTurn);
    const adviceEventId = `draft:${draftId}:turn:${pickSequence}:chandler-advice`;
    const fallbackEventId = `draft:${draftId}:turn:${pickSequence}:chandler-fallback`;

    if (
      pending &&
      pending.draftId === draftId &&
      pending.pickSequence === pickSequence &&
      pending.rosterId === String(chandlerRosterId)
    ) {
      return;
    }

    if (this.hasDeliveredHook(fallbackEventId)) return;

    const deadline = nextChandlerFallbackAt(new Date(clockStartedAt)).toISOString();
    const rosterId = String(chandlerRosterId);

    const adviceAccepted = await this.deliverOpenClawHook("chandler-pick", {
      testMode: false,
      eventId: adviceEventId,
      eventType: "TurnStarted",
      draftId,
      pickSequence,
      turnStartPickNumber: currentTurn.startPickNumber,
      turnEndPickNumber: currentTurn.endPickNumber,
      turnPickNumbers: currentTurn.pickNumbers,
      rosterId,
      clockStartedAt,
      deadline,
      sourceVersion: "draft-watcher-v1",
      strategyVersion: CHANDLER_STRATEGY_VERSION,
      strategySnapshot: CHANDLER_ADVICE_STRATEGY,
    });

    if (!adviceAccepted) return;

    const fallback: PendingChandlerFallback = {
      draftId,
      pickSequence,
      turnStartPickNumber: currentTurn.startPickNumber,
      turnEndPickNumber: currentTurn.endPickNumber,
      rosterId,
      clockStartedAt,
      deadline,
      thresholdReachedAt: deadline,
      sourceVersion: "draft-watcher-v1",
      strategyVersion: CHANDLER_STRATEGY_VERSION,
      strategySnapshot: CHANDLER_FALLBACK_STRATEGY,
    } as PendingChandlerFallback;
    await this.ctx.storage.put("pendingChandlerFallback", fallback);
    await this.scheduleNextAlarm(status);
    this.appendEvent("chandler.fallback.scheduled", "Chandler fallback scheduled", {
      draftId,
      pickSequence,
      turnStartPickNumber: currentTurn.startPickNumber,
      turnEndPickNumber: currentTurn.endPickNumber,
      thresholdReachedAt: deadline,
    });
  }

  private async checkChandlerFallback(pending: PendingChandlerFallback): Promise<void> {
    try {
      const picks = normalizeSleeperPicks(await fetchSleeperPicks(pending.draftId));
      const lastPickNumber = getLastPickNumber(picks);
      const turnStartPickNumber = pendingFallbackTurnStartPickNumber(pending);
      const turnEndPickNumber = pendingFallbackTurnEndPickNumber(pending);

      if (
        turnEndPickNumber !== null &&
        lastPickNumber !== null &&
        lastPickNumber >= turnEndPickNumber
      ) {
        await this.ctx.storage.delete("pendingChandlerFallback");
        this.appendEvent("chandler.fallback.skipped", "Chandler already picked", {
          draftId: pending.draftId,
          pickSequence: pending.pickSequence,
          turnEndPickNumber,
          lastPickNumber,
        });
        return;
      }

      await this.deliverOpenClawHook("chandler-fallback", {
        testMode: false,
        eventId: `draft:${pending.draftId}:turn:${pending.pickSequence}:chandler-fallback`,
        eventType: "FallbackDue",
        draftId: pending.draftId,
        pickSequence: pending.pickSequence,
        turnStartPickNumber,
        turnEndPickNumber,
        turnPickNumbers:
          turnStartPickNumber !== null && turnEndPickNumber !== null
            ? pickNumbersBetween(turnStartPickNumber, turnEndPickNumber)
            : [],
        rosterId: pending.rosterId,
        clockStartedAt: pending.clockStartedAt,
        deadline: pending.deadline,
        thresholdReachedAt: new Date().toISOString(),
        sourceVersion: "draft-watcher-v1",
        strategyVersion: pending.strategyVersion,
        strategySnapshot: pending.strategySnapshot,
      });

      await this.ctx.storage.delete("pendingChandlerFallback");
    } catch (error) {
      this.appendEvent("chandler.fallback.error", "Chandler fallback check failed", {
        draftId: pending.draftId,
        pickSequence: pending.pickSequence,
        error: error instanceof Error ? error.message : "Unknown fallback error",
      });
    }
  }

  private async deliverOpenClawHook(
    hook: ManualHookName,
    payload: Record<string, unknown>,
  ): Promise<boolean> {
    const eventId = String(payload.eventId ?? "");
    if (!eventId) throw new Error("OpenClaw hook payload is missing eventId");

    if (this.hasDeliveredHook(eventId)) {
      this.appendEvent("webhook.duplicate_skipped", "Webhook already delivered", {
        eventId,
        hook,
      });
      return true;
    }

    try {
      const result = await triggerOpenClawHook(this.env as RuntimeEnv, hook, payload);
      this.recordHookAttempt(eventId, hook, true, result.status, null);
      this.markHookDelivered(eventId, hook, result.status);
      this.appendEvent("webhook.delivered", "Webhook delivered", {
        eventId,
        hook,
        status: result.status,
      });
      return true;
    } catch (error) {
      this.recordHookAttempt(
        eventId,
        hook,
        false,
        null,
        error instanceof Error ? error.message : "Unknown webhook error",
      );
      this.appendEvent("webhook.error", "Webhook delivery failed", {
        eventId,
        hook,
        error: error instanceof Error ? error.message : "Unknown webhook error",
      });
      return false;
    }
  }

  private hasDeliveredHook(eventId: string): boolean {
    return Boolean(
      this.ctx.storage.sql
        .exec("SELECT event_id FROM delivered_hooks WHERE event_id = ?", eventId)
        .toArray()[0],
    );
  }

  private markHookDelivered(eventId: string, hook: string, status: number): void {
    this.ctx.storage.sql.exec(
      `INSERT OR IGNORE INTO delivered_hooks (event_id, hook, delivered_at, status)
       VALUES (?, ?, ?, ?)`,
      eventId,
      hook,
      new Date().toISOString(),
      status,
    );
  }

  private recordHookAttempt(
    eventId: string,
    hook: string,
    accepted: boolean,
    status: number | null,
    error: string | null,
  ): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO hook_attempts (event_id, hook, attempted_at, accepted, status, error)
       VALUES (?, ?, ?, ?, ?, ?)`,
      eventId,
      hook,
      new Date().toISOString(),
      accepted ? 1 : 0,
      status,
      error,
    );
  }

  private async scheduleNextAlarm(status: WatcherStatus): Promise<void> {
    const times: number[] = [];
    if (status.isPolling && status.nextPollAt) {
      times.push(Date.parse(status.nextPollAt));
    }

    const pendingFallback = await this.ctx.storage.get<PendingChandlerFallback>(
      "pendingChandlerFallback",
    );
    if (pendingFallback) {
      times.push(Date.parse(pendingFallback.thresholdReachedAt));
    }

    const next = times.filter(Number.isFinite).sort((a, b) => a - b)[0];
    if (next) {
      await this.ctx.storage.setAlarm(next);
    } else {
      await this.ctx.storage.deleteAlarm();
    }
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

      const testHookRoute = matchTestHookRoute(url.pathname);
      if (testHookRoute && request.method === "POST") {
        if (!(env as RuntimeEnv).WEBHOOK_TOKEN) {
          return json({ error: "WEBHOOK_TOKEN is not configured" }, 503);
        }
        return json(await triggerManualHookTest(env, testHookRoute.draftId, testHookRoute.hook));
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

      if (request.method === "GET" && route.action === "hooks") {
        const limit = Number(url.searchParams.get("limit") ?? "50");
        return json({ hooks: await watcher.hooks(Number.isFinite(limit) ? limit : 50) });
      }

      if (request.method === "POST" && route.action === "start") {
        const body = await readJson<StartOptions>(request);
        const label = normalizeLabel(body.label);
        const status = await watcher.start(route.draftId, { ...body, label });
        const registry = env.REGISTRY.getByName("registry");
        await registry.registerDraft(route.draftId, label);
        return json(status);
      }

      if (request.method === "POST" && route.action === "stop") {
        return json(await watcher.stop(route.draftId));
      }

      if (request.method === "POST" && route.action === "poll") {
        const registry = env.REGISTRY.getByName("registry");
        const status = await watcher.pollNow(route.draftId);
        if (!status.lastError) {
          await registry.registerDraft(route.draftId, null);
        }
        return json(status, status.lastError ? 400 : 200);
      }

      if (request.method === "DELETE" && route.action === "remove") {
        const registry = env.REGISTRY.getByName("registry");
        await watcher.remove(route.draftId);
        return json(await registry.unregisterDraft(route.draftId));
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

async function fetchSleeperDraft(draftId: string): Promise<SleeperDraft> {
  const response = await fetch(`${SLEEPER_API_BASE}/draft/${encodeURIComponent(draftId)}`, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok) {
    throw new Error(`Sleeper draft returned ${response.status}`);
  }

  const body = await response.json();
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("Sleeper returned an unexpected draft payload");
  }

  return body as SleeperDraft;
}

async function triggerManualHookTest(env: Env, draftId: string, hook: ManualHookName) {
  const runtimeEnv = env as RuntimeEnv;
  const [sleeperDraft, sleeperPicks] = await Promise.all([
    fetchSleeperDraft(draftId),
    fetchSleeperPicks(draftId),
  ]);
  const picks = normalizeSleeperPicks(sleeperPicks);
  const observedAt = new Date().toISOString();
  const eventId = `manual-test:${hook}:${crypto.randomUUID()}`;
  const payload = buildManualHookPayload({
    draftId,
    eventId,
    hook,
    observedAt,
    picks,
    sleeperDraft,
    env: runtimeEnv,
  });
  const result = await triggerOpenClawHook(runtimeEnv, hook, payload);

  return {
    accepted: true,
    hook,
    eventId: result.eventId,
    status: result.status,
    message: manualHookMessage(hook, payload),
  };
}

function buildManualHookPayload({
  draftId,
  eventId,
  hook,
  observedAt,
  picks,
  sleeperDraft,
  env,
}: {
  draftId: string;
  eventId: string;
  hook: ManualHookName;
  observedAt: string;
  picks: DraftPick[];
  sleeperDraft: SleeperDraft;
  env: RuntimeEnv;
}): Record<string, unknown> {
  if (hook === "draft-pick-announce") {
    const pick = latestCompletedPick(picks);
    if (!pick) {
      throw new Error("No completed Sleeper pick has pickNumber, playerId, and rosterId");
    }

    return {
      eventId,
      eventType: "PickMade",
      draftId,
      pickNumber: pick.pickNo,
      playerId: pick.playerId,
      rosterId: pick.rosterId,
      sourceVersion: "manual-test-v1",
      observedAt,
    };
  }

  if (hook === "round-summary") {
    const completedRound = latestCompletedRound(sleeperDraft, picks);
    if (!completedRound) {
      throw new Error("No fully completed Sleeper round found");
    }

    return {
      eventId: `manual-test:round:${completedRound.round}:${crypto.randomUUID()}`,
      eventType: "RoundCompleted",
      draftId,
      round: completedRound.round,
      firstPickNumber: completedRound.pickStart,
      lastPickNumber: completedRound.pickEnd,
      completedAt: observedAt,
      sourceVersion: "manual-test-v1",
    };
  }

  const currentTurn = currentDraftTurn(sleeperDraft, picks);
  const chandlerRosterId = parseOptionalInteger(env.CHANDLER_ROSTER_ID);
  const rosterId = chandlerRosterId ?? currentTurn.rosterId;
  const chandlerTurn =
    rosterId === null ? null : chandlerDraftTurn(sleeperDraft, picks, rosterId);
  const turnStartPickNumber = chandlerTurn?.startPickNumber ?? currentTurn.pickNumber;
  const turnEndPickNumber = chandlerTurn?.endPickNumber ?? currentTurn.pickNumber;
  const turnPickNumbers =
    chandlerTurn?.pickNumbers ?? pickNumbersBetween(turnStartPickNumber, turnEndPickNumber);
  const pickSequence =
    turnStartPickNumber === turnEndPickNumber
      ? String(turnStartPickNumber)
      : `${turnStartPickNumber}-${turnEndPickNumber}`;
  const clockStartedAt = observedAt;
  const deadline = new Date(Date.parse(observedAt) + 20 * 60 * 60 * 1000).toISOString();

  if (rosterId === null) {
    throw new Error("Could not determine Chandler roster ID");
  }

  if (hook === "chandler-pick") {
    return {
      testMode: false,
      eventId: `manual-test:chandler-advice:${crypto.randomUUID()}`,
      eventType: "TurnStarted",
      draftId,
      pickSequence,
      turnStartPickNumber,
      turnEndPickNumber,
      turnPickNumbers,
      rosterId: String(rosterId),
      clockStartedAt,
      deadline,
      sourceVersion: "manual-test-v1",
      strategyVersion: "synthetic-test-v1",
      strategySnapshot:
        "Synthetic test only. Prefer value, maintain positional balance, and plan two rounds ahead. Do not use Chandler private data.",
    };
  }

  return {
    testMode: false,
    eventId: `manual-test:chandler-fallback:${crypto.randomUUID()}`,
    eventType: "FallbackDue",
    draftId,
    pickSequence,
    turnStartPickNumber,
    turnEndPickNumber,
    turnPickNumbers,
    rosterId: String(rosterId),
    clockStartedAt,
    deadline,
    thresholdReachedAt: deadline,
    sourceVersion: "manual-test-v1",
    strategyVersion: "synthetic-test-v1",
    strategySnapshot:
      "Synthetic test only. Prefer value, maintain positional balance, and do not use Chandler private data.",
  };
}

async function triggerOpenClawHook(
  env: RuntimeEnv,
  path: ManualHookName,
  payload: Record<string, unknown>,
): Promise<{ status: number; eventId: string }> {
  if (!env.WEBHOOK_TOKEN) {
    throw new Error("WEBHOOK_TOKEN is not configured");
  }

  const baseUrl = (env.OPENCLAW_BASE_URL ?? "https://ai-ff-commissioner.fly.dev").replace(/\/+$/, "");
  const response = await fetch(`${baseUrl}/hooks/${path}`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.WEBHOOK_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(15_000),
  });

  await response.text();

  if (!response.ok) {
    throw new Error(`OpenClaw rejected the webhook with HTTP ${response.status}`);
  }

  return {
    status: response.status,
    eventId: String(payload.eventId),
  };
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

function latestCompletedPick(picks: DraftPick[]): DraftPick | null {
  return [...picks]
    .filter(
      (pick) =>
        typeof pick.pickNo === "number" &&
        typeof pick.playerId === "string" &&
        typeof pick.rosterId === "number",
    )
    .sort((a, b) => (b.pickNo ?? 0) - (a.pickNo ?? 0))[0] ?? null;
}

function latestCompletedRound(
  draft: SleeperDraft,
  picks: DraftPick[],
): { round: number; pickStart: number; pickEnd: number; pickCount: number } | null {
  return completedRounds(draft, picks).sort((a, b) => b.round - a.round)[0] ?? null;
}

function completedRounds(
  draft: SleeperDraft,
  picks: DraftPick[],
): Array<{ round: number; pickStart: number; pickEnd: number; pickCount: number }> {
  const teams = draftTeamCount(draft, picks);
  if (!teams) return [];

  const rounds = new Map<number, DraftPick[]>();
  for (const pick of picks) {
    if (typeof pick.round !== "number") continue;
    const existing = rounds.get(pick.round) ?? [];
    existing.push(pick);
    rounds.set(pick.round, existing);
  }

  return [...rounds.entries()]
    .map(([round, roundPicks]) => {
      const pickNumbers = roundPicks
        .map((pick) => pick.pickNo)
        .filter((pickNo): pickNo is number => typeof pickNo === "number");
      const draftSlots = new Set(
        roundPicks
          .map((pick) => pick.draftSlot)
          .filter((draftSlot): draftSlot is number => typeof draftSlot === "number"),
      );

      if (pickNumbers.length < teams || draftSlots.size < teams) return null;
      return {
        round,
        pickStart: Math.min(...pickNumbers),
        pickEnd: Math.max(...pickNumbers),
        pickCount: roundPicks.length,
      };
    })
    .filter((round): round is { round: number; pickStart: number; pickEnd: number; pickCount: number } => Boolean(round))
    .sort((a, b) => a.round - b.round);
}

function currentDraftTurn(
  draft: SleeperDraft,
  picks: DraftPick[],
): { pickNumber: number; round: number | null; draftSlot: number | null; rosterId: number | null } {
  return draftTurnAtPick(draft, picks.length + 1, picks);
}

function draftTurnAtPick(
  draft: SleeperDraft,
  pickNumber: number,
  picks: DraftPick[],
): { pickNumber: number; round: number | null; draftSlot: number | null; rosterId: number | null } {
  const teams = draftTeamCount(draft, picks);
  if (!teams) {
    return { pickNumber, round: null, draftSlot: null, rosterId: null };
  }

  const round = Math.floor((pickNumber - 1) / teams) + 1;
  const indexInRound = (pickNumber - 1) % teams;
  const isSnakeEvenRound = draft.type === "snake" && round % 2 === 0;
  const draftSlot = isSnakeEvenRound ? teams - indexInRound : indexInRound + 1;
  const rosterId = numberFromRecord(draft.slot_to_roster_id, String(draftSlot));
  return { pickNumber, round, draftSlot, rosterId };
}

function chandlerDraftTurn(
  draft: SleeperDraft,
  picks: DraftPick[],
  chandlerRosterId: number,
): { startPickNumber: number; endPickNumber: number; pickNumbers: number[] } | null {
  const currentPickNumber = picks.length + 1;
  const currentTurn = draftTurnAtPick(draft, currentPickNumber, picks);
  if (currentTurn.rosterId !== chandlerRosterId) return null;

  let startPickNumber = currentPickNumber;
  let endPickNumber = currentPickNumber;
  const totalPicks = totalDraftPicks(draft, picks);

  while (
    startPickNumber > 1 &&
    draftTurnAtPick(draft, startPickNumber - 1, picks).rosterId === chandlerRosterId
  ) {
    startPickNumber -= 1;
  }

  while (
    endPickNumber < totalPicks &&
    draftTurnAtPick(draft, endPickNumber + 1, picks).rosterId === chandlerRosterId
  ) {
    endPickNumber += 1;
  }

  return {
    startPickNumber,
    endPickNumber,
    pickNumbers: pickNumbersBetween(startPickNumber, endPickNumber),
  };
}

function chandlerTurnPickSequence(turn: { startPickNumber: number; endPickNumber: number }): string {
  if (turn.startPickNumber === turn.endPickNumber) return String(turn.startPickNumber);
  return `${turn.startPickNumber}-${turn.endPickNumber}`;
}

function pickNumbersBetween(startPickNumber: number, endPickNumber: number): number[] {
  const picks: number[] = [];
  for (let pickNumber = startPickNumber; pickNumber <= endPickNumber; pickNumber += 1) {
    picks.push(pickNumber);
  }
  return picks;
}

function pendingFallbackTurnEndPickNumber(pending: PendingChandlerFallback): number | null {
  if (typeof pending.turnEndPickNumber === "number" && Number.isFinite(pending.turnEndPickNumber)) {
    return pending.turnEndPickNumber;
  }

  if (typeof pending.pickSequence === "number" && Number.isFinite(pending.pickSequence)) {
    return pending.pickSequence;
  }

  const sequence = String(pending.pickSequence);
  const match = /(\d+)$/.exec(sequence);
  return match ? Number(match[1]) : null;
}

function pendingFallbackTurnStartPickNumber(pending: PendingChandlerFallback): number | null {
  if (
    typeof pending.turnStartPickNumber === "number" &&
    Number.isFinite(pending.turnStartPickNumber)
  ) {
    return pending.turnStartPickNumber;
  }

  if (typeof pending.pickSequence === "number" && Number.isFinite(pending.pickSequence)) {
    return pending.pickSequence;
  }

  const sequence = String(pending.pickSequence);
  const match = /^(\d+)/.exec(sequence);
  return match ? Number(match[1]) : null;
}

function nextChandlerFallbackAt(clockStartedAt: Date): Date {
  const eatOffsetMs = 3 * 60 * 60 * 1000;
  const eatNow = new Date(clockStartedAt.getTime() + eatOffsetMs);
  const eatHour = eatNow.getUTCHours();

  if (eatHour >= 12 && eatHour < 18) {
    return new Date(clockStartedAt.getTime() + 6 * 60 * 60 * 1000);
  }

  const nextNoonEat = new Date(eatNow);
  nextNoonEat.setUTCHours(12, 0, 0, 0);
  if (eatNow.getTime() >= nextNoonEat.getTime()) {
    nextNoonEat.setUTCDate(nextNoonEat.getUTCDate() + 1);
  }

  return new Date(nextNoonEat.getTime() - eatOffsetMs);
}

function draftTeamCount(draft: SleeperDraft, picks: DraftPick[]): number | null {
  if (typeof draft.settings?.teams === "number" && draft.settings.teams > 0) {
    return draft.settings.teams;
  }

  const draftOrderSize = draft.draft_order ? Object.keys(draft.draft_order).length : 0;
  if (draftOrderSize > 0) return draftOrderSize;

  const maxSlot = Math.max(
    0,
    ...picks
      .map((pick) => pick.draftSlot)
      .filter((draftSlot): draftSlot is number => typeof draftSlot === "number"),
  );
  return maxSlot > 0 ? maxSlot : null;
}

function totalDraftPicks(draft: SleeperDraft, picks: DraftPick[]): number {
  const teams = draftTeamCount(draft, picks);
  if (teams && typeof draft.settings?.rounds === "number" && draft.settings.rounds > 0) {
    return teams * draft.settings.rounds;
  }

  return Math.max(picks.length + (teams ?? 1), picks.length + 1);
}

function numberFromRecord(record: Record<string, number> | undefined, key: string): number | null {
  const value = record?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function parseOptionalInteger(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : null;
}

function manualHookMessage(hook: ManualHookName, payload: Record<string, unknown>): string {
  if (hook === "chandler-pick" || hook === "chandler-fallback") {
    return `Accepted for roster ${payload.rosterId}. If there is no Telegram message, SCBot likely returned NO_REPLY because Chandler is not on the clock.`;
  }

  return "Accepted by OpenClaw.";
}

function matchTestHookRoute(pathname: string): { draftId: string; hook: ManualHookName } | null {
  const match = /^\/api\/drafts\/([^/]+)\/test-hooks\/([^/]+)$/.exec(pathname);
  if (!match || !isManualHookName(match[2])) {
    return null;
  }

  return {
    draftId: decodeURIComponent(match[1]),
    hook: match[2],
  };
}

function isManualHookName(value: string): value is ManualHookName {
  return (
    value === "draft-pick-announce" ||
    value === "round-summary" ||
    value === "chandler-pick" ||
    value === "chandler-fallback"
  );
}

function matchDraftRoute(pathname: string): { draftId: string; action: string } | null {
  const match = /^\/api\/drafts\/([^/]+)\/(status|events|hooks|start|stop|poll|remove)$/.exec(pathname);
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
        width: min(1480px, calc(100vw - 24px));
        margin: 0 auto;
        min-height: 100vh;
        padding: 24px 0;
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
      button:disabled {
        cursor: wait;
        opacity: 0.58;
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
      .draft-meta {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
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
        min-height: calc(100vh - 310px);
        align-content: start;
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
      .tabs {
        display: grid;
        gap: 12px;
      }
      .tab-list {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }
      .tab-button {
        color: var(--text);
        background: transparent;
        border-color: var(--border);
      }
      .tab-button.active {
        color: #fff;
        background: var(--accent);
        border-color: var(--accent);
      }
      .tab-panel[hidden] {
        display: none;
      }
      .tab-panel {
        max-height: calc(100vh - 500px);
        min-height: 280px;
        overflow: auto;
      }
      div.tab-panel {
        border: 1px solid var(--border);
        border-radius: 6px;
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
        .draft-meta { grid-template-columns: 1fr; }
        .toolbar { grid-template-columns: 1fr; }
        .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .draft { min-height: auto; }
        .tab-panel { max-height: none; }
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
        <div class="draft-meta">
          <div class="panel metric"><span>Draft ID</span><strong>1389377683300831233</strong></div>
          <div class="panel metric"><span>Name</span><strong>Southerner's Cup Draft</strong></div>
          <div class="panel metric"><span>Teams</span><strong>12</strong></div>
        </div>
        <div class="summary" id="summary"></div>
        <div class="drafts" id="drafts"></div>
      </section>
    </main>

    <script>
      const summary = document.getElementById("summary");
      const drafts = document.getElementById("drafts");
      const activeTabs = new Map();

      async function api(path, options) {
        const res = await fetch(path, options);
        if (!res.ok) {
          let message = await res.text();
          try {
            message = JSON.parse(message).error || message;
          } catch (_) {}
          throw new Error(message);
        }
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
          return '<p>No picks recorded yet.</p>';
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

      function renderHooks(hooks) {
        if (!hooks || hooks.length === 0) return "No hooks fired yet.";
        return hooks.map((hook) => [
          hook.attemptedAt + "  " + hook.hook + "  " + (hook.accepted ? "accepted" : "failed"),
          "eventId: " + hook.eventId,
          "status: " + (hook.status ?? "none"),
          hook.error ? "error: " + hook.error : null,
        ].filter(Boolean).join("\\n")).join("\\n\\n");
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

        drafts.innerHTML = items.map((draft) => {
          const activeTab = activeTabs.get(draft.draftId) || "picks";
          return '<article class="panel draft">'
            + '<div class="status-line"><h2>Southerner\\'s Cup Draft</h2>'
            + '<span class="badge ' + (draft.lastError ? 'err' : draft.isPolling ? 'ok' : '') + '">' + (draft.lastError ? 'error' : draft.isPolling ? 'polling' : 'stopped') + '</span></div>'
            + '<dl>'
            + '<dt>Last poll</dt><dd>' + esc(fmt(draft.lastPollAt)) + '</dd>'
            + '<dt>Next poll</dt><dd>' + esc(fmt(draft.nextPollAt)) + '</dd>'
            + '<dt>Picks</dt><dd>' + esc(draft.lastPickCount) + '</dd>'
            + '<dt>Last pick</dt><dd>' + esc(draft.lastKnownPickNumber || 'none') + '</dd>'
            + '<dt>Error</dt><dd>' + esc(draft.lastError || 'none') + '</dd>'
            + '</dl>'
            + '<div class="status-line"><button data-poll="' + esc(draft.draftId) + '" class="secondary">Poll now</button></div>'
            + '<div class="tabs">'
            + '<div class="tab-list" role="tablist">'
            + tabButton(draft.draftId, "picks", "Picks / players", activeTab)
            + tabButton(draft.draftId, "events", "Recent polling events", activeTab)
            + tabButton(draft.draftId, "hooks", "Fired hooks", activeTab)
            + '</div>'
            + '<div class="tab-panel" ' + (activeTab === "picks" ? "" : "hidden") + '>' + renderPicks(draft.picks) + '</div>'
            + '<pre class="tab-panel" id="events-' + esc(draft.draftId) + '" ' + (activeTab === "events" ? "" : "hidden") + '>Loading events...</pre>'
            + '<pre class="tab-panel" id="hooks-' + esc(draft.draftId) + '" ' + (activeTab === "hooks" ? "" : "hidden") + '>Loading hooks...</pre>'
            + '</div>'
            + '</article>';
        }).join("") || '<p>No drafts registered yet.</p>';

        for (const draft of items) {
          loadEvents(draft.draftId);
          loadHooks(draft.draftId);
        }
      }

      function tabButton(draftId, tab, labelText, activeTab) {
        return '<button type="button" class="tab-button ' + (activeTab === tab ? 'active' : '') + '" data-tab="' + tab + '" data-draft="' + esc(draftId) + '">' + labelText + '</button>';
      }

      async function load() {
        renderStatus(await api("/api/status"));
      }

      async function loadEvents(id) {
        const el = document.getElementById("events-" + id);
        if (!el) return;
        const data = await api("/api/drafts/" + encodeURIComponent(id) + "/events?limit=100");
        el.textContent = data.events.map((event) => event.at + "  " + event.type + "  " + event.message).join("\\n") || "No events yet.";
      }

      async function loadHooks(id) {
        const el = document.getElementById("hooks-" + id);
        if (!el) return;
        const data = await api("/api/drafts/" + encodeURIComponent(id) + "/hooks?limit=100");
        el.textContent = renderHooks(data.hooks);
      }

      document.getElementById("refresh").addEventListener("click", load);
      drafts.addEventListener("click", async (event) => {
        const target = event.target;
        if (!(target instanceof HTMLButtonElement)) return;
        if (target.dataset.tab && target.dataset.draft) {
          activeTabs.set(target.dataset.draft, target.dataset.tab);
          await load();
          return;
        }
        if (target.dataset.poll) {
          await api("/api/drafts/" + encodeURIComponent(target.dataset.poll) + "/poll", { method: "POST" });
        }
        await load();
      });

      load();
      setInterval(load, 10000);
    </script>
  </body>
</html>`;
}
