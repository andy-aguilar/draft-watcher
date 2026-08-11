# draft-watcher

Generic Cloudflare Worker for watching Sleeper drafts and exposing a small public dashboard.

## Routes

- `GET /` or `/dashboard` - dashboard
- `GET /api/status` - all registered drafts
- `GET /api/drafts/:draftId/status` - one draft
- `GET /api/drafts/:draftId/events` - recent events
- `POST /api/drafts/:draftId/start` - start polling
- `POST /api/drafts/:draftId/stop` - stop polling
- `POST /api/drafts/:draftId/poll` - force one poll
- `DELETE /api/drafts/:draftId/remove` - remove a draft from the dashboard registry

## Webhooks

When a poll sees new picks, `draft-watcher` can POST one deterministic event per
new pick to SCBot/OpenClaw.

Configure:

```bash
npx wrangler secret put DRAFT_EVENT_WEBHOOK_URL
npx wrangler secret put DRAFT_EVENT_WEBHOOK_TOKEN
```

`DRAFT_EVENT_WEBHOOK_URL` should be the SCBot/OpenClaw inbound hook URL.
`DRAFT_EVENT_WEBHOOK_TOKEN` is sent as `Authorization: Bearer <token>`.

Payload:

```json
{
  "id": "draft:<draft_id>:pick:<pick_no>",
  "type": "PickMade",
  "source": "draft-watcher",
  "draftId": "<draft_id>",
  "occurredAt": "2026-08-11T12:00:00.000Z",
  "statusUrl": "https://draft-watcher.aaguil3.workers.dev/api/drafts/<draft_id>/status",
  "pick": {
    "pickNo": 1,
    "round": 1,
    "draftSlot": 1,
    "playerId": "4046",
    "pickedBy": "123",
    "rosterId": 1,
    "firstName": "Christian",
    "lastName": "McCaffrey",
    "fullName": "Christian McCaffrey",
    "position": "RB",
    "team": "SF"
  }
}
```

Headers:

- `Authorization: Bearer <DRAFT_EVENT_WEBHOOK_TOKEN>`
- `X-Draft-Watcher-Event-Id: draft:<draft_id>:pick:<pick_no>`
- `X-Draft-Watcher-Event-Type: PickMade`

SCBot should treat `id` as the idempotency key and fetch `statusUrl` for
canonical draft state instead of relying on the webhook payload as the full
source of truth.

## Manual SCBot Hook Tests

The dashboard includes four server-side test buttons per registered draft:

- Test pick announcement
- Test round summary
- Test Chandler advice
- Test Chandler fallback

The browser calls `draft-watcher`; `draft-watcher` calls OpenClaw server-side.
The OpenClaw bearer token is never included in browser JavaScript, URLs, or
responses.

Configure the manual hook token:

```bash
npx wrangler secret put WEBHOOK_TOKEN
```

Optional non-secret config:

```json
{
  "OPENCLAW_BASE_URL": "https://ai-ff-commissioner.fly.dev",
  "CHANDLER_ROSTER_ID": "7"
}
```

Routes:

- `POST /api/drafts/:draftId/test-hooks/draft-pick-announce`
- `POST /api/drafts/:draftId/test-hooks/round-summary`
- `POST /api/drafts/:draftId/test-hooks/chandler-pick`
- `POST /api/drafts/:draftId/test-hooks/chandler-fallback`

The round-summary test derives the latest completed Sleeper round and sends:

```json
{
  "eventId": "manual-test:round:<round>:<uuid>",
  "eventType": "RoundCompleted",
  "draftId": "<draft_id>",
  "round": 2,
  "firstPickNumber": 13,
  "lastPickNumber": 24,
  "completedAt": "2026-08-11T18:55:00.000Z",
  "sourceVersion": "manual-test-v1"
}
```

The Chandler advice test sends:

```json
{
  "testMode": false,
  "eventId": "manual-test:chandler-advice:<uuid>",
  "eventType": "TurnStarted",
  "draftId": "<draft_id>",
  "pickSequence": "<current_pick_number>",
  "rosterId": "7",
  "clockStartedAt": "2026-08-11T19:30:00.000Z",
  "deadline": "2026-08-12T15:30:00.000Z",
  "sourceVersion": "manual-test-v1",
  "strategyVersion": "synthetic-test-v1",
  "strategySnapshot": "Synthetic test only. Prefer value, maintain positional balance, and plan two rounds ahead. Do not use Chandler private data."
}
```

The Chandler fallback test sends:

```json
{
  "testMode": false,
  "eventId": "manual-test:chandler-fallback:<uuid>",
  "eventType": "FallbackDue",
  "draftId": "<draft_id>",
  "pickSequence": "<current_pick_number>",
  "rosterId": "7",
  "clockStartedAt": "2026-08-11T19:30:00.000Z",
  "deadline": "2026-08-12T15:30:00.000Z",
  "thresholdReachedAt": "2026-08-12T15:30:00.000Z",
  "sourceVersion": "manual-test-v1",
  "strategyVersion": "synthetic-test-v1",
  "strategySnapshot": "Synthetic test only. Prefer value, maintain positional balance, and do not use Chandler private data."
}
```

Start payload:

```json
{
  "label": "Optional display label",
  "pollIntervalSeconds": 15
}
```

Drafts are registered for the dashboard only after Sleeper returns a successful picks response.

## Development

```bash
npm install
npm run types
npm run dev
```

## Deploy

```bash
npm run deploy
```
