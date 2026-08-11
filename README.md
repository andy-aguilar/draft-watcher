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
