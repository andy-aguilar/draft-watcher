# draft-watcher

Generic Cloudflare Worker for watching Sleeper drafts and exposing a small public dashboard.

## Routes

- `GET /` or `/dashboard` - dashboard
- `GET /api/status` - all registered drafts
- `GET /api/drafts/:draftId/status` - one draft
- `GET /api/drafts/:draftId/events` - recent events
- `GET /api/drafts/:draftId/hooks` - recent SCBot hook attempts
- `POST /api/drafts/:draftId/start` - start polling
- `POST /api/drafts/:draftId/stop` - stop polling
- `POST /api/drafts/:draftId/poll` - force one poll
- `DELETE /api/drafts/:draftId/remove` - remove a draft from the dashboard registry

## SCBot Automation

When polling sees new Sleeper picks, `draft-watcher` calls SCBot/OpenClaw
server-side. The browser never receives the webhook token.

Configure:

```bash
npx wrangler secret put WEBHOOK_TOKEN
```

Non-secret config:

```json
{
  "OPENCLAW_BASE_URL": "https://ai-ff-commissioner.fly.dev",
  "CHANDLER_ROSTER_ID": "7"
}
```

Automatic hooks:

- New completed pick: `POST /hooks/draft-pick-announce`
- Newly completed round: `POST /hooks/round-summary`
- Chandler on the clock for a turn: `POST /hooks/chandler-pick`
- Chandler fallback check due and the turn still has an unmade pick: `POST /hooks/chandler-fallback`

Pick announcement payload:

```json
{
  "eventId": "draft:<draft_id>:pick:<pick_no>",
  "eventType": "PickMade",
  "draftId": "<draft_id>",
  "pickNumber": 24,
  "playerId": "11604",
  "rosterId": 8,
  "sourceVersion": "draft-watcher-v1",
  "observedAt": "2026-08-11T12:00:00.000Z"
}
```

Round, Chandler advice, and Chandler fallback payloads use the same shapes as
the manual test buttons below, with stable `draft:<...>` event IDs and
`sourceVersion: draft-watcher-v1`. Successfully accepted event IDs are stored in
the draft Durable Object so repeated polls do not double-post the same hook.

The Chandler advice and fallback hooks are keyed by the whole turn, not by a
single pick. `pickSequence` remains a single pick number string for compatibility,
using the first pick in the turn. At a snake turn, event IDs and payloads include
a range key such as `turnKey: "12-13"` plus `turnStartPickNumber`,
`turnEndPickNumber`, and `turnPickNumbers`. That keeps advice/fallback from
double-firing when Chandler has consecutive picks at the end or beginning of a
round without breaking existing hook consumers.

The Chandler fallback timer uses East Africa Time, UTC+3. If Chandler comes on
clock from noon through 17:59 EAT, the fallback check is scheduled six hours
later; otherwise it waits until the next noon EAT. The fallback alarm re-checks
Sleeper before calling SCBot and skips the hook if Chandler's full turn has
already been picked.

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
  "pickSequence": "<first_pick_number_in_turn>",
  "turnKey": "12-13",
  "turnStartPickNumber": 12,
  "turnEndPickNumber": 13,
  "turnPickNumbers": [12, 13],
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
  "pickSequence": "<first_pick_number_in_turn>",
  "turnKey": "12-13",
  "turnStartPickNumber": 12,
  "turnEndPickNumber": 13,
  "turnPickNumbers": [12, 13],
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
