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
