# Endpoints

## Route prefix

The Azure Function host uses `routePrefix = api` from `host.json`.

## Health endpoint

`GET /api/health`

- Auth: anonymous
- Purpose: check if the Function App is alive
- Response: `200` with `{"status":"healthy"}`

## Single field upsert

`POST /api/session-log/upsert-field`

- Auth: function key required
- Request body limit: 1 MB
- Payload:
```json
{
  "sessionId": "<GUID>",
  "fieldName": "my_field",
  "fieldValue": "some value",
  "isKpi": false,
  "capturedAtUtc": "2026-04-16T12:34:56Z"
}
```

### Behavior

- Validates `sessionId` and `fieldName`.
- Validates `fieldValue` length and type rules.
- Accepts optional `capturedAtUtc` if it is a valid ISO timestamp.
- Writes or merges the session JSON into blob storage.
- If the field name is invalid, the session payload may still be written with `incompatibleFieldNames` for later analysis.
- Returns accepted result or detailed rejection reasons.

## Batch field upsert

`POST /api/session-log/upsert-batch`

- Auth: function key required
- Request body limit: 1 MB
- Maximum fields: 100

### Payload example

```json
{
  "sessionId": "<GUID>",
  "botId": "bot-123",
  "userId": "user-456",
  "userEmail": "user@example.com",
  "userDisplayName": "Jane Doe",
  "fields": [
    {"fieldName": "foo", "fieldValue": "bar", "isKpi": false, "eventId": "evt-1", "capturedAtUtc": "2026-04-16T12:34:56Z"},
    {"fieldName": "csat", "fieldValue": "5", "isKpi": true}
  ]
}
```

### Behavior

- Validates session metadata and each field individually.
- Supports optional `botId`, `userId`, `userEmail`, `userDisplayName`, `eventId`, and `capturedAtUtc`.
- Invalid field names can be kept in `incompatibleFieldNames` while still preserving the session document.
- Returns accepted and rejected field details.

## Get session

`GET /api/session-log/get-session?sessionId=<GUID>`

- Auth: function key required
- Returns the raw stored session JSON from blob storage.
- If not found, returns `404`.

## Validation and limits

- `sessionId`: required, must be a GUID.
- `fieldName`: max 150 characters.
- `fieldValue`: max 4000 characters.
- `capturedAtUtc`: if present, must be a valid ISO datetime.
- `userEmail`: validated by regex format.
- Boolean fields: `true`/`false`, `1`/`0`, `yes`/`no`.
- `satisfaction_score`: allowed only `1`-`5`.
- Non-KPI fields may be rejected for probable noise.
