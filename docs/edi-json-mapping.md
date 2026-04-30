# EDI JSON Mapping

HarmonicMesh uses simplified JSON with EDIFACT-style message type names.
Full EDIFACT is not implemented — this document explains how a real integration would map.

## Message Types

| JSON `message_type` | EDIFACT equivalent | Description |
|---|---|---|
| `ORDERS` | ORDERS D96A | Purchase order |
| `ORDRSP` | ORDRSP D96A | Order response (≤24h SLA) |
| `DESADV` | DESADV D96A | Despatch advice / advance shipping notice |
| `RECADV` | RECADV D96A | Receiving advice |
| `INVOIC` | INVOIC D96A | Invoice |

## Simplifications

A real EDIFACT message wraps the payload in UNB/UNH envelopes, uses segment delimiters,
and includes interchange control references. HarmonicMesh replaces all of that with a
flat JSON envelope:

```json
{
  "message_type": "ORDERS",
  "order_id": "PO-2026-1842",
  "partner_code": "HGB",
  "event_time": "2026-05-15T09:00:00Z",
  "payload": {}
}
```
