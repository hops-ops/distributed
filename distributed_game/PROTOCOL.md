# distributed_game wire protocol — version 1

Normative specification of the binary protocol spoken between a
`distributed_game` zone server and its clients. The Rust implementation
lives in `src/protocol/` and `src/schema.rs`; the reference client-side
state machine is `protocol::ClientMirror`. Golden byte fixtures for
independent implementations live in `tests/fixtures/*.json` (§9).

Key words MUST / MUST NOT / SHOULD / MAY are used as in RFC 2119.

---

## 1. Transport and framing

### 1.1 WebSocket (normative transport)

- Endpoint: `GET /zones/{zone_id}` upgraded to a WebSocket. The path
  selects the zone; the zone in the admission ticket MUST equal it
  (transfer tickets: §8.1).
- A host that does not own the zone answers the Hello with
  `Redirect(zone_id, url)` and closes; the client MUST reconnect to `url`
  and send the **same** Hello (the ticket was not consumed). Clients
  SHOULD follow at most 3 redirects in a row.
- Every protocol message is exactly **one binary WebSocket message**.
  There is no additional length prefix inside a WebSocket message.
- Text messages are a protocol violation: the server replies
  `Error(BAD_MESSAGE)` and closes.
- The transport is reliable and ordered. Deltas are encoded against the
  last state the server *sent* to that connection (§7.2), so a client MUST
  apply every server message in order and MUST NOT skip any.
- Byte 0 of every message is the **message type** (§6). Client→server
  types are `0x01..0x7f`; server→client types are `0x80..0xff`.
- Servers reject inbound messages larger than 64 KiB by default.

### 1.2 Stream framing (non-message transports)

For byte-stream transports (raw TCP, pipes, test harnesses) each message
is prefixed with its byte length as a `varuint` (§2): `varuint(len) ‖ message`.
WebSocket transports MUST NOT use this prefix.

## 2. Primitive encodings

All multi-byte fixed-width numbers are **little-endian**.

| Name | Encoding |
|---|---|
| `u8` | 1 byte. |
| `bool` | `u8`, `0x00` = false, `0x01` = true. Any other value is a decode error. |
| `varuint` | Unsigned LEB128 of a 32-bit value: 7 bits per byte, least-significant group first; bit 7 (`0x80`) set means another byte follows. At most 5 bytes; in the 5th byte only the low 4 bits may be set (`<= 0x0f`), otherwise decode error (overflow). Encoders MUST emit the minimal form. |
| `varint` | Signed 32-bit, zigzag-mapped then `varuint`: `z = (v << 1) ^ (v >> 31)` (arithmetic shift); decode `v = (z >>> 1) ^ -(z & 1)`. |
| `f32` | IEEE-754 binary32, 4 bytes LE. |
| `f64` | IEEE-754 binary64, 8 bytes LE. |
| `string` | `varuint` byte length, then that many bytes of UTF-8. Length MUST be <= 1 MiB. Invalid UTF-8 is a decode error. |
| `bytes` | `varuint` length, then raw bytes (<= 1 MiB). |
| `short_string` | `u8` byte length (0..255), then UTF-8. Encoders truncate at a char boundary to fit 255 bytes. |

Examples (from `fixtures/primitives.json`):

| Value | Kind | Bytes |
|---|---|---|
| 0 | varuint | `00` |
| 127 | varuint | `7f` |
| 128 | varuint | `80 01` |
| 300 | varuint | `ac 02` |
| 4294967295 | varuint | `ff ff ff ff 0f` |
| -1 | varint | `01` |
| 1 | varint | `02` |
| -64 | varint | `7f` |
| 64 | varint | `80 01` |
| 1.5 | f32 | `00 00 c0 3f` |
| 1234.5 | f64 | `00 00 00 00 00 4a 93 40` |
| "hi" | string | `02 68 69` |
| "héros" | string | `06 68 c3 a9 72 6f 73` |

JavaScript note: `varuint` values up to `2^32-1` exceed the int32 range;
decode with multiplication (`value += (b & 0x7f) * 2 ** shift`) or `>>> 0`,
never with signed `<<` on the 5th byte.

## 3. Ids, ticks and times

- **Sequence numbers** (`seq`) are `varuint`, start at **1** per connection
  and strictly increase. `ack_seq = 0` means "nothing processed yet".
- **Ticks** are `varuint` holding the low 32 bits of the server's tick
  counter.
- **Times** (`server_time`, `client_time`) are `f64` milliseconds on the
  sender's own monotonic clock (origin arbitrary). Clients estimate
  offset/RTT from Ping/Pong:
  `rtt = now - pong.client_time`; `offset = pong.server_time + rtt/2 - now`.
- **Entity ids** are `varuint`, assigned by the game, unique within a zone.
  An id MAY be reused after its remove has been sent.
- **Kind ids** are `varuint` values `<= 65535`.

## 4. Field types and quantization

Each component field has a wire type. Type tags appear in the schema (§5).

| Tag | Type | Params (in schema) | Value encoding | Decoded value |
|---|---|---|---|---|
| 0 | `bool` | — | `bool` | boolean |
| 1 | `u8` | — | 1 raw byte | unsigned int |
| 2 | `u16` | — | `varuint` (MUST be <= 65535) | unsigned int |
| 3 | `u32` | — | `varuint` | unsigned int |
| 4 | `i32` | — | `varint` (zigzag) | signed int |
| 5 | `f32` | — | `f32` | float |
| 6 | `quant_f32` | `f32 min`, `f32 max`, `u8 bits` (1..32) | `varuint q` | float |
| 7 | `angle` | `u8 bits` (1..32) | `varuint q` | float (radians, `[0, 2π)`) |
| 8 | `short_str` | — | `short_string` | string |
| 9 | `bytes` | — | `bytes` | byte array |

Unknown tags are a decode error.

All quantization math below is performed in **IEEE-754 binary64**
(JavaScript `number`) in exactly the order written; conversions to f32
are round-to-nearest-even (`Math.fround`). `floor` is mathematical floor.
Server and client MUST use identical formulas so client-side prediction
can reproduce exactly what the server will send.

### 4.1 `quant_f32 {min, max, bits}`

```
maxq = 2^bits - 1                          (as f64)
lo   = f64(min);  hi = f64(max)
encode(v):                                  v is an f32 widened to f64
  if hi <= lo:         q = 0
  else:
    x = isNaN(v) ? lo : v
    x = clamp(x, lo, hi)
    t = (x - lo) / (hi - lo)
    q = min(floor(t * maxq + 0.5), maxq)    (round half up)
decode(q):
  q MUST be <= maxq, otherwise decode error
  v = fround(lo + (q / maxq) * (hi - lo))
```

Worked example (`min=-100, max=100, bits=16`, from `fixtures/quantization.json`):
`v = 0.0` → `t = 0.5` → `q = floor(0.5 * 65535 + 0.5) = 32768` → bytes
`80 80 02`; decodes to `fround(-100 + (32768/65535) * 200) = 0.0015259022`
(bits `3ac800c8`). `v = 250` clamps to `q = 65535` (`ff ff 03`) → `100.0`.

*Informative:* a symmetric range (`min = -max`) has `2^bits - 1` intervals,
so `0` is not on the grid (the example above decodes `0.0` as
`0.0015259022`). Signed quantities that must rest at exactly zero use
`max = -min · (1 - 2^-(bits-1))` (Rust: `FieldType::quant_centered`), which
puts `0` at `q = 2^(bits-1)`.

### 4.2 `angle {bits}`

```
TAU   = 6.283185307179586                  (2π as f64)
steps = 2^bits                             (as f64)
encode(v):
  if !isFinite(v): q = 0
  else:
    a = v % TAU                            (truncated remainder, JS `%`, C fmod)
    if a < 0: a = a + TAU
    q = floor((a / TAU) * steps + 0.5) % steps
decode(q):
  q MUST be < steps, otherwise decode error
  v = fround((q * TAU) / steps)
```

Example (`bits=8`): `π` → `q = 128`; `-π/2` → `a = 3π/2` → `q = 192`;
`-0.000001` → `a ≈ TAU` → `q = 256 % 256 = 0`.

### 4.3 Coercion (server side)

The server coerces game-supplied values to the field type (numeric
saturation, `bool` = non-zero). Clients only ever see canonical values.

## 5. Entity schema encoding

Sent inside Welcome. A generic client decodes Deltas with it, no codegen.

```
schema   := varuint kind_count, kind*
kind     := varuint kind_id, string name, varuint field_count (<= 32), field*
field    := string name, u8 type_tag, params
params   := (tag 6) f32 min, f32 max, u8 bits
          | (tag 7) u8 bits
          | (other tags) nothing
```

Field order is wire order: field `i` is bit `i` of update masks. A kind
has at most 32 fields. Duplicate kind ids are a decode error. Server-side
replication priorities are not transmitted.

## 6. Messages

### 6.1 Type bytes

| Byte | Direction | Message |
|---|---|---|
| `0x01` | C→S | Hello |
| `0x02` | C→S | Input |
| `0x03` | C→S | Ping |
| `0x04` | C→S | Leave |
| `0x80` | S→C | Welcome |
| `0x81` | S→C | Delta |
| `0x82` | S→C | Event |
| `0x83` | S→C | Pong |
| `0x84` | S→C | Freeze |
| `0x85` | S→C | Resume |
| `0x86` | S→C | Handoff |
| `0x87` | S→C | Kick |
| `0x88` | S→C | Error |
| `0x89` | S→C | Redirect |

Unknown type bytes and trailing bytes after a complete message are
decode errors.

### 6.2 Client → server

```
Hello  := 0x01, varuint protocol_version, string content_version,
          varuint capabilities, string ticket, string reconnect_token
Input  := 0x02, varuint seq, varuint client_tick, bytes payload
Ping   := 0x03, f64 client_time
Leave  := 0x04
```

- **Hello** MUST be the first message, sent within the server's hello
  timeout (default 5 s). `protocol_version` MUST be `1`.
  `content_version` MUST equal the server's when the server pins one.
  `capabilities` are game-defined bits, opaque to the runtime. For a fresh
  join `ticket` is a signed admission ticket (§8) and `reconnect_token` is
  empty; for a reconnect `ticket` MAY be empty and `reconnect_token` is the
  token from the last Welcome. A second Hello gets `Error(BAD_MESSAGE)`.
- **Input**: `payload` is game-defined. Duplicate or already-processed
  `seq` values are ignored. Inputs are rate limited per connection
  (default 120/s, burst 240); excess inputs are dropped and the server
  sends `Error(RATE_LIMITED)` at most once per second; sustained abuse
  gets `Kick(RATE_LIMITED)`. Observers' inputs are ignored.
  `client_tick` SHOULD be the latest `server_tick` the client has applied
  (the tick its interpolated view is based on). Servers that do lag
  compensation evaluate shots at `time(client_tick) - interp_delay`, where
  `interp_delay` is game-defined and carried **inside `payload`**; the
  server clamps the rewind to its configured maximum, so the value is
  advisory and never trusted beyond that bound.
- **Ping**: the server answers with Pong immediately.
- **Leave**: graceful exit; the player is removed at once (no reconnect
  grace).

Worked example — Hello (`version 1, content "c1", caps 5, ticket "t.k.n"`):

```
01                 type Hello
01                 protocol_version 1
02 63 31           content_version "c1"
05                 capabilities 5
05 74 2e 6b 2e 6e  ticket "t.k.n"
00                 reconnect_token ""
```

Input (`seq 300, client_tick 1000, payload 01 ff`): `02 ac02 e807 02 01ff`.
Ping (`1234.5`): `03 0000000000004a9340`. Leave: `04`.

### 6.3 Server → client

```
Welcome := 0x80, string session_id, string zone_id, u8 role,
           varuint tick_rate, varuint patch_rate, varuint server_tick,
           f64 server_time, string reconnect_token, string content_version,
           schema
Delta   := 0x81, see §7
Event   := 0x82, varuint type_id, bytes payload
Pong    := 0x83, f64 client_time, f64 server_time
Freeze  := 0x84, string reason, string version, string notes
Resume  := 0x85, string reason, string version, string notes
Handoff := 0x86, string target_zone, string transfer_ticket
Kick    := 0x87, varuint code, string message
Error   := 0x88, varuint code, string message
Redirect := 0x89, string zone_id, string url
```

- **Welcome**: `role` `0` = player, `1` = observer. `session_id` is stable
  across reconnects of the same player. `reconnect_token` is single-use and
  rotated on every Welcome (empty for observers). A client MUST discard all
  mirrored entities on Welcome; the server resends creates afterwards.
  `tick_rate`/`patch_rate` are Hz.
- **Event**: reliable, ordered, game-defined (`type_id`, `payload`). Events
  are queued when emitted and may arrive before the Delta of the tick that
  produced them.
- **Pong**: echoes `client_time`; `server_time` is the server clock.
- **Freeze**: the zone stops simulating; inputs are dropped (and counted)
  until **Resume**. For a content rollout `reason` is `content_update` and
  `version` is the **target** content version. Clients SHOULD stop
  sending inputs and show `notes`.
- **Resume**: simulation continues. `version` is the content version the
  zone now runs: the new one after a committed rollout
  (`reason = content_update`), the old one after an aborted rollout
  (`reason = content_update_aborted`). Events and facts after Resume are
  produced by `version`. A client whose own content differs MAY keep
  playing until told to reload (§8.2).
- **Handoff**: the player has been removed from this zone; connect to
  `/zones/{target_zone}` (on the current host; follow any Redirect) and
  present `transfer_ticket` as the Hello `ticket` (empty
  `reconnect_token`). The server closes the socket. See §8.1.
- **Kick**: the session is over; the server closes the socket.
  Codes: `1` BY_ZONE, `2` REPLACED (same principal connected again),
  `3` SHUTDOWN, `4` RATE_LIMITED, `5` ZONE_DISPOSED, `6` SLOW_CONSUMER,
  `7` RELOAD. **RELOAD** is the exception to "session over": the player
  is kept for the reconnect grace window; the client MUST reload its
  content to the zone's current version (`Resume.version` / Welcome
  `content_version`) and reconnect with the token from its last Welcome
  and the new `content_version` in Hello.
- **Redirect**: this host does not own `zone_id`; reconnect to `url` (a
  full `ws://` / `wss://` URL including `/zones/{zone_id}`) with the same
  Hello. The server closes the socket.
- **Error**: codes `1` BAD_MESSAGE, `2` UNAUTHORIZED, `3` REJECTED,
  `4` ZONE_FULL, `5` VERSION_MISMATCH, `6` RATE_LIMITED, `7` NOT_FOUND,
  `8` RECONNECT_FAILED, `9` ZONE_UNAVAILABLE (the zone could not be created
  right now; retry later), `10` ASLEEP (an observer is not admitted to a
  sleeping zone it would otherwise wake; zones hosted as celld objects treat
  observers as passive watchers; retry later). Errors during the handshake are followed by close;
  `BAD_MESSAGE` and `RATE_LIMITED` after joining are not.

Worked example — Welcome (session "s1", zone "z1", player, 30 Hz ticks,
20 Hz patches, tick 42, time 1000.25 ms, token "rt", content "c1",
schema of `fixtures/messages.json`):

```
80                       Welcome
02 73 31                 session_id "s1"
02 7a 31                 zone_id "z1"
00                       role player
1e                       tick_rate 30
14                       patch_rate 20
2a                       server_tick 42
00 00 00 00 00 42 8f 40  server_time 1000.25
02 72 74                 reconnect_token "rt"
02 63 31                 content_version "c1"
02                       2 kinds
  01 04 68 65 72 6f 0a   kind 1 "hero", 10 fields
    05 616c697665 00     "alive" bool
    05 6c6576656c 01     "level" u8
    02 6870 02           "hp" u16
    04 676f6c64 03       "gold" u32
    02 647a 04           "dz" i32
    05 7370656564 05     "speed" f32
    01 78 06 0000c8c2 0000c842 10    "x" quant_f32 min -100 max 100 bits 16
    03 796177 07 0a      "yaw" angle bits 10
    04 6e616d65 08       "name" short_str
    03 746167 09         "tag" bytes
  02 03 6f 72 62 01      kind 2 "orb", 1 field
    01 78 06 00000000 00008042 0c    "x" quant_f32 min 0 max 64 bits 12
```

Redirect (`fixtures/messages.json` → `redirect`):
`89 0a 7265616c6d2e77657374 23 77733a2f2f31302e302e302e323a323536372f7a6f6e65732f7265616c6d2e77657374`
= zone `"realm.west"`, url `"ws://10.0.0.2:2567/zones/realm.west"`.
Kick RELOAD (`kick_reload`): `87 07 13 "content c2 required"`.

## 7. Delta

### 7.1 Encoding

```
Delta   := 0x81, varuint server_tick, varuint ack_seq,
           varuint remove_count, varuint id * remove_count,
           varuint create_count, create * create_count,
           varuint update_count, update * update_count
create  := varuint id, varuint kind_id, value(field_0) … value(field_{n-1})
update  := varuint id, varuint mask, value(field_i) for each set bit i, ascending
```

- `ack_seq` is the highest input `seq` of this connection's player that
  the simulation has consumed (or discarded under a "latest" policy).
  Prediction: drop pending inputs with `seq <= ack_seq`, reset to the
  authoritative state, replay the rest.
- A **create** carries every field of the kind in schema order.
- An **update** carries a 32-bit field bitmask as a `varuint` (bit `i` set =
  field `i` present) followed by only those fields in ascending `i`. A
  mask bit `>= field_count` is a decode error. An update's kind is the
  kind of the entity as known by the client (from its create, possibly
  earlier in the same Delta).
- The server always sends a Delta every patch to every connection, even
  when empty (it carries `server_tick` and `ack_seq`).

### 7.2 Applying (normative, see `ClientMirror::apply`)

1. Remove every id in `removes`.
2. Insert every create (replacing any existing entry with the same id).
3. For each update, overwrite the present fields of the known entity. An
   update for an unknown id is a protocol error.
4. Set `server_tick` and `ack_seq`.

### 7.3 Server semantics (informative)

- Deltas are computed per connection against what was last **sent** to it.
- Interest: each connection has a view (center, radius) on the zone's 2D
  interest plane. An entity is created when within `radius` and removed
  when beyond `radius × (1 + leave_margin)` (default margin 0.1) or gone.
  Global entities are visible everywhere.
- Change detection compares encoded field bytes, so changes smaller than
  the quantization step are never sent.
- Per-connection byte budget (default 4096 B of create+update data per
  Delta): candidates gain `kind priority × entity priority × max(changed
  field priority) × 1/(1 + distance/radius)` per patch into an
  accumulator; the highest accumulators are sent first and reset;
  deferred entities keep accumulating, so none starve. At least one
  candidate is sent per Delta. Removes are never budgeted.

### 7.4 Worked example

`fixtures/messages.json` → `delta`, with the client already knowing
entity 5 (kind 1 "hero") and 6 (kind 2 "orb"):

```
81                    Delta
c8 01                 server_tick 200
11                    ack_seq 17
02 09 82 01           removes [9, 130]
02                    2 creates
  ac 02 01            id 300, kind 1
    01                alive true
    07                level 7
    ac 02             hp 300
    f0 a2 04          gold 70000
    03                dz -2 (zigzag 3)
    00 00 c0 3f       speed 1.5
    00                x -100.0 (q 0)
    00                yaw 0 (q 0)
    03 41 6e 61       name "Ana"
    02 de ad          tag de ad
  ad 02 02            id 301, kind 2
    80 10             x 32.0 → q 2048 (12 bits over 0..64)
03                    3 updates
  05 44               id 5, mask 0b0100_0100 (fields 2 and 6)
    ab 02             hp 299
    80 80 02          x q 32768 → 0.0015259022
  06 01               id 6, mask 0b1
    ff 1f             x q 4095 → 64.0
  ad 02 01            id 301 (created above), mask 0b1
    00                x q 0 → 0.0
```

Full hex: `81c801110209820102ac02010107ac02f0a204030000c03f000003416e6102deadad02028010030544ab028080020601ff1fad020100`.

An empty Delta at tick 43: `81 2b 00 00 00 00`.

## 8. Admission tickets

A ticket is a compact JWT signed with **HS256** (HMAC-SHA256 over the
ASCII `base64url(header) "." base64url(claims)` with a shared secret;
unpadded base64url). Any HS256 JWT library can mint one.

- Header: `{"alg":"HS256","typ":"JWT"}`; any other `alg` is rejected.
- Claims:
  - `sub` (string, required) — the **principal**. The server takes
    identity only from this verified claim; nothing in Hello, Input or any
    payload can set or change it.
  - `zone` (string, required) — MUST equal the route's zone id.
  - `exp` (integer Unix seconds, required) — rejected when `exp <= now`.
  - `nonce` (string, required) — single use; reuse before `exp` is
    rejected.
  - `role` (string, optional) — `"player"` (default) or `"observer"`.
    Observers receive Deltas and Events but own no player and cannot send
    inputs.
  - `name` (string, optional) — display name vouched for by the issuer
    (never identity). Carried into transfer tickets.
- Failures produce `Error(UNAUTHORIZED)` and close.

**Custom ticket formats.** A server MAY admit join tickets in another
format through a pluggable verifier (`ServerConfig::ticket_verifier`, e.g.
a web backend's own JWTs with `iss`/`aud`/`jti` and no `zone`/`nonce`).
The verifier maps the token to the same claims (principal, zone = route
zone, expiry, replay key, role, name); the server still rejects a zone
other than the route's, `exp <= now` and a reused replay key. Transfer
tickets (§8.1) always use the format above.

### 8.1 Transfer tickets and handoff

A zone hands a player to another zone (possibly on another host) with a
**transfer ticket**: the same HS256 JWT format, minted by the source host,
with extra claims — `kind: "transfer"`, `src` (source zone), `iss`
(issuing host id), `gen` (source zone ownership token), `carry`
(base64url carried game state, <= 16 KiB). Clients treat it as opaque.

Procedure (client side):

1. Receive `Handoff(target_zone, transfer_ticket)`; the socket closes.
   The player is already removed from the source zone.
2. Connect to `/zones/{target_zone}`; follow `Redirect` if any; send
   Hello with `ticket = transfer_ticket`.
3. Welcome → in the target zone (the zone received the carried state).
4. If the target refuses (any `Error` before Welcome, or no Welcome
   within ~10 s): connect to `/zones/{source_zone}` (the zone the client
   just left) and send the **same** Hello; the source re-admits the player
   with its carried state. If that also fails, fall back to the lobby for
   a fresh admission ticket.

Server guarantees: a transfer ticket is single use across hosts — the
target reserves it with the issuing host before admitting, confirms on
success and releases on refusal; the source accepts it back only while it
is neither reserved nor used. In doubt (an unconfirmed reservation that
lapsed) the ticket counts as used: a player is never in two zones.
Tickets expire after 30 s by default.

### 8.2 Content versions (barrier and safe rooms)

A world runs one content version. A rollout freezes every zone
(`Freeze{version: target}`), applies the new content, then resumes every
zone (`Resume{version: target}`); if any zone fails to freeze in time
all zones resume on the old version (`Resume{reason:
content_update_aborted}`). Servers pin a version per policy:

- **Strict** (default): Hello `content_version` MUST equal the server's
  for joins and reconnects; otherwise `Error(VERSION_MISMATCH)`.
- **SafeRoom** ("keep playing until a safe room"): after a *rolling*
  rollout, clients on the old version keep playing in the zone they are
  in (reconnects allowed). They may only be handed off into *gate* zones
  (safe rooms) unless the server allows roaming; on entering a gate they
  get Welcome and then `Kick(RELOAD)`. A fresh join with an old version
  gets `Error(VERSION_MISMATCH)` (message ends with `reload`).
- A *hard* rollout sends `Kick(RELOAD)` to every stale client right after
  Resume.

### 8.3 Reconnect

Reconnect: after an abrupt disconnect the player stays in the zone for
the reconnect grace window (default 15 s). A Hello with the last Welcome's
`reconnect_token` rebinds the same player (same `session_id`, new token,
fresh creates). After the window the player is removed and the token
fails with `Error(RECONNECT_FAILED)`. A fresh join of the same principal
replaces any existing connection (`Kick(REPLACED)` to the old one).

## 9. Golden fixtures

`tests/fixtures/` (regenerate with `UPDATE_GOLDEN=1 cargo test -p
distributed_game --test golden`; a drift fails the test):

- `primitives.json` — `{cases: [{kind, value, hex}]}` for varuint, varint,
  f32, f64, string, short_string.
- `quantization.json` — `{cases: [{field, input_bits, q, hex, decoded}]}`;
  `input_bits` is the input f32 bit pattern (hex, big-endian digits),
  `q` the quantized integer, `hex` its varuint bytes.
- `messages.json` — `{messages: [{name, direction, hex, decoded,
  known_entities?, schema?}]}`. Delta entries include the schema and the
  `known_entities` (`[{id, kind}]`) needed to decode updates.
- `authoring.json` (`--test authoring`) — a sample game surface and
  input / event payloads encoded with the typed payload codec (§11), with
  their expected TypeScript values; generated clients are round-tripped
  against it (`js/tests/game-generated.test.mjs`).

JSON conventions in `decoded`:

- byte arrays are lowercase hex strings;
- floats are `{"f32": number, "bits": "xxxxxxxx"}` (`bits` is the exact
  binary32 pattern; compare bits, not decimal text);
- field values are tagged: `{"bool"}`, `{"u"}`, `{"i"}`, `{"f32","bits"}`,
  `{"str"}`, `{"bytes"}`;
- schema fields are `{"name", "type", ...params}` with `type` one of
  `bool u8 u16 u32 i32 f32 quant_f32 angle short_str bytes`.

## 10. Versioning

`protocol_version` changes on any incompatible wire change. Message type
bytes `0x05..0x7f` and `0x8a..0xff`, and field type tags `10..255`, are
reserved for future versions.

Additive changes within version 1 (clients SHOULD ignore unknown kick and
error codes and treat them as "session over" / "request failed"):

- `0x89 Redirect` (server → client).
- Kick code `7` RELOAD.
- Error code `9` ZONE_UNAVAILABLE.
- Error code `10` ASLEEP.
- Transfer tickets (§8.1) and content-version policies (§8.2); `Handoff`
  and `Freeze`/`Resume` encodings are unchanged.

## 11. Typed payload codec (informative)

`Input.payload` and `Event.payload` stay game-defined; this section is not
part of the frame format and changes nothing above. It specifies the
encoding the authoring derives (`#[derive(GameInput)]`,
`#[derive(GameEvent)]`, `#[derive(GameRecord)]`, `distributed_game::codec`)
and generated clients use, so both sides of a game can agree on it from a
game surface (`GameSurface` JSON, schema v1).

A payload is its fields concatenated in declaration order, no tags or
lengths between fields. A whole payload must be consumed (trailing bytes
are an error). Field encodings:

| Wire type | Encoding |
|---|---|
| `bool`, `u8`, `u16`, `u32`, `i32`, `f32`, `quant`, `angle`, `short_str`, `bytes` | exactly as the §4 field type of the same name (`quant` = `quant_f32`) |
| `i8` | one byte, two's complement |
| `i16` | zigzag `varint`, rejected outside `-32768..=32767` |
| `u64` | unsigned LEB128 like `varuint`, up to 8 bytes, value ≤ `2^53 − 1` (encoders saturate; JavaScript decodes it exactly) |
| `f64` | binary64 LE |
| `str` | `string` (§2: `varuint` length + UTF-8) |
| `list<T>` | `varuint` count, then the items |
| `optional<T>` | `u8` 0 (absent) or 1 followed by the value; other tags are invalid |
| `record` | the record's fields in order |

An input decoded by `GameInput::from_payload` is then sanitized: fields
with a `range [min, max]` are clamped (integers truncated after the
clamp), bit-flag fields keep only their named bits, and a non-finite
`f32` rejects the whole input.

