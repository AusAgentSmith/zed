# Zed Mobile Companion — Build Plan

A mobile app that connects to a running Zed instance on the desktop, giving full
bidirectional access to Zed's terminal panes from Android (and iOS). Zed owns all
state; the mobile app is a view and input surface only.

---

## Architecture Overview

```
Zed (modified fork)
  Terminal Pane 1 ──> PTY 1 ──┐
  Terminal Pane 2 ──> PTY 2 ──┤
  Terminal Pane N ──> PTY N ──┴──> zed_local_api (WS :7700, localhost)
                                          │
                            ┌─────────────┴─────────────┐
                         On LAN                     Remote (Tailscale)
                    direct WebSocket              same direct WebSocket
                                                 via Tailscale IP
                            └─────────────┬─────────────┘
                                    Mobile App
                              React Native + Expo
                          xterm.js terminal  │  CodeMirror code view
                          custom key toolbar │  terminal list / nav
```

**Core principle:** State lives entirely in Zed. The mobile client is stateless.
On any disconnect or reconnect, the client requests current state from Zed and
continues from there. Nothing is buffered or persisted on the client or in a relay.

**Connectivity:** Tailscale handles network reachability. No relay server required.
Direct WebSocket from phone to the machine running Zed, wherever that is.

---

## Repositories

| Repo | Description |
|------|-------------|
| `zed` (this repo, fork) | Adds `crates/zed_local_api` + PTY tap hooks |
| `zed-mobile` (new) | React Native + Expo mobile app |

---

## Phase 1 — Core Terminal Access

### 1.1 — Zed: PTY byte tap (`crates/terminal`)

**Goal:** Make raw PTY output available to subscribers outside the terminal model,
and accept raw input bytes from outside to write to the PTY.

**Changes to `crates/terminal/src/terminal.rs`:**

- Add a `tokio::sync::broadcast::Sender<Bytes>` to the `Terminal` struct, created
  at PTY spawn time.
- At the point where alacritty reads bytes from the PTY (before VT parsing), clone
  the bytes into the broadcast sender.
- Add a `write_to_pty(bytes: Bytes)` method that writes directly to the PTY fd,
  bypassing the normal UI input path.
- Expose a `subscribe_output() -> broadcast::Receiver<Bytes>` method.
- Add a `scrollback_snapshot() -> Vec<Bytes>` method that serialises the current
  alacritty scrollback buffer back to raw byte sequences for replay on reconnect.

**No changes to terminal rendering.** Zed's display is unaffected; this is purely
an additive tap.

---

### 1.2 — Zed: Local API server (`crates/zed_local_api`)

New crate. Axum-based WebSocket + HTTP server, localhost-only, port configurable
in Zed settings (default `7700`).

**Endpoints:**

```
GET  /terminals
     → JSON list of open terminal panes:
       [{ id, title, rows, cols, is_active }]

WS   /terminals/:id
     → On connect:   send scrollback snapshot as raw bytes (replay)
     → Ongoing:      stream live raw PTY output bytes
     → Inbound:      raw bytes written directly to the PTY
     → On disconnect: clean up subscriber, Zed continues unaffected

GET  /buffers
     → JSON list of open buffers: [{ path, language, is_dirty }]

GET  /buffers/*path
     → Plain text content of the named buffer as currently held in Zed
       (unsaved changes included)
```

**Connection resilience contract:**
- Server never buffers for a disconnected client.
- Reconnect always triggers a fresh scrollback replay so the client can rebuild its
  view from current Zed state.
- Terminal list and buffer list are always live reads — no cached copies.

**Security:** Bind to `127.0.0.1` only. No auth on LAN (Tailscale ACLs provide
the perimeter). A shared secret header option can be added later if needed.

**Wiring:** The server is started in Zed's app init. A `TerminalRegistry` (simple
`Arc<Mutex<HashMap<TerminalId, Terminal>>>`) is updated as panes open and close,
and passed to the API server.

---

### 1.3 — Zed: Settings integration

Add to Zed settings schema:

```json
{
  "local_api": {
    "enabled": true,
    "port": 7700
  }
}
```

Disabled by default; documented in Zed's settings docs.

---

### 1.4 — Mobile app: Project setup (`zed-mobile`)

- React Native 0.76+ with **Expo** (managed workflow)
- **NativeWind** for styling (Tailwind-compatible utility classes)
- **Expo Router** for navigation (file-based, native tabs + stack)
- TypeScript throughout

Dependencies:
| Package | Purpose |
|---------|---------|
| `@xterm/xterm` + `@xterm/addon-fit` + `@xterm/addon-webgl` | Terminal rendering |
| `react-native-webview` | Host xterm.js |
| `react-native-keyboard-controller` | Keyboard height + inset management |
| `socket.io-client` | WebSocket with auto-reconnect |
| `@shopify/react-native-skia` | (optional) key toolbar rendering |
| `react-native-mmkv` | Fast local KV store for connection profiles |
| `expo-secure-store` | Store Tailscale IP / connection config |

---

### 1.5 — Mobile app: Connection management

- **Connection profiles**: name + Tailscale IP + port, stored in secure store.
- **Auto-reconnect**: exponential backoff with jitter, max 30s interval.
- **Reconnect flow**:
  1. Re-establish WebSocket.
  2. Fetch `/terminals` to rebuild the terminal list.
  3. For each previously-open terminal, reconnect the WS stream and receive
     scrollback replay — xterm.js is reset and replayed from Zed's state.
  4. Refetch any open buffer views.
- **UI state**: show a non-blocking "reconnecting…" banner; do not discard the
  last-seen terminal content until the replay arrives.

---

### 1.6 — Mobile app: Terminal list screen

- Displays open Zed terminal panes by title (polls `/terminals` on focus).
- Tap to open terminal view.
- Pull-to-refresh.
- Visual indicator for active/foreground pane in Zed.

---

### 1.7 — Mobile app: Terminal view

**Rendering:**
- `xterm.js` with `@xterm/addon-webgl` runs inside a `react-native-webview`.
- WebView is input-blocked — no keyboard events reach it directly.
- Raw PTY bytes arrive via WebSocket and are pushed to xterm.js via
  `webviewRef.injectJavaScript(...)`.

**Input:**
- A standard `TextInput` (transparent, zero-size) holds focus and captures the
  soft keyboard.
- All `KeyEvent`s are intercepted natively before the WebView sees them.
- Character input and special keys are serialised to the correct terminal byte
  sequences and sent over the WebSocket.
- Modifier state (Ctrl active, Alt active) is tracked in React state.

**Key toolbar** (persistent row above the soft keyboard):
```
[ ESC ][ TAB ][ CTRL ][ ALT ][ ↑ ][ ↓ ][ ← ][ → ][ | ][ ~ ][ / ][ ` ]
```
- CTRL and ALT are toggle modifiers — highlight when active.
- Haptic feedback on each key.
- Configurable: long-press to assign custom macro sequences.

**Terminal sizing:**
- On layout change, calculate rows/cols from the xterm.js character cell size.
- Send a resize message over the WebSocket; Zed propagates to the PTY via
  `ioctl(TIOCSWINSZ)`.

---

### 1.8 — Mobile app: Code panel

- Accessible from a tab or swipe within terminal view context.
- Fetches buffer list from `/buffers`, lets user pick a file.
- Renders content in a **CodeMirror 6** WebView (syntax highlighting, read-only).
- Auto-refreshes when switching back to the tab (re-fetches from Zed).
- Language detected from buffer metadata returned by Zed.

---

### 1.9 — Scrollback buffer sizing

- Increase alacritty's default scrollback in the Zed local API server config.
- Recommend `10000` lines as default for agent sessions (long-running outputs).
- Make configurable in `local_api` settings.

---

## Phase 2 — Voice AI Agent (Aspirational)

The goal: speak naturally to drive the AI agents running in your Zed terminals,
with full awareness of what's on screen and in your open files.

- **Voice capture** on the mobile device (continuous push-to-talk or VAD-gated).
- **Speech-to-text** via Whisper (API or on-device via `whisper.rn`).
- **Claude API with tool use** — the agent's tools are the Zed API:
  - `list_terminals()` → GET /terminals
  - `read_terminal(id)` → snapshot of current terminal content
  - `send_to_terminal(id, text)` → write to PTY over WS
  - `read_buffer(path)` → GET /buffers/{path}
  - `list_buffers()` → GET /buffers
- **Context window management** — inject relevant terminal scrollback + active
  buffer content as context before each turn.
- **Text-to-speech** response delivered via earpiece (ElevenLabs or Claude TTS).
- **LiveKit** for low-latency audio transport between phone and a local agent
  process (avoiding round-trip to cloud for audio streaming).
- **Approval-gated actions** — voice commands that write to a terminal show a
  confirmation UI before sending, configurable per-session.
- **Ambient monitoring mode** — agent watches terminal output passively and speaks
  up when a build fails, a test suite completes, or an agent asks a question.
- **Session continuity** — voice conversation history persists across reconnects,
  scoped to the Zed session.

---

## Reference

- **Happier** (`github.com/happier-dev/happier`) — reference only (not forked).
  Useful for: xterm.js + React Native WebView integration pattern, LiveKit voice
  integration approach.
- **alacritty_terminal** — scrollback buffer API and VT event model used by Zed's
  terminal crate.
- **Axum** — already in the Zed dependency tree via `crates/collab`.
- **Tailscale** — provides the secure network tunnel; no relay server needed.
