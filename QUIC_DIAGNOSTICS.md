# QUIC Implementation Diagnostics

## Current Issues (quic-impl-1 branch)

### Issue 1: `peer_bidi_stream_count` in wrong place (CONFIRMED FIX NEEDED)
- **Status**: Fixing now
- **Problem**: `peer_bidi_stream_count: 100` and `peer_unidi_stream_count: 10` are passed in `accept_opts` in `QuicListener.handle_info(:accept)`, but **quicer ignores accept_opts since v0.2**
- **Evidence**: `deps/quicer/c_src/quicer_connection.c:902` — `// @NOTE: since 0.2, we ignore argv[1]`
- **Effect**: Default `peer_bidi_stream_count` of 1 is used, so only 1 bidi stream works per connection. Rust SDK needs 1 control + 2N data streams.
- **Fix**: Move these to `listen_opts` in `QuicListener.init/1`

### Issue 2: `handshake/1` returns `:invalid_state` (FIXED)
- **Status**: Fixed
- **Problem**: `:quicer.accept/3` can return a connection already in CONNECTED state. Calling `handshake/1` on it returns `:invalid_state`, causing connection close.
- **Evidence**: `deps/quicer/src/quicer.erl` — accept receives either `{quic, new_conn, C, _}` or `{quic, connected, C, _}`
- **Fix**: Handle `:invalid_state` as "already connected" and proceed to stream accept

### Issue 3: BootLoader 3s timeout too short for QUIC (INVESTIGATING)
- **Status**: Investigating
- **Problem**: `config/test.exs` post_hooks wait 3 seconds for `compute_engine_connected_event`. QUIC handshake + stream setup + control channel init may exceed this.
- **Evidence**: BootLoader crashes with "ComputeEngine not connected" before any QUIC control stream is accepted
- **Effect**: Supervisor shuts down, killing QuicListener, which closes all QUIC connections. Rust clients see `ApplicationClosed(error_code: 0)`
- **Note**: May resolve once Issue 1 is fixed (stream count limit could be causing Rust SDK failures that cascade)

### Issue 4: PortPilot restart counter (MITIGATED)
- **Status**: Counter raised to 10
- **Problem**: With 2 external processes, rapid failures hit the counter limit quickly. Each PortPilot restart spawns 2 new processes which also fail, doubling the failure rate.
- **Fix**: Raised `@port_down_counter` from 1 to 10. Root cause fix is making QUIC work so processes don't crash.

### Issue 5: `accept_stream` returns `:closed` (DOWNSTREAM)
- **Status**: Downstream of BootLoader crash
- **Problem**: After BootLoader crashes and supervisor shuts down, QuicListener is killed. All accepted QUIC connections are closed. `accept_stream` returns `:closed`.
- **Evidence**: Appears AFTER BootLoader crash timestamp. Rust sees `ApplicationClosed(error_code: 0)`.
- **Fix**: Should resolve when upstream issues are fixed.

## Architecture Notes

### Quicer option flow (from C source analysis)
```
listen(port, listen_opts)     → Creates QUIC_SETTINGS + TLS config (cert/key/alpn)
accept(listener, [], timeout) → Accept opts IGNORED since v0.2, config inherited from listener
handshake(conn)               → Applies config via ConnectionSetConfiguration, starts TLS
handshake(conn, override_opts)→ Can override specific settings before TLS starts
accept_stream(conn, [])       → Waits for peer to open a stream
```

### Correct listen_opts should include:
- `certfile`, `keyfile`, `alpn` (TLS)
- `peer_bidi_stream_count: 100` (allow SDK to open many bidi streams)
- `peer_unidi_stream_count: 10`
- `idle_timeout_ms: 30_000`

### Supervisor child order (relevant to timing)
```
EventManager → BootLoader → Identificator → HubManager → FleetSupervisor →
FleetManager → QuicListener → PluginManager → PortPilot → Orchestrator
```
BootLoader starts BEFORE QuicListener and PortPilot. Its `handle_continue` fires asynchronously.

## Test Command
```bash
# In Docker dev container:
docker exec flezha_agent_dev bash -c '. "$HOME/.cargo/env" && cd /flezha && FLEZHA_TRANSPORT=quic mix test test/quic_test.exs'
```
