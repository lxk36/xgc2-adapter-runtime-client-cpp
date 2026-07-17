# Adapter Runtime C++ SDK design

## Trust boundary

`ClientConfig::FromBootstrapFile` is the only identity/proof bootstrap path. It
rejects relative paths, symlinks, non-regular files, wrong ownership, modes
other than 0600, unknown format versions, malformed protobuf, and an SDK version
that conflicts with the linked library. The SDK writes its actual version into
registration; it never lets application defaults stand in for Supervisor
proofs. The embedded Runtime target must be a canonical absolute Unix socket;
network schemes and alternate relative transports are rejected before a gRPC
channel is created.

`BindCapability` can attach callbacks only to an exact contract already present
in bootstrap registration. Before applying a spec, the SDK validates the whole
candidate, including instance/process generation, monotonic revision, initial
bootstrap revision/digest, canonical scope data, version-pinned secret
references, contract identity, and endpoint subset. Only then can application
callbacks run.

## Spec transaction and capability gating

Spec application is serialized against deactivation:

1. stop accepting new dispatch and request cancellation of in-flight work;
2. wait for cooperative handlers to return;
3. validate the complete candidate before changing the application;
4. stop and clear the previous committed configuration;
5. apply top-level configuration and call `start` only for enabled contracts;
6. commit the exact revision/digest and readiness set together;
7. call `ready` for each committed capability.

`start` is pre-commit. `ready` is explicitly post-commit and is the place to
launch native workers that need Runtime Link APIs.
Missing or invalid grants remain disabled and never start a worker. If a start
fails, already-started candidate capabilities are unwound in reverse order and
the candidate revision is reported as not applied. A thrown `ready` callback
is normalized to a permanent `ready-callback-failed` apply error, terminally
fails the session, and never marks Work ready.

Callbacks execute without the state mutex held. Lifecycle callbacks are
serialized by the transition lock. Work callbacks may run concurrently up to
`dispatch_workers`; callback implementations must honor cancellation and must
not call `Client::Stop()` from a client-owned thread.

## Paired streams and fencing

Register is attempted exactly once for a process generation. As soon as that
RPC completes, the SDK overwrites and releases both in-memory copies of the
single-use bootstrap token. A running process never retries Register.

After Register succeeds, the supervisor opens Control for connection epoch 1.
It does not open Work until the exact desired spec is applied and its successful
`ApplyInstanceSpecResult` has been written on Control. Work then writes a
mandatory `WorkAttach` first frame carrying that applied revision and digest.
Control and Work form one replaceable transport pair with the same epoch.

Every frame validates:

- instance and session IDs;
- process and session generations;
- Runtime and paired-connection epochs;
- monotonically increasing per-direction frame sequence.

Failure of either stream cancels and joins both. New dispatch stops, queued and
in-flight Work is cancelled and quiesced, connection-local queues and tokens
are cleared, and local source streams receive one `source_closed` callback.
The committed spec, native capability state, and bounded terminal replay table
survive the pair replacement. This is a transport generation change, not an
application or session generation change.

The replacement pair uses the same session/process/runtime fence. An
acknowledged pair advances `connection_epoch` by exactly one; a connection
attempt that never receives a valid Host Control frame retries its candidate
epoch. When the Host repeats the same full spec revision and digest,
the SDK reports it applied without rerunning `apply_instance_spec`, `start`, or
`ready`. Only a genuinely newer spec performs the stop/clear/apply/start/ready
transaction.

Pair retries use bounded exponential backoff. A rejected session/fence/epoch or
an exhausted retry budget stops native state, enters `kSessionLost`, and invokes
`session_lost` once. The owning process must exit so the Supervisor can launch
a new process generation with a new one-time bootstrap proof.

## Dispatch, cancellation, and replay

The Work reader never runs application handlers. It validates the frame fence,
places unary/operation requests in a bounded queue, and remains responsive to
cancellation and stream credit. Worker threads revalidate the committed spec
immediately before invoking a handler.

TTL is relative to SDK receipt. The SDK computes the minimum of absolute
deadline and receipt-plus-TTL, stores that as `deadline_unix_nanos`, and clears
`ttl_ms` before the callback. A callback therefore receives one unambiguous
dispatch deadline.

Operation accepted/started events are SDK-owned. Handler output must be
terminal. Admission reserves a terminal identity slot and worst-case frame
bytes before a handler can run. Terminal unary/operation frames are cached by
work ID plus request digest without an eviction path. Identical redelivery
replays the result; different content reusing an ID is a protocol violation.
The cache is session-local rather than pair-local, so redelivery after a
transport replacement cannot rerun a completed non-idempotent native
operation.

After durably committing a terminal, the Host sends
`TerminalAcknowledgement`. Its digest is SHA-256 over the fully-qualified
terminal protobuf name, one NUL byte, and that submessage's deterministic
protobuf encoding. The type domain prevents different terminal message types
with identical wire bytes from colliding. An exact Work acknowledgement frees
the replay entry; an exact source acknowledgement frees its connection-local
tombstone. Wrong digest, wrong identity kind, and active-identity
acknowledgements produce `WorkProtocolError`; an already released duplicate is
absorbed idempotently.

## Host-owned source lifecycle

The Host alone creates source identities. A valid `SourceOpenRequest` names a
granted `STREAM_SOURCE` endpoint, uses nonempty `WorkContext.work_id` as its
sole stream identity, and supplies positive initial message and byte credit.
Every Adapter response echoes that identity as `stream_id`.
The SDK reserves source identity and byte state before dispatching
`source_open`. Data publication is fenced until the callback accepts and the
corresponding `SourceOpenResult` has entered the bounded Work queue.

Every accepted `PublishSource` preflights the complete framed message, consumes
message and byte credit atomically with queue admission, and advances sequence
only after admission succeeds. `CloseSource` and Host cancellation emit one
`SourceClose`; rejected opens and closes remain exact tombstones until Host
acknowledgement. No timeout or FIFO path can forget an unacknowledged terminal.

## Backpressure and shutdown

Control, Work, dispatch, terminal replay/reservations, and source state all
have independent entry and byte bounds. Complete frames include their
worst-case fenced `SessionHeader` in byte accounting. A source write is
accepted only when both peer-granted message credit and byte credit cover the
complete chunk; credit is consumed atomically with queue admission. Exhausting
an identity reservation retires the connection pair instead of accepting Work
whose terminal could not be retained.

Drain stops new dispatch and reports completion after queued and in-flight work
reach zero. Remote stop and local `Stop()` request cancellation, cancel all gRPC
contexts, join stream/supervisor/dispatch threads, stop active capabilities,
clear the full spec, and return only after callbacks can no longer run.
