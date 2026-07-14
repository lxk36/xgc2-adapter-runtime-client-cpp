# AdapterLink client design

## Boundary

This product is a transport and session client. It does not know ROS, MAVROS,
Scout messages, a robot namespace, or how a semantic operation is executed.
Concrete adapters own those decisions through callbacks.

The public protocol target is generated at product build time from the schemas
installed by `xgc2-protobuf-dev`. Generated files are installed as part of this
binary development package so every C++ adapter uses the same generated ABI.

## Threads and callbacks

After a successful synchronous `Start()`, the client owns three workers:

1. lifecycle: heartbeat, plan reload, session invalidation, and reconnect;
2. telemetry: bounded batch construction and `PushTelemetry`;
3. operations: `StreamOperations`, handler dispatch, and event reporting.

`validate_and_apply_plan` and `clear_plan` are serialized by the plan-transition
lock. `handle_operation` is also serialized against a plan transition and is
never called concurrently for two operations. Telemetry producers may call
`Publish` concurrently.

Callbacks execute without the client state mutex held. A callback may publish
telemetry or inspect `session()`, but it must not call `Stop()` because Stop
joins the worker which may currently be executing that callback.

## Session failure

Transport failure, a rejected batch, a changed plan revision, or a closed
operation stream invalidates the current session. Invalidation performs:

1. cancel the operation stream;
2. clear identity and queued telemetry;
3. invoke `clear_plan` exactly once for that applied plan;
4. retry registration using bounded exponential backoff.

The bootstrap credential remains in memory for reconnect after a Core restart.
The file is never rewritten or deleted by this library.

## Operation semantics

The library validates session/revision, robot membership, enabled channel, and
the channel advertisement before invoking application code. It reports
`accepted`, then `started`, and finally exactly one terminal event. The last
terminal result is cached by operation ID. Delivery of the same serialized
request replays the terminal result; reuse of an ID with different bytes is
rejected.

The cache is bounded FIFO and intentionally process-local. Durable operation
truth belongs to Core.
