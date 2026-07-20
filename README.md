# xgc2-adapter-runtime-client-cpp

`xgc2-adapter-runtime-client-cpp` is the generic C++14 SDK for
`xgc.adapter.v1.AdapterRuntimeLinkService`. It owns the complete process-side
Runtime Link without knowing any native middleware or resource domain.

The SDK provides:

- one trusted binary `AdapterProcessBootstrap` input;
- registration proof and immutable capability-contract advertisement;
- single-use registration plus replaceable paired Control and Work streams;
- atomic full-spec application and capability readiness;
- bounded unary/operation dispatch, cooperative cancellation, and bounded
  terminal replay;
- Host-opened source streams with strict message-and-byte credit;
- exact terminal acknowledgement for Work and source identity release;
- bounded same-session reconnect, terminal session-loss reporting,
  deterministic shutdown, and bounded queues.

There is no legacy API or compatibility layer in this package.

## Bootstrap contract

Every process receives exactly one argument:

```text
--adapter-bootstrap-file /absolute/path/bootstrap.pb
```

The Process Supervisor writes that file as a binary
`xgc.adapter.v1.AdapterProcessBootstrap`, owned by the process user with mode
0600. It contains the Runtime target, trusted `RegisterRequest`, and first full
`AdapterInstanceSpec`. Applications do not synthesize instance identity,
generation, token, definition/build/manifest proof, or capability contracts.
The Runtime target is restricted to a canonical absolute Unix socket; the SDK
does not accept TCP, DNS, or relative transport targets.

```cpp
auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(path);

// The first complete spec is available before native-runtime initialization.
const auto& initial = config.initial_spec();

xgc2::adapter_runtime::CapabilityCallbacks handlers;
handlers.start = [](const auto& full_spec, const auto& grant,
                    std::string* error) {
  return application.PrepareAndStart(full_spec, grant, error);
};
handlers.ready = [] {
  // Post-commit hook. Native workers may start here.
  application.OnCapabilityReady();
};
handlers.stop = [] { application.StopCapability(); };
handlers.unary = [](const auto& request, const auto& cancellation) {
  return application.HandleUnary(request, cancellation);
};
handlers.operation = [](const auto& request, const auto& cancellation) {
  return application.HandleOperation(request, cancellation);
};
handlers.source_open = [](const auto& request, const auto& cancellation) {
  return application.AcceptHostSource(request, cancellation);
};
handlers.source_closed = [](const auto& request, const auto& error) {
  application.StopSource(request.context().work_id(), error);
};

std::string error;
if (!config.BindCapability("example.native", 1, contract_digest,
                           std::move(handlers), &error)) {
  throw std::runtime_error(error);
}

xgc2::adapter_runtime::ClientCallbacks lifecycle;
lifecycle.apply_instance_spec = [](const auto& spec, std::string* error) {
  return application.ApplyTopLevelConfiguration(spec, error);
};
lifecycle.clear_instance_spec = [] { application.ClearConfiguration(); };
lifecycle.session_lost = [](const std::string& reason) {
  // Signal the owning main loop and exit. A new process generation must receive
  // a new bootstrap proof; this client never retries Register.
  application.RequestProcessExit(reason);
};

xgc2::adapter_runtime::Client client(std::move(config), std::move(lifecycle));
if (!client.Start(&error)) {
  throw std::runtime_error(error);
}
```

`start` is a pre-commit capability hook. `ready` runs after the entire spec and
every enabled capability have committed; native workers start there. A thrown
`ready` callback is a terminal apply failure and the client never advertises
the capability as ready. Capabilities
absent from the valid spec never receive `start` or `ready`, so worker creation
is gated generically by the exact contract and endpoint grant.

## Work semantics

Register consumes the bootstrap token once and the SDK erases its in-memory
copies when the RPC completes. The first Adapter-to-Host Work frame is always
`WorkAttach`, but Work is opened only after the exact spec's successful apply
result has been written on Control. Control and Work are replaced as one pair,
using the same `connection_epoch`; failure of either stream fences both halves.

Pair replacement remains inside the registered session. It cancels pair-local
Work and source streams but preserves the committed spec, native capability
state, and terminal replay cache. The Host's repeated exact spec is acknowledged
without repeating lifecycle callbacks. An acknowledged pair advances the epoch
by one, so successive replacements use epochs 2, 3, and so on. Retry-budget
exhaustion or a session/fence/epoch rejection enters `kSessionLost` and invokes
`session_lost` once; recovery then requires process exit and a new generation
with a new Register proof.

Unary and operation handlers run on a bounded worker pool. The SDK validates
session, spec revision, exact contract ID/version/digest, enabled endpoint,
interaction mode, schema, request size, deadline, and idempotency before a
callback runs. `Deadline.ttl_ms` is measured from SDK receipt and normalized to
the earliest absolute deadline; callbacks always see `ttl_ms == 0` plus the
effective `deadline_unix_nanos`.

Operation handlers receive a cooperative `CancellationToken`. Accepted and
started events are emitted by the SDK. Before accepting unary or operation
Work, the SDK reserves both an identity slot and bytes for its eventual
terminal. The exact terminal is retained without FIFO eviction; redelivery
with the same work ID and request digest replays it without calling the handler
again.

The Host releases retained state with `TerminalAcknowledgement` only after its
terminal commit is durable. `terminal_digest` is
`sha256(fully-qualified protobuf message name || NUL || deterministic protobuf
bytes)` and therefore also binds the terminal message type. A wrong digest,
wrong identity kind, or acknowledgement of active Work is a protocol error.
An exact acknowledgement releases the retained identity and byte budget;
duplicates after release are idempotent.

## Source streams and credit

Only the Host can open a source. It sends an exact `SourceOpenRequest` for a
granted `STREAM_SOURCE` endpoint, with `WorkContext.work_id` as the sole stream
identity and positive initial message and byte credit. Adapter responses echo
that identity as `stream_id`. The SDK invokes `source_open`; the
Adapter may accept that exact identity or return a typed rejection. Publication
is unavailable while the callback is running, and the SDK queues the accepted
`SourceOpenResult` before making `PublishSource` available.

`PublishSource(stream_id, items)` consumes both message and byte credit
atomically with bounded Work-queue admission. It returns `kNoCredit` without
buffering when either grant is insufficient. `CloseSource` queues the sole
terminal `SourceClose`. Rejected opens and closes remain pair-local tombstones
until an exact source `TerminalAcknowledgement` releases them; they are never
forgotten by FIFO eviction. Host cancellation is acknowledged with
`SourceClose` and produces at most one `source_closed` callback.

## Consume with CMake

```cmake
find_package(xgc2_adapter_runtime_client REQUIRED CONFIG)
target_link_libraries(my_adapter PRIVATE xgc2::adapter_runtime_client)
```

Public targets:

- `xgc2::adapter_runtime_client`
- `xgc2::adapter_runtime_protocol`

Headers:

```cpp
#include <xgc2/adapter_runtime/client.hpp>
#include <xgc2/adapter_runtime/version.hpp>
```

## Debian package boundary

`libxgc2-adapter-runtime-client1` owns only ABI-1 shared objects and declares
their system-library requirements through Debian shlibs metadata. Deployed
Adapter executables depend on this SONAME package, so a compatible SDK rebuild
does not force them to install headers or protocol schema sources.

`libxgc2-adapter-runtime-client-dev` keeps the existing CMake and pkg-config
consumer interface. It owns the unversioned linker symlinks, public/generated
headers, and build metadata, and depends on the exact matching ABI package.
Upgrading the existing `-dev` package therefore installs the split runtime
package automatically. ABI-breaking changes must increment both `SOVERSION`
and the runtime package suffix; they must never replace ABI 1 in place.

## Build and test

Point CMake at an exact `xgc2-protobuf-dev` 0.5.0-1 prefix containing Runtime
Link protocol v2:

```bash
cmake -S . -B build \
  -DCMAKE_PREFIX_PATH=/path/to/xgc2-protobuf-prefix/usr \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
(cd build && ctest --output-on-failure)
```

See [docs/design.md](docs/design.md) for concurrency and lifecycle invariants.
