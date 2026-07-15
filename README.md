# xgc2-adapter-link-client-cpp

Release 0.2 consumes the XGC2 protocol 0.3 envelope and PX4 diagnostic
semantics. Registry identity is validated once during session registration;
individual messages carry only routing, time, message ID, sequence, and
protobuf payload.

`xgc2-adapter-link-client-cpp` is the ROS-independent C++ client for the XGC2
AdapterLink protocol. Robot integrations keep their native middleware at the
edge and reuse this library for the complete Core session lifecycle.

The library owns:

- the gRPC channel to the Core Unix-domain socket;
- bootstrap registration, protocol/profile advertisement, and plan activation;
- heartbeat, session invalidation, and bounded reconnect backoff;
- bounded telemetry batching;
- the operation stream, operation-event reporting, and terminal-result replay;
- deterministic stop and cancellation of an active stream.

It deliberately contains no ROS headers, ROS clocks, topic names, robot
profiles, or robot-specific behavior. A PX4 ROS 1 adapter and a Scout ROS 1
adapter are separate processes which link the same client library.

## Install

```bash
sudo apt update
sudo apt install libxgc2-adapter-link-client-dev
```

The development package depends on `xgc2-protobuf-dev`, the system protobuf
runtime, and the system gRPC++ runtime.

`.xgc2/dependencies/xgc2-protobuf.env` locks the exact protocol source revision
and minimum product version used by CI and by the generated Debian dependency.

## Consume with CMake

```cmake
find_package(xgc2_adapter_link_client REQUIRED CONFIG)

target_link_libraries(my_adapter
  PRIVATE
    xgc2::adapter_link_client
)
```

The package exports two targets:

- `xgc2::adapter_link_client` is the high-level lifecycle client.
- `xgc2::adapter_link_protocol` contains the generated XGC2 protobuf/gRPC C++
  bindings and is already a public dependency of the client target.

Consumers do not run `protoc`, do not link gRPC directly, and may include both
the client API and generated semantic messages:

```cpp
#include <xgc2/adapter_link/client.hpp>
#include <xgc/semantic/aerial/v1/control.pb.h>
```

## Minimal integration

```cpp
xgc2::adapter_link::ClientConfig config;
config.adapter_id = "px4-adapter-run-42";
config.socket_path = "/run/xgc2/adapter/adapter-link.sock";
config.bootstrap_token_file = "/run/xgc2/adapter/run-42.token";
config.native_protocol = "ros1";
config.software_version = "1.0.0";
config.supported_protocol_versions = {2};
config.registry_fingerprint = generated_contract::kRegistryFingerprint;
config.supported_profiles = {generated_contract::Px4Profile()};

xgc2::adapter_link::ClientCallbacks callbacks;
callbacks.validate_and_apply_plan =
    [](const xgc::adapter::v1::AdapterPlan& plan, std::string* error) {
      return px4_runtime.ReplacePlan(plan, error);
    };
callbacks.clear_plan = [] { px4_runtime.Clear(); };
callbacks.handle_operation = [](const auto& request) {
  return px4_runtime.Execute(request);
};

xgc2::adapter_link::Client client(std::move(config), std::move(callbacks));
std::string error;
if (!client.Start(&error)) {
  throw std::runtime_error(error);
}

client.Publish(plan_revision, std::move(telemetry));
client.Stop();
```

`Start()` returns only after Core has accepted registration, the application
has atomically applied the first plan, and Core has acknowledged that plan by
heartbeat. After that point transport loss is handled inside the client. The
application's `clear_plan` callback is invoked before a stale session is
replaced.

See [docs/design.md](docs/design.md) for callback and concurrency guarantees.

## Build and test

The schema development package must already be installed:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
(cd build && ctest --output-on-failure)
```
