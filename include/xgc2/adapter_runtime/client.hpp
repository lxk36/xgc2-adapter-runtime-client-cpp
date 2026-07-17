#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "xgc/adapter/v1/adapter.pb.h"
#include "xgc/v1/message.pb.h"

namespace xgc2 {
namespace adapter_runtime {

namespace internal {
class ClientConfigAccess;
}

enum class LogLevel {
  kDebug,
  kInfo,
  kWarning,
  kError,
};

enum class ClientState {
  kStopped,
  kConnecting,
  kRegistered,
  kApplyingSpec,
  kReady,
  kDraining,
  kReconnecting,
  kSessionLost,
  kStopping,
};

enum class SourceWriteResult {
  kAccepted,
  kNotReady,
  kUnknownStream,
  kNoCredit,
  kQueueFull,
  kTooLarge,
};

struct CancellationState {
  std::atomic<bool> requested{false};
};

class CancellationToken {
 public:
  CancellationToken() = default;
  explicit CancellationToken(std::shared_ptr<CancellationState> state);

  bool IsCancellationRequested() const noexcept;

 private:
  std::shared_ptr<CancellationState> state_;
};

struct UnaryResult {
  bool succeeded = false;
  xgc::v1::Payload output;
  xgc::adapter::v1::AdapterError error;

  static UnaryResult Success(xgc::v1::Payload output = {});
  static UnaryResult Failure(xgc::adapter::v1::ErrorClass error_class, std::string code,
                             std::string message, std::uint32_t retry_after_ms = 0);
};

struct OperationResult {
  xgc::adapter::v1::OperationPhase phase = xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED;
  bool has_output = false;
  xgc::v1::Payload output;
  xgc::adapter::v1::AdapterError error;
  std::int32_t native_code = 0;

  static OperationResult Success(xgc::v1::Payload output = {}, bool has_output = false);
  static OperationResult Failure(xgc::adapter::v1::ErrorClass error_class,
                                 std::string code, std::string message,
                                 std::int32_t native_code = 0);
  static OperationResult Rejected(std::string code, std::string message);
  static OperationResult Cancelled(std::string message = "operation cancelled");
};

// Decision returned by a STREAM_SOURCE open handler. The Host owns the
// WorkContext.work_id stream identity and initial credit; the Adapter can only
// accept that exact request or reject it with a typed AdapterError.
struct SourceOpenDecision {
  bool accepted = false;
  bool has_initial_payload = false;
  xgc::v1::Payload initial_payload;
  xgc::adapter::v1::AdapterError error;

  static SourceOpenDecision Accept();
  static SourceOpenDecision Accept(xgc::v1::Payload initial_payload);
  static SourceOpenDecision Reject(xgc::adapter::v1::ErrorClass error_class,
                                   std::string code, std::string message,
                                   std::uint32_t retry_after_ms = 0);
};

struct CapabilityCallbacks {
  // Invoked only after the full spec and exact contract/endpoint grant have
  // been validated. A false result prevents the capability from becoming
  // ready and prevents the spec revision from being committed.
  std::function<bool(const xgc::adapter::v1::AdapterInstanceSpec&,
                     const xgc::adapter::v1::EnabledCapability&, std::string*)>
      start;

  // Invoked when an active capability is disabled by a new spec, the registered
  // session is lost, or the client stops. Transport-pair replacement alone does
  // not stop native capability state.
  std::function<void()> stop;

  // Runs after the complete spec and every enabled capability have committed.
  std::function<void()> ready;

  std::function<UnaryResult(const xgc::adapter::v1::UnaryRequest&,
                            const CancellationToken&)>
      unary;
  std::function<OperationResult(const xgc::adapter::v1::OperationRequest&,
                                const CancellationToken&)>
      operation;

  // Invoked for an exact Host-owned STREAM_SOURCE request. PublishSource is
  // intentionally unavailable for this stream until the callback returns an
  // accepted decision and the SDK has queued SourceOpenResult first.
  std::function<SourceOpenDecision(const xgc::adapter::v1::SourceOpenRequest&,
                                   const CancellationToken&)>
      source_open;

  // Reports Host cancellation or loss of the owning connection/session for an
  // accepted or opening source. Each such source is reported at most once.
  std::function<void(const xgc::adapter::v1::SourceOpenRequest&,
                     const xgc::adapter::v1::AdapterError&)>
      source_closed;
};

struct CapabilityBinding {
  xgc::adapter::v1::CapabilityContract contract;
  CapabilityCallbacks callbacks;
};

struct ClientCallbacks {
  // Atomically applies top-level, domain-owned configuration before any
  // capability start callback runs. Contract and endpoint grants are validated
  // by the SDK before this callback is invoked.
  std::function<bool(const xgc::adapter::v1::AdapterInstanceSpec&, std::string*)>
      apply_instance_spec;

  // Clears top-level applied configuration after all active capability stop
  // callbacks have completed.
  std::function<void()> clear_instance_spec;

  std::function<void(const xgc::adapter::v1::StopRequest&)> stop_requested;

  // Reports an irrecoverable loss of the registered Runtime session. This is
  // invoked at most once, after the paired streams and native callbacks have
  // stopped. It runs on the supervisor thread and therefore must not call
  // Client::Stop(); applications should notify their owning main loop and exit
  // so the Process Supervisor can launch a new process generation.
  std::function<void(const std::string&)> session_lost;
  std::function<void(LogLevel, const std::string&)> log;
};

class ClientConfig {
 public:
  // Reads and validates one mode-0600 binary AdapterProcessBootstrap. Identity
  // proofs and contracts come only from this trusted Supervisor handoff.
  static ClientConfig FromBootstrapFile(const std::string& path);

  // Attaches application callbacks to an exact trusted bootstrap contract.
  // A handler cannot introduce or widen a capability contract.
  bool BindCapability(std::string capability_id, std::uint32_t contract_version,
                      std::string contract_digest, CapabilityCallbacks callbacks,
                      std::string* error = nullptr);

  const std::string& runtime_target() const noexcept { return runtime_target_; }
  const xgc::adapter::v1::RegisterRequest& registration() const noexcept {
    return registration_;
  }
  const xgc::adapter::v1::AdapterInstanceSpec& initial_spec() const noexcept {
    return initial_spec_;
  }
  const std::vector<CapabilityBinding>& capabilities() const noexcept {
    return capabilities_;
  }

  std::uint32_t rpc_timeout_ms = 3000;
  std::uint32_t initial_connect_timeout_ms = 10000;
  std::uint32_t reconnect_initial_delay_ms = 250;
  std::uint32_t reconnect_max_delay_ms = 5000;
  // Retries after the initial pair. Exhaustion terminally loses the session.
  std::size_t maximum_pair_reconnect_attempts = 8;
  std::size_t maximum_control_queue = 128;
  std::size_t maximum_work_queue = 4096;
  std::size_t maximum_dispatch_queue = 1024;
  std::size_t maximum_terminal_replay = 1024;
  std::size_t maximum_source_streams = 128;
  // Total Host source identities retained for one connection pair until
  // durable terminal acknowledgements release them.
  std::size_t maximum_source_tombstones = 1024;
  std::size_t maximum_control_queue_bytes = 4U * 1024U * 1024U;
  std::size_t maximum_work_queue_bytes = 16U * 1024U * 1024U;
  std::size_t maximum_dispatch_queue_bytes = 16U * 1024U * 1024U;
  std::size_t maximum_terminal_replay_bytes = 16U * 1024U * 1024U;
  std::size_t maximum_source_state_bytes = 16U * 1024U * 1024U;
  std::size_t dispatch_workers = 1;

 private:
  friend class internal::ClientConfigAccess;

  ClientConfig() = default;

  std::string runtime_target_;
  xgc::adapter::v1::RegisterRequest registration_;
  xgc::adapter::v1::AdapterInstanceSpec initial_spec_;
  std::vector<CapabilityBinding> capabilities_;
};

struct SessionSnapshot {
  ClientState state = ClientState::kStopped;
  std::string host_id;
  std::string session_id;
  std::uint64_t process_generation = 0;
  std::uint64_t session_generation = 0;
  std::uint64_t runtime_epoch = 0;
  std::uint64_t spec_revision = 0;
  std::string spec_digest;
  std::uint32_t runtime_link_protocol_version = 0;
  std::uint32_t heartbeat_interval_ms = 0;
  std::size_t in_flight_work = 0;
  std::uint64_t rejected_dispatches = 0;
  std::uint64_t dropped_outbound_frames = 0;
  std::string last_error;
  std::vector<xgc::adapter::v1::CapabilityStatus> capabilities;
};

class Client {
 public:
  Client(ClientConfig config, ClientCallbacks callbacks = {});
  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  Client(Client&&) = delete;
  Client& operator=(Client&&) = delete;

  // Registers exactly once, starts the paired Runtime Link, and returns only
  // after the first full instance spec and its enabled capabilities are ready.
  // Pair failures reconnect within that session; Start cannot register again.
  bool Start(std::string* error = nullptr);

  // Idempotently cancels Register/Control/Work, joins all workers, and returns
  // after capability and lifecycle callbacks can no longer be invoked.
  void Stop();

  // Publishes only to a Host-opened source that the source_open callback has
  // accepted. Both message and byte credit are consumed atomically.
  SourceWriteResult PublishSource(const std::string& stream_id,
                                  std::vector<std::string> items);
  bool CloseSource(const std::string& stream_id,
                   const xgc::adapter::v1::AdapterError* error = nullptr);

  SessionSnapshot session() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

const char* ToString(ClientState state) noexcept;
const char* ToString(SourceWriteResult result) noexcept;

}  // namespace adapter_runtime
}  // namespace xgc2
