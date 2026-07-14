#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "xgc/adapter/v1/adapter.pb.h"
#include "xgc/v1/message.pb.h"

namespace xgc2 {
namespace adapter_link {

enum class LogLevel {
  kDebug,
  kInfo,
  kWarning,
  kError,
};

enum class ClientState {
  kStopped,
  kConnecting,
  kReady,
  kReconnecting,
  kStopping,
};

struct OperationExecutionResult {
  xgc::adapter::v1::OperationPhase phase = xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED;
  xgc::adapter::v1::ResultCode code = xgc::adapter::v1::RESULT_CODE_OK;
  std::string detail;
  std::int32_t native_code = 0;
  bool has_response = false;
  xgc::v1::Message response;

  static OperationExecutionResult Succeeded(std::string detail = {});
  static OperationExecutionResult Failed(
      std::string detail,
      xgc::adapter::v1::ResultCode code = xgc::adapter::v1::RESULT_CODE_INTERNAL,
      std::int32_t native_code = 0);
  static OperationExecutionResult Rejected(
      std::string detail,
      xgc::adapter::v1::ResultCode code = xgc::adapter::v1::RESULT_CODE_REJECTED);
};

struct ClientConfig {
  std::string adapter_id;
  std::string socket_path = "/run/xgc2/adapter/adapter-link.sock";
  std::string bootstrap_token_file;
  std::string native_protocol;
  std::string software_version;
  std::vector<std::uint32_t> supported_protocol_versions;
  std::uint64_t registry_fingerprint = 0;
  std::vector<xgc::adapter::v1::ProfileAdvertisement> supported_profiles;

  std::uint32_t rpc_timeout_ms = 3000;
  std::uint32_t initial_connect_timeout_ms = 10000;
  std::uint32_t reconnect_initial_delay_ms = 250;
  std::uint32_t reconnect_max_delay_ms = 5000;
  std::size_t maximum_telemetry_queue = 4096;
  std::size_t maximum_telemetry_messages_per_batch = 64;
  std::size_t maximum_operation_cache = 1024;
};

struct ClientCallbacks {
  // Must replace the complete native runtime atomically. Returning false keeps
  // the previous session from becoming ready.
  std::function<bool(const xgc::adapter::v1::AdapterPlan&, std::string*)>
      validate_and_apply_plan;

  // Called when a ready session is invalidated and once during Stop(). It must
  // be idempotent and must not call Client::Stop().
  std::function<void()> clear_plan;

  // Called serially by the operation worker. The library reports accepted and
  // started phases before invoking it, then reports the returned terminal
  // result. Throwing is converted into RESULT_CODE_INTERNAL.
  std::function<OperationExecutionResult(const xgc::adapter::v1::OperationRequest&)>
      handle_operation;

  // Logging is optional. Exceptions from this callback are ignored.
  std::function<void(LogLevel, const std::string&)> log;
};

struct SessionSnapshot {
  ClientState state = ClientState::kStopped;
  std::string core_id;
  std::string session_id;
  std::string asset_digest;
  std::uint64_t plan_revision = 0;
  std::uint32_t heartbeat_interval_ms = 0;
  std::uint32_t maximum_batch_bytes = 0;
  std::uint64_t dropped_telemetry_messages = 0;
};

class Client {
 public:
  Client(ClientConfig config, ClientCallbacks callbacks);
  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  Client(Client&&) = delete;
  Client& operator=(Client&&) = delete;

  // Synchronously establishes and activates the first session. On failure the
  // object remains stopped and may be started again.
  bool Start(std::string* error = nullptr);

  // Idempotently cancels RPCs, joins workers, clears the applied plan, and
  // returns only after callbacks can no longer be invoked.
  void Stop();

  // Queues telemetry only if plan_revision is the currently applied revision.
  // The bounded queue drops its oldest element under sustained backpressure.
  bool Publish(std::uint64_t plan_revision, xgc::v1::Message message);

  SessionSnapshot session() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

std::string ReadBootstrapTokenFile(const std::string& path);
const char* ToString(ClientState state) noexcept;

}  // namespace adapter_link
}  // namespace xgc2
