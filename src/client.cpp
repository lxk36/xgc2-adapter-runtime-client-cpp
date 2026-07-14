#include "xgc2/adapter_link/client.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "xgc/adapter/v1/adapter.grpc.pb.h"

namespace xgc2 {
namespace adapter_link {
namespace {

constexpr std::uint32_t kDefaultHeartbeatIntervalMs = 1000;
constexpr std::uint32_t kDefaultMaximumBatchBytes = 256 * 1024;

std::chrono::system_clock::time_point DeadlineAfter(std::uint32_t milliseconds) {
  return std::chrono::system_clock::now() + std::chrono::milliseconds(milliseconds);
}

std::int64_t UnixNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string TrimWhitespace(std::string value) {
  const auto is_space = [](unsigned char character) {
    return std::isspace(character) != 0;
  };
  value.erase(value.begin(), std::find_if_not(value.begin(), value.end(), is_space));
  value.erase(std::find_if_not(value.rbegin(), value.rend(), is_space).base(),
              value.end());
  return value;
}

bool IsTerminalPhase(xgc::adapter::v1::OperationPhase phase) {
  return phase == xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_REJECTED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_FAILED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_EXPIRED;
}

xgc::adapter::v1::OperationEvent MakeOperationEvent(
    const std::string& operation_id, xgc::adapter::v1::OperationPhase phase,
    xgc::adapter::v1::ResultCode code, const std::string& detail,
    std::int32_t native_code = 0) {
  xgc::adapter::v1::OperationEvent event;
  event.set_operation_id(operation_id);
  event.set_phase(phase);
  event.set_code(code);
  event.set_occurred_unix_nanos(UnixNanos());
  event.set_detail(detail);
  event.set_native_code(native_code);
  return event;
}

std::int64_t OperationDeadline(const xgc::adapter::v1::OperationRequest& request) {
  std::int64_t deadline = request.deadline_unix_nanos();
  if (request.ttl_ms() > 0 && request.issued_unix_nanos() > 0) {
    const std::int64_t ttl_nanos =
        static_cast<std::int64_t>(request.ttl_ms()) * 1000000LL;
    const std::int64_t issued = request.issued_unix_nanos();
    const std::int64_t ttl_deadline =
        issued > std::numeric_limits<std::int64_t>::max() - ttl_nanos
            ? std::numeric_limits<std::int64_t>::max()
            : issued + ttl_nanos;
    if (deadline <= 0 || ttl_deadline < deadline) {
      deadline = ttl_deadline;
    }
  }
  return deadline;
}

}  // namespace

OperationExecutionResult OperationExecutionResult::Succeeded(std::string detail) {
  OperationExecutionResult result;
  result.detail = std::move(detail);
  return result;
}

OperationExecutionResult OperationExecutionResult::Failed(
    std::string detail, xgc::adapter::v1::ResultCode code, std::int32_t native_code) {
  OperationExecutionResult result;
  result.phase = xgc::adapter::v1::OPERATION_PHASE_FAILED;
  result.code = code;
  result.detail = std::move(detail);
  result.native_code = native_code;
  return result;
}

OperationExecutionResult OperationExecutionResult::Rejected(
    std::string detail, xgc::adapter::v1::ResultCode code) {
  OperationExecutionResult result;
  result.phase = xgc::adapter::v1::OPERATION_PHASE_REJECTED;
  result.code = code;
  result.detail = std::move(detail);
  return result;
}

std::string ReadBootstrapTokenFile(const std::string& path) {
  if (path.empty() || path.front() != '/') {
    throw std::invalid_argument("bootstrap_token_file must be an absolute path");
  }
  std::ifstream input(path, std::ios::in | std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open bootstrap_token_file");
  }
  std::string token((std::istreambuf_iterator<char>(input)),
                    std::istreambuf_iterator<char>());
  if (input.bad()) {
    throw std::runtime_error("failed to read bootstrap_token_file");
  }
  token = TrimWhitespace(std::move(token));
  if (token.empty()) {
    throw std::invalid_argument(
        "bootstrap_token_file is empty after trimming whitespace");
  }
  return token;
}

const char* ToString(ClientState state) noexcept {
  switch (state) {
    case ClientState::kStopped:
      return "stopped";
    case ClientState::kConnecting:
      return "connecting";
    case ClientState::kReady:
      return "ready";
    case ClientState::kReconnecting:
      return "reconnecting";
    case ClientState::kStopping:
      return "stopping";
  }
  return "unknown";
}

class Client::Impl {
 public:
  Impl(ClientConfig config, ClientCallbacks callbacks)
      : config_(std::move(config)), callbacks_(std::move(callbacks)) {}

  ~Impl() { Stop(); }

  bool Start(std::string* error) {
    std::string validation_error;
    if (!ValidateConfig(&validation_error)) {
      SetError(error, validation_error);
      return false;
    }

    try {
      bootstrap_token_ = ReadBootstrapTokenFile(config_.bootstrap_token_file);
    } catch (const std::exception& exception) {
      SetError(error, exception.what());
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ != ClientState::kStopped || lifecycle_thread_.joinable() ||
          telemetry_thread_.joinable() || operation_thread_.joinable()) {
        SetError(error, "AdapterLink client is already started");
        return false;
      }
      stop_requested_ = false;
      state_ = ClientState::kConnecting;
    }

    channel_ = grpc::CreateChannel("unix:" + config_.socket_path,
                                   grpc::InsecureChannelCredentials());
    stub_ = xgc::adapter::v1::AdapterLink::NewStub(channel_);

    const auto started_at = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::milliseconds(config_.initial_connect_timeout_ms);
    std::uint32_t delay_ms = config_.reconnect_initial_delay_ms;
    std::string connect_error;
    while (!StopRequested()) {
      if (RegisterAndActivate(&connect_error)) {
        lifecycle_thread_ = std::thread(&Impl::LifecycleLoop, this);
        telemetry_thread_ = std::thread(&Impl::TelemetryLoop, this);
        operation_thread_ = std::thread(&Impl::OperationLoop, this);
        Log(LogLevel::kInfo, "AdapterLink initial session is ready");
        return true;
      }
      if (config_.initial_connect_timeout_ms == 0 ||
          std::chrono::steady_clock::now() - started_at >= timeout) {
        break;
      }
      Log(LogLevel::kWarning,
          "AdapterLink initial connection failed: " + connect_error);
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait_for(lock, std::chrono::milliseconds(delay_ms),
                          [this] { return stop_requested_; });
      delay_ms = NextBackoff(delay_ms);
    }

    ClearPlanAndReset(ClientState::kStopped);
    channel_.reset();
    stub_.reset();
    SetError(error,
             connect_error.empty() ? "AdapterLink start was canceled" : connect_error);
    return false;
  }

  void Stop() {
    std::shared_ptr<grpc::ClientContext> operation_context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == ClientState::kStopped && !lifecycle_thread_.joinable() &&
          !telemetry_thread_.joinable() && !operation_thread_.joinable()) {
        return;
      }
      stop_requested_ = true;
      state_ = ClientState::kStopping;
      operation_context = active_operation_context_;
    }
    if (operation_context) {
      operation_context->TryCancel();
    }
    condition_.notify_all();

    if (lifecycle_thread_.joinable()) {
      lifecycle_thread_.join();
    }
    if (telemetry_thread_.joinable()) {
      telemetry_thread_.join();
    }
    if (operation_thread_.joinable()) {
      operation_thread_.join();
    }

    ClearPlanAndReset(ClientState::kStopped);
    stub_.reset();
    channel_.reset();
    bootstrap_token_.clear();
  }

  bool Publish(std::uint64_t plan_revision, xgc::v1::Message message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ != ClientState::kReady || session_id_.empty() ||
        plan_revision != plan_revision_) {
      return false;
    }
    if (telemetry_queue_.size() >= config_.maximum_telemetry_queue) {
      telemetry_queue_.pop_front();
      ++dropped_telemetry_messages_;
    }
    telemetry_queue_.push_back(QueuedTelemetry{plan_revision, std::move(message)});
    condition_.notify_all();
    return true;
  }

  SessionSnapshot session() const {
    std::lock_guard<std::mutex> lock(mutex_);
    SessionSnapshot snapshot;
    snapshot.state = state_;
    snapshot.core_id = core_id_;
    snapshot.session_id = session_id_;
    snapshot.asset_digest = asset_digest_;
    snapshot.plan_revision = plan_revision_;
    snapshot.heartbeat_interval_ms = heartbeat_interval_ms_;
    snapshot.maximum_batch_bytes = maximum_batch_bytes_;
    snapshot.dropped_telemetry_messages = dropped_telemetry_messages_;
    return snapshot;
  }

 private:
  struct QueuedTelemetry {
    std::uint64_t plan_revision;
    xgc::v1::Message message;
  };

  struct CachedOperation {
    std::string request_bytes;
    xgc::adapter::v1::OperationEvent terminal_event;
  };

  bool ValidateConfig(std::string* error) const {
    if (config_.adapter_id.empty()) {
      return Fail(error, "adapter_id must not be empty");
    }
    if (config_.socket_path.empty() || config_.socket_path.front() != '/') {
      return Fail(error, "socket_path must be an absolute filesystem path");
    }
    if (config_.native_protocol.empty()) {
      return Fail(error, "native_protocol must not be empty");
    }
    if (config_.software_version.empty()) {
      return Fail(error, "software_version must not be empty");
    }
    if (config_.supported_protocol_versions.empty()) {
      return Fail(error, "supported_protocol_versions must not be empty");
    }
    if (config_.registry_fingerprint == 0) {
      return Fail(error, "registry_fingerprint must not be zero");
    }
    if (!callbacks_.validate_and_apply_plan) {
      return Fail(error, "validate_and_apply_plan callback is required");
    }
    if (config_.rpc_timeout_ms == 0 || config_.reconnect_initial_delay_ms == 0 ||
        config_.reconnect_max_delay_ms < config_.reconnect_initial_delay_ms ||
        config_.maximum_telemetry_queue == 0 ||
        config_.maximum_telemetry_messages_per_batch == 0 ||
        config_.maximum_operation_cache == 0) {
      return Fail(error, "client bounds and timeouts must be positive and ordered");
    }

    std::set<std::uint32_t> versions;
    for (const auto version : config_.supported_protocol_versions) {
      if (version == 0 || !versions.insert(version).second) {
        return Fail(error, "supported_protocol_versions contains zero or a duplicate");
      }
    }
    std::set<std::string> profile_ids;
    for (const auto& profile : config_.supported_profiles) {
      if (profile.profile_id().empty() || profile.profile_digest().empty()) {
        return Fail(error, "supported profile id and digest must not be empty");
      }
      if (!profile_ids.insert(profile.profile_id()).second) {
        return Fail(error, "supported profile is advertised more than once: " +
                               profile.profile_id());
      }
      std::set<std::string> channel_ids;
      for (const auto& channel : profile.channels()) {
        if (channel.channel_id().empty() ||
            channel.kind() == xgc::adapter::v1::CHANNEL_KIND_UNSPECIFIED ||
            !channel_ids.insert(channel.channel_id()).second) {
          return Fail(error, "supported profile contains an invalid channel: " +
                                 profile.profile_id());
        }
      }
    }
    return true;
  }

  bool RegisterAndActivate(std::string* error) {
    xgc::adapter::v1::RegisterAdapterRequest request;
    request.set_adapter_id(config_.adapter_id);
    request.set_native_protocol(config_.native_protocol);
    request.set_software_version(config_.software_version);
    request.set_registry_fingerprint(config_.registry_fingerprint);
    request.set_bootstrap_token(bootstrap_token_);
    for (const auto version : config_.supported_protocol_versions) {
      request.add_supported_protocol_versions(version);
    }
    for (const auto& profile : config_.supported_profiles) {
      *request.add_supported_profiles() = profile;
    }

    xgc::adapter::v1::RegisterAdapterResponse response;
    grpc::ClientContext context;
    context.set_deadline(DeadlineAfter(config_.rpc_timeout_ms));
    const grpc::Status status = stub_->RegisterAdapter(&context, request, &response);
    if (!status.ok()) {
      return Fail(error, "registration transport failed: " + status.error_message());
    }
    if (!response.accepted()) {
      return Fail(error, "registration rejected: " + response.message());
    }
    if (response.core_id().empty() || response.session_id().empty()) {
      return Fail(error, "registration omitted Core or session identity");
    }
    if (response.registry_fingerprint() != config_.registry_fingerprint) {
      return Fail(error, "registration registry fingerprint mismatch");
    }
    if (std::find(config_.supported_protocol_versions.begin(),
                  config_.supported_protocol_versions.end(),
                  response.selected_protocol_version()) ==
        config_.supported_protocol_versions.end()) {
      return Fail(error, "Core selected an unsupported protocol version");
    }

    xgc::adapter::v1::AdapterPlan plan;
    if (!GetPlan(response.session_id(), 0, response.plan_revision(), &plan, error)) {
      return false;
    }
    if (!ValidateAndApplyPlan(plan, error)) {
      return false;
    }

    const std::uint32_t heartbeat_interval =
        response.heartbeat_interval_ms() == 0
            ? kDefaultHeartbeatIntervalMs
            : std::max<std::uint32_t>(250, response.heartbeat_interval_ms());
    if (!AcknowledgePlan(response.session_id(), plan.revision(), heartbeat_interval,
                         error)) {
      SafeClearPlan();
      return false;
    }

    bool stopped = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped = stop_requested_;
      if (!stopped) {
        core_id_ = response.core_id();
        session_id_ = response.session_id();
        asset_digest_ = plan.asset_digest();
        plan_revision_ = plan.revision();
        active_plan_ = plan;
        heartbeat_interval_ms_ = heartbeat_interval;
        maximum_batch_bytes_ = response.max_batch_bytes() == 0
                                   ? kDefaultMaximumBatchBytes
                                   : response.max_batch_bytes();
        state_ = ClientState::kReady;
      }
    }
    if (stopped) {
      SafeClearPlan();
      return Fail(error, "AdapterLink start was canceled");
    }
    condition_.notify_all();
    return true;
  }

  bool GetPlan(const std::string& session_id, std::uint64_t known_revision,
               std::uint64_t expected_revision, xgc::adapter::v1::AdapterPlan* plan,
               std::string* error) {
    xgc::adapter::v1::GetAdapterPlanRequest request;
    request.set_adapter_id(config_.adapter_id);
    request.set_session_id(session_id);
    request.set_known_revision(known_revision);
    grpc::ClientContext context;
    context.set_deadline(DeadlineAfter(config_.rpc_timeout_ms));
    const grpc::Status status = stub_->GetAdapterPlan(&context, request, plan);
    if (!status.ok()) {
      return Fail(error, "plan request transport failed: " + status.error_message());
    }
    if (!plan->accepted()) {
      return Fail(error, "plan rejected: " + plan->message());
    }
    if (plan->revision() != expected_revision) {
      std::ostringstream detail;
      detail << "plan revision mismatch: expected " << expected_revision
             << ", received " << plan->revision();
      return Fail(error, detail.str());
    }
    return true;
  }

  bool ValidateAndApplyPlan(const xgc::adapter::v1::AdapterPlan& plan,
                            std::string* error) {
    if (!ValidatePlan(plan, error)) {
      return false;
    }
    std::lock_guard<std::mutex> transition_lock(plan_transition_mutex_);
    return ApplyPlanCallback(plan, error);
  }

  bool ApplyPlanCallback(const xgc::adapter::v1::AdapterPlan& plan,
                         std::string* error) {
    try {
      if (!callbacks_.validate_and_apply_plan(plan, error)) {
        if (error != nullptr && error->empty()) {
          *error = "application rejected AdapterPlan";
        }
        return false;
      }
      return true;
    } catch (const std::exception& exception) {
      return Fail(error,
                  "validate_and_apply_plan threw: " + std::string(exception.what()));
    } catch (...) {
      return Fail(error, "validate_and_apply_plan threw an unknown exception");
    }
  }

  bool ValidatePlan(const xgc::adapter::v1::AdapterPlan& plan,
                    std::string* error) const {
    std::map<std::string, const xgc::adapter::v1::ProfileAdvertisement*> profiles;
    for (const auto& profile : config_.supported_profiles) {
      profiles.emplace(profile.profile_id(), &profile);
    }
    std::set<std::string> robot_ids;
    for (const auto& robot : plan.robots()) {
      if (robot.robot_id().empty() || !robot_ids.insert(robot.robot_id()).second) {
        return Fail(error, "plan contains an empty or duplicate robot_id");
      }
      const auto profile_it = profiles.find(robot.profile_id());
      if (profile_it == profiles.end()) {
        return Fail(error, "plan requires unsupported profile " + robot.profile_id());
      }
      const auto* profile = profile_it->second;
      if (robot.profile_digest() != profile->profile_digest()) {
        return Fail(error,
                    "plan profile digest mismatch for robot " + robot.robot_id());
      }
      std::map<std::string, const xgc::adapter::v1::ChannelAdvertisement*>
          advertised_channels;
      for (const auto& channel : profile->channels()) {
        advertised_channels.emplace(channel.channel_id(), &channel);
      }
      std::set<std::string> channel_ids;
      for (const auto& channel : robot.channels()) {
        if (channel.channel_id().empty() ||
            !channel_ids.insert(channel.channel_id()).second ||
            advertised_channels.find(channel.channel_id()) ==
                advertised_channels.end()) {
          return Fail(error, "plan contains an unknown or duplicate channel for " +
                                 robot.robot_id());
        }
      }
    }
    if (plan.robots_size() > 0 && plan.asset_digest().empty()) {
      return Fail(error, "non-empty plan omitted asset_digest");
    }
    return true;
  }

  bool AcknowledgePlan(const std::string& session_id, std::uint64_t plan_revision,
                       std::uint32_t heartbeat_interval_ms, std::string* error) {
    xgc::adapter::v1::HeartbeatRequest request;
    request.set_adapter_id(config_.adapter_id);
    request.set_session_id(session_id);
    request.set_observed_unix_nanos(UnixNanos());
    request.set_applied_plan_revision(plan_revision);
    xgc::adapter::v1::HeartbeatResponse response;
    grpc::ClientContext context;
    context.set_deadline(
        DeadlineAfter(std::max(config_.rpc_timeout_ms, heartbeat_interval_ms)));
    const grpc::Status status = stub_->Heartbeat(&context, request, &response);
    if (!status.ok()) {
      return Fail(error,
                  "applied-plan heartbeat transport failed: " + status.error_message());
    }
    if (!response.accepted()) {
      return Fail(error, "Core rejected applied-plan heartbeat");
    }
    if (response.current_plan_revision() != plan_revision || response.reload_plan()) {
      return Fail(error, "Core changed plan during activation");
    }
    return true;
  }

  void LifecycleLoop() {
    std::uint32_t reconnect_delay = config_.reconnect_initial_delay_ms;
    while (!StopRequested()) {
      SessionSnapshot snapshot = session();
      if (snapshot.session_id.empty() || snapshot.state != ClientState::kReady) {
        SetState(ClientState::kReconnecting);
        std::string error;
        if (RegisterAndActivate(&error)) {
          reconnect_delay = config_.reconnect_initial_delay_ms;
          Log(LogLevel::kInfo, "AdapterLink session re-established");
          continue;
        }
        Log(LogLevel::kWarning, "AdapterLink reconnect failed: " + error);
        WaitFor(reconnect_delay);
        reconnect_delay = NextBackoff(reconnect_delay);
        continue;
      }

      if (WaitFor(snapshot.heartbeat_interval_ms, snapshot.session_id)) {
        continue;
      }
      if (StopRequested()) {
        return;
      }
      SendHeartbeat(snapshot);
    }
  }

  void SendHeartbeat(const SessionSnapshot& snapshot) {
    xgc::adapter::v1::HeartbeatRequest request;
    request.set_adapter_id(config_.adapter_id);
    request.set_session_id(snapshot.session_id);
    request.set_observed_unix_nanos(UnixNanos());
    request.set_applied_plan_revision(snapshot.plan_revision);
    xgc::adapter::v1::HeartbeatResponse response;
    grpc::ClientContext context;
    context.set_deadline(DeadlineAfter(config_.rpc_timeout_ms));
    const grpc::Status status = stub_->Heartbeat(&context, request, &response);
    if (!status.ok() || !response.accepted()) {
      InvalidateSession(snapshot.session_id,
                        status.ok()
                            ? "heartbeat was rejected"
                            : "heartbeat transport failed: " + status.error_message());
      return;
    }
    if (response.reload_plan() ||
        response.current_plan_revision() != snapshot.plan_revision) {
      ReloadPlan(snapshot, response.current_plan_revision());
    }
  }

  void ReloadPlan(const SessionSnapshot& snapshot, std::uint64_t expected_revision) {
    xgc::adapter::v1::AdapterPlan plan;
    std::string error;
    if (!GetPlan(snapshot.session_id, snapshot.plan_revision, expected_revision, &plan,
                 &error)) {
      InvalidateSession(snapshot.session_id, "plan reload failed: " + error);
      return;
    }

    std::shared_ptr<grpc::ClientContext> operation_context;
    bool transition_failed = false;
    {
      std::unique_lock<std::mutex> transition_lock(plan_transition_mutex_);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_requested_ || session_id_ != snapshot.session_id ||
            plan_revision_ != snapshot.plan_revision) {
          return;
        }
      }
      if (!ValidatePlan(plan, &error) || !ApplyPlanCallback(plan, &error) ||
          !AcknowledgePlan(snapshot.session_id, plan.revision(),
                           snapshot.heartbeat_interval_ms, &error)) {
        transition_failed = true;
      } else {
        std::lock_guard<std::mutex> lock(mutex_);
        if (session_id_ != snapshot.session_id || stop_requested_) {
          return;
        }
        plan_revision_ = plan.revision();
        asset_digest_ = plan.asset_digest();
        active_plan_ = plan;
        telemetry_queue_.clear();
        operation_context = active_operation_context_;
      }
    }
    if (transition_failed) {
      InvalidateSession(snapshot.session_id, "plan reload failed: " + error);
      return;
    }
    if (operation_context) {
      operation_context->TryCancel();
    }
    condition_.notify_all();
    Log(LogLevel::kInfo, "AdapterLink plan reloaded");
  }

  void TelemetryLoop() {
    while (!StopRequested()) {
      xgc::adapter::v1::TelemetryBatch batch;
      std::string session_id;
      std::uint64_t revision = 0;
      std::uint32_t maximum_batch_bytes = 0;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] {
          return stop_requested_ || (state_ == ClientState::kReady &&
                                     !session_id_.empty() && !telemetry_queue_.empty());
        });
        if (stop_requested_) {
          return;
        }
        session_id = session_id_;
        revision = plan_revision_;
        maximum_batch_bytes = maximum_batch_bytes_;
        batch.set_adapter_id(config_.adapter_id);
        batch.set_session_id(session_id);
        batch.set_plan_revision(revision);
        batch.set_batch_id(++telemetry_batch_id_);

        std::size_t estimated_bytes = 256;
        while (!telemetry_queue_.empty() &&
               static_cast<std::size_t>(batch.messages_size()) <
                   config_.maximum_telemetry_messages_per_batch) {
          auto& queued = telemetry_queue_.front();
          if (queued.plan_revision != revision) {
            telemetry_queue_.pop_front();
            continue;
          }
          const std::size_t message_bytes =
              static_cast<std::size_t>(queued.message.ByteSize()) + 16;
          if (estimated_bytes + message_bytes > maximum_batch_bytes) {
            if (batch.messages_size() == 0) {
              telemetry_queue_.pop_front();
              ++dropped_telemetry_messages_;
              continue;
            }
            break;
          }
          estimated_bytes += message_bytes;
          batch.add_messages()->Swap(&queued.message);
          telemetry_queue_.pop_front();
        }
      }
      if (batch.messages_size() == 0) {
        continue;
      }

      xgc::adapter::v1::BatchAck acknowledgement;
      grpc::ClientContext context;
      context.set_deadline(DeadlineAfter(config_.rpc_timeout_ms));
      const grpc::Status status =
          stub_->PushTelemetry(&context, batch, &acknowledgement);
      if (!status.ok() || !acknowledgement.accepted() ||
          acknowledgement.batch_id() != batch.batch_id() ||
          acknowledgement.accepted_count() !=
              static_cast<std::uint32_t>(batch.messages_size()) ||
          acknowledgement.rejected_count() != 0) {
        InvalidateSession(session_id, status.ok() ? "telemetry batch was rejected"
                                                  : "telemetry transport failed: " +
                                                        status.error_message());
      }
    }
  }

  void OperationLoop() {
    while (!StopRequested()) {
      SessionSnapshot snapshot;
      std::vector<std::string> robot_ids;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] {
          return stop_requested_ ||
                 (state_ == ClientState::kReady && !session_id_.empty() &&
                  PlanHasOperationChannel(active_plan_));
        });
        if (stop_requested_) {
          return;
        }
        snapshot = SnapshotLocked();
        for (const auto& robot : active_plan_.robots()) {
          robot_ids.push_back(robot.robot_id());
        }
      }

      xgc::adapter::v1::OperationStreamRequest request;
      request.set_adapter_id(config_.adapter_id);
      request.set_session_id(snapshot.session_id);
      request.set_applied_plan_revision(snapshot.plan_revision);
      for (const auto& robot_id : robot_ids) {
        request.add_robot_ids(robot_id);
      }

      auto context = std::make_shared<grpc::ClientContext>();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_requested_ || session_id_ != snapshot.session_id ||
            plan_revision_ != snapshot.plan_revision) {
          continue;
        }
        active_operation_context_ = context;
      }

      auto reader = stub_->StreamOperations(context.get(), request);
      xgc::adapter::v1::OperationRequest operation;
      bool reporting_failed = false;
      while (!StopRequested() && reader->Read(&operation)) {
        if (!HandleOperation(snapshot, operation)) {
          reporting_failed = true;
          context->TryCancel();
          break;
        }
      }
      const grpc::Status status = reader->Finish();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (active_operation_context_ == context) {
          active_operation_context_.reset();
        }
      }
      if (StopRequested()) {
        return;
      }
      const SessionSnapshot latest = session();
      if (latest.session_id != snapshot.session_id ||
          latest.plan_revision != snapshot.plan_revision) {
        continue;
      }
      if (reporting_failed ||
          (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED)) {
        InvalidateSession(snapshot.session_id,
                          reporting_failed
                              ? "operation event reporting failed"
                              : "operation stream failed: " + status.error_message());
      } else {
        WaitFor(config_.reconnect_initial_delay_ms, snapshot.session_id);
      }
    }
  }

  bool HandleOperation(const SessionSnapshot& stream,
                       const xgc::adapter::v1::OperationRequest& request) {
    if (request.operation_id().empty()) {
      return ReportOperationEvent(
          stream, MakeOperationEvent("", xgc::adapter::v1::OPERATION_PHASE_REJECTED,
                                     xgc::adapter::v1::RESULT_CODE_INVALID_ARGUMENT,
                                     "operation_id must not be empty"));
    }

    const std::string bytes = request.SerializeAsString();
    const auto cached = operation_cache_.find(request.operation_id());
    if (cached != operation_cache_.end()) {
      if (cached->second.request_bytes != bytes) {
        return ReportOperationEvent(
            stream,
            MakeOperationEvent(request.operation_id(),
                               xgc::adapter::v1::OPERATION_PHASE_REJECTED,
                               xgc::adapter::v1::RESULT_CODE_INVALID_ARGUMENT,
                               "operation_id was reused with a different request"));
      }
      return ReportOperationEvent(stream, cached->second.terminal_event);
    }

    std::unique_lock<std::mutex> transition_lock(plan_transition_mutex_);
    xgc::adapter::v1::OperationEvent validation_error;
    if (!ValidateOperation(stream, request, &validation_error)) {
      return ReportOperationEvent(stream, validation_error);
    }
    if (!ReportOperationEvent(
            stream, MakeOperationEvent(request.operation_id(),
                                       xgc::adapter::v1::OPERATION_PHASE_ACCEPTED,
                                       xgc::adapter::v1::RESULT_CODE_OK,
                                       "operation accepted by adapter")) ||
        !ReportOperationEvent(
            stream, MakeOperationEvent(request.operation_id(),
                                       xgc::adapter::v1::OPERATION_PHASE_STARTED,
                                       xgc::adapter::v1::RESULT_CODE_OK,
                                       "native operation started"))) {
      return false;
    }

    OperationExecutionResult result;
    try {
      if (callbacks_.handle_operation) {
        result = callbacks_.handle_operation(request);
      } else {
        result = OperationExecutionResult::Rejected(
            "adapter has no operation handler",
            xgc::adapter::v1::RESULT_CODE_UNSUPPORTED);
      }
    } catch (const std::exception& exception) {
      result = OperationExecutionResult::Failed("operation handler threw: " +
                                                std::string(exception.what()));
    } catch (...) {
      result = OperationExecutionResult::Failed(
          "operation handler threw an unknown exception");
    }
    if (!IsTerminalPhase(result.phase)) {
      result = OperationExecutionResult::Failed(
          "operation handler returned a non-terminal phase");
    }

    auto terminal = MakeOperationEvent(request.operation_id(), result.phase,
                                       result.code, result.detail, result.native_code);
    if (result.has_response) {
      *terminal.mutable_response() = std::move(result.response);
    }
    RememberOperation(request.operation_id(), bytes, terminal);
    return ReportOperationEvent(stream, terminal);
  }

  bool ValidateOperation(const SessionSnapshot& stream,
                         const xgc::adapter::v1::OperationRequest& request,
                         xgc::adapter::v1::OperationEvent* error) {
    const auto reject = [&](xgc::adapter::v1::ResultCode code,
                            const std::string& detail,
                            xgc::adapter::v1::OperationPhase phase =
                                xgc::adapter::v1::OPERATION_PHASE_REJECTED) {
      *error = MakeOperationEvent(request.operation_id(), phase, code, detail);
      return false;
    };

    xgc::adapter::v1::AdapterPlan plan;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (session_id_ != stream.session_id || plan_revision_ != stream.plan_revision ||
          request.plan_revision() != stream.plan_revision) {
        return reject(xgc::adapter::v1::RESULT_CODE_REJECTED,
                      "operation belongs to a stale adapter plan");
      }
      plan = active_plan_;
    }
    if (request.message().robot_id().empty() ||
        request.message().channel_id().empty()) {
      return reject(xgc::adapter::v1::RESULT_CODE_INVALID_ARGUMENT,
                    "operation message must identify robot and channel");
    }

    const xgc::adapter::v1::RobotPlan* robot = nullptr;
    for (const auto& candidate : plan.robots()) {
      if (candidate.robot_id() == request.message().robot_id()) {
        robot = &candidate;
        break;
      }
    }
    if (robot == nullptr) {
      return reject(xgc::adapter::v1::RESULT_CODE_NOT_FOUND,
                    "operation robot is absent from the applied plan");
    }

    bool channel_enabled = false;
    for (const auto& channel : robot->channels()) {
      if (channel.channel_id() == request.message().channel_id()) {
        channel_enabled = channel.enabled();
        break;
      }
    }
    if (!channel_enabled) {
      return reject(xgc::adapter::v1::RESULT_CODE_UNSUPPORTED,
                    "operation channel is disabled in the applied plan");
    }

    const xgc::adapter::v1::ChannelAdvertisement* advertisement = nullptr;
    for (const auto& profile : config_.supported_profiles) {
      if (profile.profile_id() != robot->profile_id()) {
        continue;
      }
      for (const auto& channel : profile.channels()) {
        if (channel.channel_id() == request.message().channel_id()) {
          advertisement = &channel;
          break;
        }
      }
    }
    if (advertisement == nullptr ||
        advertisement->kind() != xgc::adapter::v1::CHANNEL_KIND_OPERATION ||
        advertisement->input_message_id() != request.message().message_id()) {
      return reject(xgc::adapter::v1::RESULT_CODE_UNSUPPORTED,
                    "message does not match an advertised operation channel");
    }
    if (request.message().payload().size() > 64 * 1024) {
      return reject(xgc::adapter::v1::RESULT_CODE_INVALID_ARGUMENT,
                    "operation payload exceeds 64 KiB");
    }
    if (request.ttl_ms() > 0 && request.issued_unix_nanos() <= 0) {
      return reject(xgc::adapter::v1::RESULT_CODE_INVALID_ARGUMENT,
                    "ttl_ms requires issued_unix_nanos");
    }
    const auto deadline = OperationDeadline(request);
    if (deadline > 0 && UnixNanos() >= deadline) {
      return reject(xgc::adapter::v1::RESULT_CODE_TIMEOUT,
                    "operation deadline or TTL has expired",
                    xgc::adapter::v1::OPERATION_PHASE_EXPIRED);
    }
    return true;
  }

  bool ReportOperationEvent(const SessionSnapshot& stream,
                            const xgc::adapter::v1::OperationEvent& event) {
    xgc::adapter::v1::OperationEventBatch batch;
    batch.set_adapter_id(config_.adapter_id);
    batch.set_session_id(stream.session_id);
    batch.set_plan_revision(stream.plan_revision);
    batch.set_batch_id(++operation_event_batch_id_);
    *batch.add_events() = event;
    xgc::adapter::v1::BatchAck acknowledgement;
    grpc::ClientContext context;
    context.set_deadline(DeadlineAfter(config_.rpc_timeout_ms));
    const grpc::Status status =
        stub_->ReportOperationEvents(&context, batch, &acknowledgement);
    return status.ok() && acknowledgement.accepted() &&
           acknowledgement.batch_id() == batch.batch_id() &&
           acknowledgement.accepted_count() == 1;
  }

  void RememberOperation(const std::string& operation_id,
                         const std::string& request_bytes,
                         const xgc::adapter::v1::OperationEvent& terminal_event) {
    if (operation_cache_.find(operation_id) == operation_cache_.end()) {
      operation_cache_order_.push_back(operation_id);
    }
    operation_cache_[operation_id] = CachedOperation{request_bytes, terminal_event};
    while (operation_cache_order_.size() > config_.maximum_operation_cache) {
      operation_cache_.erase(operation_cache_order_.front());
      operation_cache_order_.pop_front();
    }
  }

  void InvalidateSession(const std::string& expected_session,
                         const std::string& reason) {
    std::shared_ptr<grpc::ClientContext> operation_context;
    bool clear = false;
    {
      std::lock_guard<std::mutex> transition_lock(plan_transition_mutex_);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (session_id_ != expected_session || stop_requested_) {
          return;
        }
        clear = !session_id_.empty();
        operation_context = active_operation_context_;
        active_operation_context_.reset();
        core_id_.clear();
        session_id_.clear();
        asset_digest_.clear();
        plan_revision_ = 0;
        active_plan_.Clear();
        telemetry_queue_.clear();
        state_ = ClientState::kReconnecting;
      }
      if (operation_context) {
        operation_context->TryCancel();
      }
      if (clear) {
        SafeClearPlanWithoutLock();
      }
    }
    condition_.notify_all();
    Log(LogLevel::kWarning, "AdapterLink session invalidated: " + reason);
  }

  void ClearPlanAndReset(ClientState state) {
    bool clear = false;
    std::shared_ptr<grpc::ClientContext> operation_context;
    {
      std::lock_guard<std::mutex> transition_lock(plan_transition_mutex_);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        clear = !session_id_.empty() || active_plan_.revision() != 0;
        operation_context = active_operation_context_;
        active_operation_context_.reset();
        core_id_.clear();
        session_id_.clear();
        asset_digest_.clear();
        plan_revision_ = 0;
        active_plan_.Clear();
        heartbeat_interval_ms_ = kDefaultHeartbeatIntervalMs;
        maximum_batch_bytes_ = kDefaultMaximumBatchBytes;
        telemetry_queue_.clear();
        state_ = state;
      }
      if (operation_context) {
        operation_context->TryCancel();
      }
      if (clear) {
        SafeClearPlanWithoutLock();
      }
    }
    condition_.notify_all();
  }

  void SafeClearPlan() {
    std::lock_guard<std::mutex> transition_lock(plan_transition_mutex_);
    SafeClearPlanWithoutLock();
  }

  void SafeClearPlanWithoutLock() {
    if (!callbacks_.clear_plan) {
      return;
    }
    try {
      callbacks_.clear_plan();
    } catch (const std::exception& exception) {
      Log(LogLevel::kError, "clear_plan threw: " + std::string(exception.what()));
    } catch (...) {
      Log(LogLevel::kError, "clear_plan threw an unknown exception");
    }
  }

  bool StopRequested() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stop_requested_;
  }

  void SetState(ClientState state) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stop_requested_) {
      state_ = state;
    }
  }

  bool WaitFor(std::uint32_t milliseconds, const std::string& expected_session = {}) {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(
        lock, std::chrono::milliseconds(milliseconds), [this, &expected_session] {
          return stop_requested_ ||
                 (!expected_session.empty() && session_id_ != expected_session);
        });
  }

  std::uint32_t NextBackoff(std::uint32_t current) const {
    if (current >= config_.reconnect_max_delay_ms) {
      return config_.reconnect_max_delay_ms;
    }
    return std::min<std::uint32_t>(config_.reconnect_max_delay_ms, current * 2);
  }

  bool PlanHasOperationChannel(const xgc::adapter::v1::AdapterPlan& plan) const {
    for (const auto& robot : plan.robots()) {
      for (const auto& channel : robot.channels()) {
        if (!channel.enabled()) {
          continue;
        }
        for (const auto& profile : config_.supported_profiles) {
          if (profile.profile_id() != robot.profile_id()) {
            continue;
          }
          for (const auto& advertised : profile.channels()) {
            if (advertised.channel_id() == channel.channel_id() &&
                advertised.kind() == xgc::adapter::v1::CHANNEL_KIND_OPERATION) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  SessionSnapshot SnapshotLocked() const {
    SessionSnapshot snapshot;
    snapshot.state = state_;
    snapshot.core_id = core_id_;
    snapshot.session_id = session_id_;
    snapshot.asset_digest = asset_digest_;
    snapshot.plan_revision = plan_revision_;
    snapshot.heartbeat_interval_ms = heartbeat_interval_ms_;
    snapshot.maximum_batch_bytes = maximum_batch_bytes_;
    snapshot.dropped_telemetry_messages = dropped_telemetry_messages_;
    return snapshot;
  }

  void Log(LogLevel level, const std::string& message) const noexcept {
    if (!callbacks_.log) {
      return;
    }
    try {
      callbacks_.log(level, message);
    } catch (...) {
    }
  }

  static bool Fail(std::string* error, const std::string& message) {
    SetError(error, message);
    return false;
  }

  static void SetError(std::string* error, const std::string& message) {
    if (error != nullptr) {
      *error = message;
    }
  }

  ClientConfig config_;
  ClientCallbacks callbacks_;
  std::string bootstrap_token_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<xgc::adapter::v1::AdapterLink::Stub> stub_;

  mutable std::mutex mutex_;
  std::mutex plan_transition_mutex_;
  std::condition_variable condition_;
  bool stop_requested_ = false;
  ClientState state_ = ClientState::kStopped;
  std::string core_id_;
  std::string session_id_;
  std::string asset_digest_;
  std::uint64_t plan_revision_ = 0;
  std::uint32_t heartbeat_interval_ms_ = kDefaultHeartbeatIntervalMs;
  std::uint32_t maximum_batch_bytes_ = kDefaultMaximumBatchBytes;
  xgc::adapter::v1::AdapterPlan active_plan_;
  std::deque<QueuedTelemetry> telemetry_queue_;
  std::uint64_t dropped_telemetry_messages_ = 0;
  std::shared_ptr<grpc::ClientContext> active_operation_context_;

  std::uint64_t telemetry_batch_id_ = 0;
  std::atomic<std::uint64_t> operation_event_batch_id_{0};
  std::map<std::string, CachedOperation> operation_cache_;
  std::deque<std::string> operation_cache_order_;

  std::thread lifecycle_thread_;
  std::thread telemetry_thread_;
  std::thread operation_thread_;
};

Client::Client(ClientConfig config, ClientCallbacks callbacks)
    : impl_(new Impl(std::move(config), std::move(callbacks))) {}

Client::~Client() = default;

bool Client::Start(std::string* error) { return impl_->Start(error); }

void Client::Stop() { impl_->Stop(); }

bool Client::Publish(std::uint64_t plan_revision, xgc::v1::Message message) {
  return impl_->Publish(plan_revision, std::move(message));
}

SessionSnapshot Client::session() const { return impl_->session(); }

}  // namespace adapter_link
}  // namespace xgc2
