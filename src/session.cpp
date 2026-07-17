#include <algorithm>
#include <chrono>
#include <utility>

#include "client_impl.hpp"
#include "xgc2/adapter_runtime/version.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::DeadlineAfter;

namespace {

void SecureErase(std::string* value) {
  if (value == nullptr) {
    return;
  }
  if (!value->empty()) {
    volatile char* bytes = &(*value)[0];
    for (std::size_t index = 0; index < value->size(); ++index) {
      bytes[index] = 0;
    }
  }
  value->clear();
  value->shrink_to_fit();
}

}  // namespace

namespace internal {

class ClientConfigAccess {
 public:
  static void ForgetBootstrapToken(ClientConfig* config) {
    SecureErase(config->registration_.mutable_bootstrap_token());
  }
};

}  // namespace internal

Client::Impl::Impl(ClientConfig config, ClientCallbacks callbacks)
    : config_(std::move(config)), callbacks_(std::move(callbacks)) {}

Client::Impl::~Impl() { Stop(); }

bool Client::Impl::IsTerminalSessionStatus(const grpc::Status& status) {
  return status.error_code() == grpc::StatusCode::FAILED_PRECONDITION ||
         status.error_code() == grpc::StatusCode::PERMISSION_DENIED ||
         status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
         status.error_code() == grpc::StatusCode::ABORTED;
}

bool Client::Impl::Start(std::string* error) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (registration_attempted_) {
      internal::SetError(
          error,
          "Adapter Runtime registration is single-use; start a new process generation");
      return false;
    }
  }
  std::string validation_error;
  if (!ValidateConfig(&validation_error)) {
    internal::SetError(error, validation_error);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ != ClientState::kStopped || supervisor_thread_.joinable() ||
        !dispatch_threads_.empty()) {
      internal::SetError(error, "Adapter Runtime client is already started");
      return false;
    }
    stop_requested_ = false;
    session_failed_ = false;
    terminal_session_failure_ = false;
    session_lost_reported_ = false;
    state_ = ClientState::kConnecting;
    last_error_.clear();
    terminal_session_error_.clear();
    replay_.clear();
    terminal_replay_bytes_ = 0;
    rejected_dispatches_ = 0;
    dropped_outbound_frames_ = 0;
  }

  channel_ =
      grpc::CreateChannel(config_.runtime_target(), grpc::InsecureChannelCredentials());
  stub_ = xgc::adapter::v1::AdapterRuntimeLinkService::NewStub(channel_);
  for (std::size_t index = 0; index < config_.dispatch_workers; ++index) {
    dispatch_threads_.emplace_back(&Impl::DispatchLoop, this);
  }
  supervisor_thread_ = std::thread(&Impl::SupervisorLoop, this);

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(config_.initial_connect_timeout_ms);
  std::unique_lock<std::mutex> lock(mutex_);
  const bool ready = condition_.wait_until(lock, deadline, [this] {
    return (state_ == ClientState::kReady && ready_callbacks_complete_ &&
            pair_ready_) ||
           state_ == ClientState::kStopped || state_ == ClientState::kSessionLost ||
           stop_requested_;
  });
  if (ready && state_ == ClientState::kReady && ready_callbacks_complete_ &&
      pair_ready_) {
    return true;
  }
  const std::string failure =
      last_error_.empty() ? "initial Runtime Link activation timed out" : last_error_;
  lock.unlock();
  Stop();
  internal::SetError(error, failure);
  return false;
}

void Client::Impl::Stop() {
  std::shared_ptr<grpc::ClientContext> registration;
  std::shared_ptr<grpc::ClientContext> control;
  std::shared_ptr<grpc::ClientContext> work;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == ClientState::kStopped && !supervisor_thread_.joinable() &&
        dispatch_threads_.empty()) {
      return;
    }
    stop_requested_ = true;
    accepting_work_ = false;
    state_ = ClientState::kStopping;
    CancelAllWorkLocked();
    registration = active_register_context_;
    control = active_control_context_;
    work = active_work_context_;
  }
  if (registration) {
    registration->TryCancel();
  }
  if (control) {
    control->TryCancel();
  }
  if (work) {
    work->TryCancel();
  }
  condition_.notify_all();
  callbacks_condition_.notify_all();

  if (supervisor_thread_.joinable()) {
    supervisor_thread_.join();
  }
  for (auto& thread : dispatch_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  dispatch_threads_.clear();

  DeactivateApplication("client stopped");
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ResetSessionLocked();
    state_ = ClientState::kStopped;
    dispatch_queue_.clear();
    while (!work_tokens_.empty()) {
      ReleaseWorkTokenLocked(work_tokens_.begin()->first);
    }
    replay_.clear();
    terminal_replay_bytes_ = 0;
  }
  stub_.reset();
  channel_.reset();
  condition_.notify_all();
}

SessionSnapshot Client::Impl::session() const {
  std::lock_guard<std::mutex> lock(mutex_);
  SessionSnapshot snapshot;
  snapshot.state = state_;
  snapshot.host_id = host_id_;
  snapshot.session_id = session_id_;
  snapshot.process_generation = config_.registration().process_generation();
  snapshot.session_generation = session_generation_;
  snapshot.runtime_epoch = runtime_epoch_;
  snapshot.spec_revision = spec_revision_;
  snapshot.spec_digest = spec_digest_;
  snapshot.runtime_link_protocol_version = runtime_link_protocol_version_;
  snapshot.heartbeat_interval_ms = heartbeat_interval_ms_;
  snapshot.in_flight_work = in_flight_work_;
  snapshot.rejected_dispatches = rejected_dispatches_;
  snapshot.dropped_outbound_frames = dropped_outbound_frames_;
  snapshot.last_error = last_error_;
  snapshot.capabilities = capability_statuses_;
  return snapshot;
}

void Client::Impl::SupervisorLoop() {
  SessionFence session_fence;
  std::string registration_error;
  if (!Register(&session_fence, &registration_error)) {
    if (!StopRequested()) {
      EnterSessionLost("registration failed: " + registration_error);
    }
    return;
  }
  InstallSession(session_fence);

  std::uint32_t reconnect_delay = config_.reconnect_initial_delay_ms;
  std::size_t reconnects_used = 0;
  while (!StopRequested()) {
    SessionFence pair_fence = session_fence;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      pair_fence.connection_epoch = connection_epoch_counter_;
      PrepareConnectionPairLocked(pair_fence);
    }
    condition_.notify_all();

    std::thread control_thread(&Impl::ControlStreamLoop, this, pair_fence);
    bool start_work = false;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this, &pair_fence] {
        return stop_requested_ || session_failed_ || terminal_session_failure_ ||
               session_id_ != pair_fence.session_id ||
               (work_attach_allowed_ &&
                work_attach_connection_epoch_ == pair_fence.connection_epoch);
      });
      start_work = !stop_requested_ && !session_failed_ && !terminal_session_failure_ &&
                   session_id_ == pair_fence.session_id && work_attach_allowed_ &&
                   work_attach_connection_epoch_ == pair_fence.connection_epoch;
    }

    std::thread work_thread;
    if (start_work) {
      work_thread = std::thread(&Impl::WorkStreamLoop, this, pair_fence);
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this, &pair_fence] {
        return stop_requested_ || session_failed_ || terminal_session_failure_ ||
               session_id_ != pair_fence.session_id;
      });
    }

    CancelActiveStreams();
    if (control_thread.joinable()) {
      control_thread.join();
    }
    if (work_thread.joinable()) {
      work_thread.join();
    }
    PreparePairReplacement();
    bool pair_was_ready = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      pair_was_ready = pair_ready_;
      ClearConnectionStateLocked();
    }
    condition_.notify_all();

    if (StopRequested()) {
      break;
    }
    if (TerminalSessionFailureRequested()) {
      std::string reason;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        reason = terminal_session_error_;
      }
      EnterSessionLost(reason);
      return;
    }
    if (pair_was_ready) {
      reconnects_used = 0;
      reconnect_delay = config_.reconnect_initial_delay_ms;
    }
    if (reconnects_used >= config_.maximum_pair_reconnect_attempts) {
      std::string pair_error;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        pair_error = last_error_;
      }
      EnterSessionLost("paired Runtime Link reconnect budget exhausted" +
                       (pair_error.empty() ? std::string() : ": " + pair_error));
      return;
    }

    ++reconnects_used;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (connection_epoch_counter_ == std::numeric_limits<std::uint64_t>::max()) {
        terminal_session_failure_ = true;
        terminal_session_error_ = "connection epoch is exhausted";
      } else {
        ++connection_epoch_counter_;
        state_ = ClientState::kReconnecting;
      }
    }
    if (TerminalSessionFailureRequested()) {
      EnterSessionLost("connection epoch is exhausted");
      return;
    }
    WaitFor(reconnect_delay);
    reconnect_delay = NextBackoff(reconnect_delay);
  }

  DeactivateApplication("Runtime Link supervisor stopped");
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ResetSessionLocked();
    state_ = ClientState::kStopped;
  }
  condition_.notify_all();
}

bool Client::Impl::Register(SessionFence* fence, std::string* error) {
  auto context = std::make_shared<grpc::ClientContext>();
  context->set_deadline(DeadlineAfter(config_.rpc_timeout_ms));
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (registration_attempted_) {
      return Fail(error, "Register is single-use for this process generation");
    }
    if (stop_requested_) {
      return Fail(error, "Register was cancelled before dispatch");
    }
    registration_attempted_ = true;
    active_register_context_ = context;
  }
  xgc::adapter::v1::RegisterRequest request = config_.registration();
  request.set_sdk_version(kClientVersion);
  xgc::adapter::v1::RegisterResponse response;
  const grpc::Status status = stub_->Register(context.get(), request, &response);
  SecureErase(request.mutable_bootstrap_token());
  internal::ClientConfigAccess::ForgetBootstrapToken(&config_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_register_context_ == context) {
      active_register_context_.reset();
    }
  }
  if (!status.ok()) {
    return Fail(error, status.error_message().empty() ? "Register transport failed"
                                                      : status.error_message());
  }
  if (!response.accepted()) {
    return Fail(error, response.error().message().empty() ? "Register was rejected"
                                                          : response.error().message());
  }
  if (response.host_id().empty() || response.session_id().empty() ||
      response.process_generation() != config_.registration().process_generation() ||
      response.session_generation() == 0 || response.runtime_epoch() == 0) {
    return Fail(error, "Register omitted or mismatched session fencing");
  }
  if (response.runtime_link_protocol_version() != kRuntimeLinkProtocolVersion) {
    return Fail(error, "Host did not select Runtime Link protocol version 2");
  }
  if (response.maximum_control_frame_bytes() == 0 ||
      response.maximum_work_frame_bytes() == 0) {
    return Fail(error, "Host omitted mandatory Control/Work frame limits");
  }
  const std::size_t work_frame_bytes = response.maximum_work_frame_bytes();
  const std::size_t digest_metadata_bytes = internal::kCanonicalSha256TextBytes * 2U;
  if (work_frame_bytes >
          std::numeric_limits<std::size_t>::max() - digest_metadata_bytes ||
      work_frame_bytes >
          (std::numeric_limits<std::size_t>::max() - digest_metadata_bytes) / 2U) {
    return Fail(error, "negotiated Work frame limit cannot be represented");
  }
  const std::size_t minimum_terminal_replay_bytes =
      work_frame_bytes + digest_metadata_bytes;
  const std::size_t minimum_source_state_bytes =
      work_frame_bytes * 2U + digest_metadata_bytes;
  if (config_.maximum_control_queue_bytes < response.maximum_control_frame_bytes() ||
      config_.maximum_work_queue_bytes < response.maximum_work_frame_bytes() ||
      config_.maximum_dispatch_queue_bytes < response.maximum_work_frame_bytes() ||
      config_.maximum_terminal_replay_bytes < minimum_terminal_replay_bytes ||
      config_.maximum_source_state_bytes < minimum_source_state_bytes) {
    return Fail(error, "configured byte budgets cannot hold one negotiated frame");
  }

  fence->host_id = response.host_id();
  fence->session_id = response.session_id();
  fence->session_generation = response.session_generation();
  fence->runtime_epoch = response.runtime_epoch();
  fence->runtime_link_protocol_version = response.runtime_link_protocol_version();
  fence->heartbeat_interval_ms = response.heartbeat_interval_ms() == 0
                                     ? internal::kDefaultHeartbeatIntervalMs
                                     : std::max(internal::kMinimumHeartbeatIntervalMs,
                                                response.heartbeat_interval_ms());
  fence->maximum_control_frame_bytes = response.maximum_control_frame_bytes();
  fence->maximum_work_frame_bytes = response.maximum_work_frame_bytes();
  fence->connection_epoch = 1;
  return true;
}

void Client::Impl::InstallSession(const SessionFence& fence) {
  std::lock_guard<std::mutex> lock(mutex_);
  session_failed_ = false;
  host_id_ = fence.host_id;
  session_id_ = fence.session_id;
  session_generation_ = fence.session_generation;
  runtime_epoch_ = fence.runtime_epoch;
  runtime_link_protocol_version_ = fence.runtime_link_protocol_version;
  heartbeat_interval_ms_ = fence.heartbeat_interval_ms;
  maximum_control_frame_bytes_ = fence.maximum_control_frame_bytes;
  maximum_work_frame_bytes_ = fence.maximum_work_frame_bytes;
  connection_epoch_counter_ = fence.connection_epoch;
  desired_spec_ = config_.initial_spec();
  control_out_sequence_ = 0;
  work_out_sequence_ = 0;
  control_in_sequence_ = 0;
  work_in_sequence_ = 0;
  control_queue_.clear();
  work_queue_.clear();
  dispatch_queue_.clear();
  source_streams_.clear();
  closed_sources_.clear();
  control_queue_bytes_ = 0;
  work_queue_bytes_ = 0;
  dispatch_queue_bytes_ = 0;
  source_state_bytes_ = 0;
  capability_statuses_ = DisabledCapabilityStatuses();
  accepting_work_ = false;
  drain_requested_ = false;
  drain_reported_ = false;
  ready_callbacks_complete_ = false;
  state_ = ClientState::kRegistered;
  condition_.notify_all();
}

void Client::Impl::PrepareConnectionPairLocked(const SessionFence& fence) {
  session_failed_ = false;
  pair_ready_ = false;
  work_attach_allowed_ = false;
  work_attach_connection_epoch_ = 0;
  control_out_sequence_ = 0;
  work_out_sequence_ = 0;
  control_in_sequence_ = 0;
  work_in_sequence_ = 0;
  control_queue_.clear();
  work_queue_.clear();
  dispatch_queue_.clear();
  while (!work_tokens_.empty()) {
    ReleaseWorkTokenLocked(work_tokens_.begin()->first);
  }
  closed_sources_.clear();
  control_queue_bytes_ = 0;
  work_queue_bytes_ = 0;
  dispatch_queue_bytes_ = 0;
  source_state_bytes_ = 0;
  accepting_work_ = false;
  drain_requested_ = false;
  drain_reported_ = false;
  ready_callbacks_complete_ = spec_revision_ != 0;
  state_ = fence.connection_epoch == 1 ? ClientState::kRegistered
                                       : ClientState::kReconnecting;
}

void Client::Impl::PreparePairReplacement() {
  std::vector<SourceState> sources;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_work_ = false;
    CancelAllWorkLocked();
    for (const auto& source : source_streams_) {
      sources.push_back(source.second);
    }
    source_streams_.clear();
    dispatch_queue_.clear();
    dispatch_queue_bytes_ = 0;
  }
  condition_.notify_all();
  WaitForCallbacks();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    while (!work_tokens_.empty()) {
      ReleaseWorkTokenLocked(work_tokens_.begin()->first);
    }
  }
  const auto error = internal::MakeError(
      xgc::adapter::v1::ERROR_CLASS_CANCELLED, "connection-pair-replaced",
      StopRequested() ? "Runtime Link stopped"
                      : "Runtime Link connection pair was replaced");
  for (const auto& source : sources) {
    NotifySourceClosed(source, error);
  }
}

void Client::Impl::ClearConnectionStateLocked() {
  active_control_context_.reset();
  active_work_context_.reset();
  control_queue_.clear();
  work_queue_.clear();
  dispatch_queue_.clear();
  while (!work_tokens_.empty()) {
    ReleaseWorkTokenLocked(work_tokens_.begin()->first);
  }
  control_queue_bytes_ = 0;
  work_queue_bytes_ = 0;
  dispatch_queue_bytes_ = 0;
  control_out_sequence_ = 0;
  work_out_sequence_ = 0;
  control_in_sequence_ = 0;
  work_in_sequence_ = 0;
  work_attach_allowed_ = false;
  pair_ready_ = false;
  work_attach_connection_epoch_ = 0;
  session_failed_ = false;
  accepting_work_ = false;
  drain_requested_ = false;
  drain_reported_ = false;
  ready_callbacks_complete_ = spec_revision_ != 0;
}

void Client::Impl::ResetSessionLocked() {
  active_register_context_.reset();
  active_control_context_.reset();
  active_work_context_.reset();
  host_id_.clear();
  session_id_.clear();
  session_generation_ = 0;
  runtime_epoch_ = 0;
  runtime_link_protocol_version_ = 0;
  heartbeat_interval_ms_ = 0;
  maximum_control_frame_bytes_ = 0;
  maximum_work_frame_bytes_ = 0;
  connection_epoch_counter_ = 0;
  spec_revision_ = 0;
  spec_digest_.clear();
  active_spec_.Clear();
  desired_spec_.Clear();
  active_capabilities_.clear();
  source_streams_.clear();
  closed_sources_.clear();
  control_queue_.clear();
  work_queue_.clear();
  dispatch_queue_.clear();
  while (!work_tokens_.empty()) {
    ReleaseWorkTokenLocked(work_tokens_.begin()->first);
  }
  replay_.clear();
  control_queue_bytes_ = 0;
  work_queue_bytes_ = 0;
  dispatch_queue_bytes_ = 0;
  source_state_bytes_ = 0;
  terminal_replay_bytes_ = 0;
  capability_statuses_ = DisabledCapabilityStatuses();
  accepting_work_ = false;
  session_failed_ = false;
  drain_requested_ = false;
  drain_reported_ = false;
  ready_callbacks_complete_ = false;
  work_attach_allowed_ = false;
  pair_ready_ = false;
  work_attach_connection_epoch_ = 0;
  terminal_session_failure_ = false;
  terminal_session_error_.clear();
}

void Client::Impl::CancelActiveStreams() {
  std::shared_ptr<grpc::ClientContext> control;
  std::shared_ptr<grpc::ClientContext> work;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    control = active_control_context_;
    work = active_work_context_;
  }
  if (control) {
    control->TryCancel();
  }
  if (work) {
    work->TryCancel();
  }
  condition_.notify_all();
}

void Client::Impl::SignalCurrentSessionFailure(const std::string& reason) {
  std::string session;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session = session_id_;
  }
  if (!session.empty()) {
    SignalSessionFailure(session, reason);
  }
}

void Client::Impl::SignalSessionFailure(const std::string& expected_session,
                                        const std::string& reason) {
  std::shared_ptr<grpc::ClientContext> control;
  std::shared_ptr<grpc::ClientContext> work;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ != expected_session || stop_requested_ || session_failed_) {
      return;
    }
    session_failed_ = true;
    accepting_work_ = false;
    state_ = ClientState::kReconnecting;
    last_error_ = reason;
    CancelAllWorkLocked();
    control = active_control_context_;
    work = active_work_context_;
  }
  if (control) {
    control->TryCancel();
  }
  if (work) {
    work->TryCancel();
  }
  condition_.notify_all();
  Log(LogLevel::kWarning, "Runtime Link session failed: " + reason);
}

void Client::Impl::SignalTerminalSessionFailure(const std::string& expected_session,
                                                const std::string& reason) {
  std::shared_ptr<grpc::ClientContext> control;
  std::shared_ptr<grpc::ClientContext> work;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ != expected_session || stop_requested_) {
      return;
    }
    terminal_session_failure_ = true;
    terminal_session_error_ = reason;
    session_failed_ = true;
    accepting_work_ = false;
    last_error_ = reason;
    CancelAllWorkLocked();
    control = active_control_context_;
    work = active_work_context_;
  }
  if (control) {
    control->TryCancel();
  }
  if (work) {
    work->TryCancel();
  }
  condition_.notify_all();
  Log(LogLevel::kError, "Runtime Link session is irrecoverable: " + reason);
}

}  // namespace adapter_runtime
}  // namespace xgc2
