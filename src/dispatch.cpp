#include <algorithm>
#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::MakeError;
using internal::NormalizeDeadline;
using internal::SchemaMatches;
using internal::UnixNanos;

namespace {

bool IsValidSubject(const xgc::adapter::v1::ScopeReference& subject) {
  if (!internal::IsCanonicalIdentifier(subject.kind()) ||
      !internal::IsCanonicalSha256(subject.key()) || subject.attributes().empty()) {
    return false;
  }
  for (const auto& attribute : subject.attributes()) {
    if (!internal::IsCanonicalIdentifier(attribute.first) ||
        !internal::IsCanonicalText(attribute.second)) {
      return false;
    }
  }
  return true;
}

bool HasError(const xgc::adapter::v1::AdapterError& error) {
  return error.class_() != xgc::adapter::v1::ERROR_CLASS_UNSPECIFIED ||
         !error.code().empty() || !error.message().empty() ||
         error.retry_after_ms() != 0 || error.has_details();
}

bool HasPayload(const xgc::v1::Payload& payload) { return payload.ByteSizeLong() != 0; }

bool FitsByteBudget(std::size_t used, std::size_t added, std::size_t maximum) {
  return added <= maximum && used <= maximum - added;
}

bool OutputMatches(const xgc::adapter::v1::CapabilityEndpointContract& endpoint,
                   const xgc::v1::Payload& output) {
  return internal::IsValidPayload(output) &&
         internal::SchemaMatches(endpoint.output_schema(), output.schema());
}

}  // namespace

void Client::Impl::EnqueueUnary(const xgc::adapter::v1::UnaryRequest& request,
                                std::uint64_t frame_sequence) {
  DispatchJob job;
  job.kind = DispatchKind::kUnary;
  job.work_id = request.context().work_id();
  job.fingerprint = RequestFingerprint(request.context());
  job.volatile_work = request.context().volatile_();
  job.inbound_frame_sequence = frame_sequence;
  job.unary = request;
  NormalizeDeadline(job.unary.mutable_context()->mutable_deadline(), UnixNanos());
  job.accounted_bytes = job.unary.ByteSizeLong();
  EnqueueDispatch(std::move(job));
}

void Client::Impl::EnqueueOperation(const xgc::adapter::v1::OperationRequest& request,
                                    std::uint64_t frame_sequence) {
  DispatchJob job;
  job.kind = DispatchKind::kOperation;
  job.work_id = request.context().work_id();
  job.fingerprint = RequestFingerprint(request.context());
  job.inbound_frame_sequence = frame_sequence;
  job.operation = request;
  NormalizeDeadline(job.operation.mutable_context()->mutable_deadline(), UnixNanos());
  job.accounted_bytes = job.operation.ByteSizeLong();
  EnqueueDispatch(std::move(job));
}

void Client::Impl::EnqueueSourceOpen(const xgc::adapter::v1::SourceOpenRequest& request,
                                     std::uint64_t frame_sequence) {
  DispatchJob job;
  job.kind = DispatchKind::kSourceOpen;
  job.work_id = request.context().work_id();
  job.fingerprint = RequestFingerprint(request.context());
  job.inbound_frame_sequence = frame_sequence;
  job.source_open = request;
  NormalizeDeadline(job.source_open.mutable_context()->mutable_deadline(), UnixNanos());
  job.accounted_bytes = job.source_open.ByteSizeLong();

  xgc::adapter::v1::WorkRequest immediate;
  bool have_immediate = false;
  bool replay_immediate = false;
  bool immediate_committed = false;
  bool identity_violation = false;
  bool retire_pair = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto reject = [&](xgc::adapter::v1::ErrorClass error_class,
                            const std::string& code, const std::string& message) {
      immediate =
          SourceOpenRejected(job.work_id, MakeError(error_class, code, message));
      have_immediate = true;
      ++rejected_dispatches_;
    };

    const bool exact_identity = !job.work_id.empty();
    const std::size_t digest_bytes = internal::kCanonicalSha256TextBytes;
    const std::size_t maximum_size = std::numeric_limits<std::size_t>::max();
    bool source_accounting_overflow =
        job.fingerprint.size() > maximum_size - digest_bytes;
    std::size_t source_metadata_bytes = digest_bytes;
    if (!source_accounting_overflow) {
      source_metadata_bytes += job.fingerprint.size();
      source_accounting_overflow =
          maximum_work_frame_bytes_ > maximum_size - source_metadata_bytes;
    }
    if (!source_accounting_overflow) {
      source_metadata_bytes += maximum_work_frame_bytes_;
      source_accounting_overflow =
          job.accounted_bytes > maximum_size - source_metadata_bytes;
    }
    const std::size_t source_accounted_bytes =
        source_accounting_overflow ? maximum_size
                                   : job.accounted_bytes + source_metadata_bytes;

    std::size_t binding_index = kNoBinding;
    const xgc::adapter::v1::CapabilityEndpointContract* endpoint = nullptr;
    std::string validation_error;
    bool duplicate_work_id = work_tokens_.find(job.work_id) != work_tokens_.end() ||
                             replay_.find(job.work_id) != replay_.end();
    for (const auto& source : source_streams_) {
      duplicate_work_id =
          duplicate_work_id || source.second.request.context().work_id() == job.work_id;
    }

    const auto closed = closed_sources_.find(job.work_id);
    if (exact_identity && closed != closed_sources_.end()) {
      if (closed->second.fingerprint == job.fingerprint &&
          !closed->second.terminal_digest.empty()) {
        immediate = closed->second.terminal_frame;
        have_immediate = true;
        replay_immediate = true;
      } else {
        identity_violation = true;
      }
    } else if (exact_identity &&
               (source_streams_.find(job.work_id) != source_streams_.end() ||
                duplicate_work_id)) {
      identity_violation = true;
    } else if (job.work_id.empty()) {
      identity_violation = true;
    } else if (request.context().capability_id().empty() ||
               request.context().endpoint_id().empty()) {
      reject(xgc::adapter::v1::ERROR_CLASS_REJECTED, "invalid-source-open",
             "source stream and WorkContext identity must not be empty");
    } else if (!request.has_initial_credit() ||
               request.initial_credit().messages() == 0 ||
               request.initial_credit().bytes() == 0) {
      reject(xgc::adapter::v1::ERROR_CLASS_REJECTED, "invalid-initial-credit",
             "source open requires positive message and byte credit");
    } else if (!ValidateWorkContextLocked(job.source_open.context(),
                                          DispatchKind::kSourceOpen, &binding_index,
                                          &endpoint, &validation_error)) {
      reject(xgc::adapter::v1::ERROR_CLASS_REJECTED, "invalid-work-context",
             validation_error);
    } else if (!config_.capabilities()[binding_index].callbacks.source_open) {
      reject(xgc::adapter::v1::ERROR_CLASS_REJECTED, "handler-not-bound",
             "source endpoint has no bound open handler");
    } else if (source_streams_.size() + closed_sources_.size() >=
               config_.maximum_source_tombstones) {
      retire_pair = true;
    } else if (source_streams_.size() >= config_.maximum_source_streams) {
      reject(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED, "source-stream-limit",
             "global source stream limit is exhausted");
    } else if (endpoint->limits().maximum_streams() > 0 &&
               CountSourceStreamsLocked(request.context().capability_id(),
                                        request.context().endpoint_id()) >=
                   endpoint->limits().maximum_streams()) {
      reject(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED, "endpoint-stream-limit",
             "endpoint source stream limit is exhausted");
    } else if (dispatch_queue_.size() >= config_.maximum_dispatch_queue) {
      reject(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED, "dispatch-queue-full",
             "bounded dispatch queue is full");
    } else if (!FitsByteBudget(dispatch_queue_bytes_, job.accounted_bytes,
                               config_.maximum_dispatch_queue_bytes)) {
      reject(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED, "dispatch-byte-budget",
             "dispatch byte budget is exhausted");
    } else if (source_accounting_overflow ||
               !FitsByteBudget(source_state_bytes_, source_accounted_bytes,
                               config_.maximum_source_state_bytes)) {
      retire_pair = true;
    } else {
      SourceState source;
      source.request = job.source_open;
      source.binding_index = binding_index;
      source.cancellation = std::make_shared<CancellationState>();
      source.connection_epoch = connection_epoch_counter_;
      source.maximum_chunk_bytes = endpoint->limits().maximum_stream_chunk_bytes();
      source.maximum_chunk_messages =
          endpoint->limits().maximum_stream_chunk_messages();
      source.accounted_bytes = source_accounted_bytes;
      job.cancellation = source.cancellation;
      job.connection_epoch = source.connection_epoch;
      source_streams_.emplace(job.work_id, std::move(source));
      source_state_bytes_ += source_accounted_bytes;
      dispatch_queue_bytes_ += job.accounted_bytes;
      dispatch_queue_.push_back(std::move(job));
      condition_.notify_all();
    }

    if (have_immediate && !replay_immediate && !identity_violation && !retire_pair) {
      if (source_streams_.size() + closed_sources_.size() >=
              config_.maximum_source_tombstones ||
          source_accounting_overflow ||
          !FitsByteBudget(source_state_bytes_, source_accounted_bytes,
                          config_.maximum_source_state_bytes)) {
        retire_pair = true;
      } else {
        SourceState terminal;
        terminal.request = job.source_open;
        terminal.cancellation = std::make_shared<CancellationState>();
        terminal.connection_epoch = connection_epoch_counter_;
        terminal.accounted_bytes = source_accounted_bytes;
        source_state_bytes_ += source_accounted_bytes;
        if (CommitSourceTerminalLocked(job.work_id, terminal, std::move(immediate))) {
          immediate_committed = true;
        } else {
          source_state_bytes_ -= source_accounted_bytes;
          retire_pair = true;
        }
      }
    }
    if (identity_violation) {
      ++rejected_dispatches_;
    }
  }
  if (identity_violation) {
    QueueWorkProtocolError(
        job.inbound_frame_sequence,
        "SourceOpenRequest must use one exact, unused context.work_id identity");
  } else if (retire_pair) {
    SignalCurrentSessionFailure(
        "source identity or byte budget requires connection-pair retirement");
  } else if (have_immediate && !immediate_committed &&
             !QueueWork(std::move(immediate))) {
    SignalCurrentSessionFailure("Work queue rejected SourceOpenResult");
  }
}

void Client::Impl::EnqueueDispatch(DispatchJob job) {
  xgc::adapter::v1::WorkRequest immediate;
  bool have_immediate = false;
  bool replay_immediate = false;
  bool identity_violation = false;
  bool retire_pair = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto replay = replay_.find(job.work_id);
    if (!job.work_id.empty() && replay != replay_.end()) {
      if (replay->second.kind == job.kind &&
          replay->second.fingerprint == job.fingerprint) {
        if (job.kind == DispatchKind::kUnary) {
          *immediate.mutable_unary_response() = replay->second.unary;
        } else {
          *immediate.mutable_operation_event() = replay->second.operation;
        }
        replay_immediate = true;
      } else {
        identity_violation = true;
      }
      have_immediate = !identity_violation;
    } else if (job.work_id.empty()) {
      identity_violation = true;
    } else if (!accepting_work_ || state_ != ClientState::kReady) {
      immediate = RejectedDispatch(job.kind, job.work_id, "not-ready",
                                   "capabilities are not ready for Work");
      have_immediate = true;
    } else if (work_tokens_.find(job.work_id) != work_tokens_.end()) {
      identity_violation = true;
    } else if (std::any_of(source_streams_.begin(), source_streams_.end(),
                           [&job](const auto& source) {
                             return source.second.request.context().work_id() ==
                                    job.work_id;
                           })) {
      identity_violation = true;
    } else if (dispatch_queue_.size() >= config_.maximum_dispatch_queue) {
      immediate = RejectedDispatch(job.kind, job.work_id, "dispatch-queue-full",
                                   "bounded dispatch queue is full");
      have_immediate = true;
    } else if (!FitsByteBudget(dispatch_queue_bytes_, job.accounted_bytes,
                               config_.maximum_dispatch_queue_bytes)) {
      immediate = RejectedDispatch(job.kind, job.work_id, "dispatch-byte-budget",
                                   "dispatch byte budget is exhausted");
      have_immediate = true;
    } else {
      job.cancellation = std::make_shared<CancellationState>();
      if (!ReserveTerminalReplayLocked(&job)) {
        retire_pair = true;
      } else {
        WorkTokenState token;
        token.cancellation = job.cancellation;
        token.terminal_reservation_bytes = job.terminal_reservation_bytes;
        token.volatile_work = job.volatile_work;
        work_tokens_.emplace(job.work_id, std::move(token));
        dispatch_queue_bytes_ += job.accounted_bytes;
        dispatch_queue_.push_back(std::move(job));
        condition_.notify_all();
      }
    }
    if (have_immediate) {
      ++rejected_dispatches_;
    }
    if (identity_violation) {
      ++rejected_dispatches_;
    }
  }
  if (identity_violation) {
    QueueWorkProtocolError(
        job.inbound_frame_sequence,
        "Work request must use one exact, unused nonempty work_id identity");
  } else if (retire_pair) {
    SignalCurrentSessionFailure(
        "terminal replay identity or byte budget requires connection-pair retirement");
  } else if (replay_immediate) {
    if (!QueueWork(std::move(immediate))) {
      SignalCurrentSessionFailure("Work queue rejected dispatch response");
    }
  } else if (have_immediate) {
    const auto commit = CommitTerminalFrame(job, std::move(immediate));
    if (commit != TerminalCommitResult::kCommitted) {
      SignalCurrentSessionFailure(
          "bounded Work transport could not retain immediate terminal for " +
          job.work_id);
    }
  }
}

void Client::Impl::DispatchLoop() {
  while (true) {
    DispatchJob job;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock,
                      [this] { return stop_requested_ || !dispatch_queue_.empty(); });
      if (stop_requested_ && dispatch_queue_.empty()) {
        return;
      }
      job = std::move(dispatch_queue_.front());
      dispatch_queue_bytes_ -= job.accounted_bytes;
      dispatch_queue_.pop_front();
      ++active_dispatch_jobs_;
    }

    if (job.kind == DispatchKind::kSourceOpen) {
      bool dispatch = false;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto source = source_streams_.find(job.work_id);
        dispatch = source != source_streams_.end() &&
                   source->second.connection_epoch == job.connection_epoch &&
                   source->second.cancellation == job.cancellation;
        if (dispatch) {
          ++in_flight_work_;
        }
      }
      if (dispatch) {
        HandleSourceOpen(job);
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (dispatch && in_flight_work_ > 0) {
          --in_flight_work_;
        }
        --active_dispatch_jobs_;
        MaybeQueueDrainStatusLocked();
      }
      callbacks_condition_.notify_all();
      condition_.notify_all();
      continue;
    }

    if (job.cancellation->requested.load()) {
      CompleteCancelledJob(job);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        --active_dispatch_jobs_;
      }
      callbacks_condition_.notify_all();
      continue;
    }
    const auto& context = job.kind == DispatchKind::kUnary ? job.unary.context()
                                                           : job.operation.context();
    std::string validation_error;
    std::size_t binding_index = kNoBinding;
    const xgc::adapter::v1::CapabilityEndpointContract* endpoint = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!ValidateWorkContextLocked(context, job.kind, &binding_index, &endpoint,
                                     &validation_error)) {
        ++rejected_dispatches_;
      } else {
        ++in_flight_work_;
      }
    }
    if (binding_index == kNoBinding || endpoint == nullptr) {
      const auto commit = CommitTerminalFrame(
          job, RejectedDispatch(job.kind, job.work_id, "invalid-work-context",
                                validation_error));
      if (commit != TerminalCommitResult::kCommitted) {
        SignalCurrentSessionFailure(
            "bounded Work transport could not retain context rejection");
      }
      EraseWorkToken(job.work_id);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        --active_dispatch_jobs_;
      }
      callbacks_condition_.notify_all();
      continue;
    }

    if (job.kind == DispatchKind::kUnary) {
      HandleUnary(job, binding_index);
    } else {
      HandleOperation(job, binding_index);
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (in_flight_work_ > 0) {
        --in_flight_work_;
      }
      --active_dispatch_jobs_;
      ReleaseWorkTokenLocked(job.work_id);
      MaybeQueueDrainStatusLocked();
    }
    callbacks_condition_.notify_all();
    condition_.notify_all();
  }
}

bool Client::Impl::ValidateWorkContextLocked(
    const xgc::adapter::v1::WorkContext& context, DispatchKind kind,
    std::size_t* binding_index,
    const xgc::adapter::v1::CapabilityEndpointContract** endpoint,
    std::string* error) const {
  if (!accepting_work_ || state_ != ClientState::kReady || session_failed_) {
    return Fail(error, "Runtime Link is not ready");
  }
  if (context.spec_revision() != spec_revision_) {
    return Fail(error, "Work references a stale spec revision");
  }
  const ActiveEndpoint active =
      FindActiveEndpointLocked(context.capability_id(), context.endpoint_id());
  if (!active.valid || context.contract_version() != active.grant->contract_version() ||
      context.contract_digest() != active.grant->contract_digest()) {
    return Fail(error, "Work does not match an active capability contract");
  }
  const auto expected_mode =
      kind == DispatchKind::kUnary
          ? xgc::adapter::v1::INTERACTION_MODE_UNARY
          : (kind == DispatchKind::kOperation
                 ? xgc::adapter::v1::INTERACTION_MODE_OPERATION
                 : xgc::adapter::v1::INTERACTION_MODE_STREAM_SOURCE);
  if (active.endpoint->interaction_mode() != expected_mode) {
    return Fail(error, "Work interaction mode does not match the endpoint");
  }
  if (context.volatile_() != active.endpoint->volatile_supported()) {
    return Fail(error, "Work volatile mode does not match the endpoint contract");
  }
  if (context.volatile_() &&
      (kind != DispatchKind::kUnary || !context.idempotency_key().empty())) {
    return Fail(error,
                "volatile Work must be unary and must not declare an idempotency key");
  }
  if (active.endpoint->deadline_required() &&
      context.deadline().deadline_unix_nanos() <= 0) {
    return Fail(error, "endpoint requires a deadline");
  }
  if (context.deadline().deadline_unix_nanos() > 0 &&
      UnixNanos() >= context.deadline().deadline_unix_nanos()) {
    return Fail(error, "Work deadline has expired");
  }
  if (active.endpoint->idempotency_mode() ==
          xgc::adapter::v1::IDEMPOTENCY_MODE_REQUIRED &&
      context.idempotency_key().empty()) {
    return Fail(error, "endpoint requires an idempotency key");
  }
  if (!internal::IsCanonicalSha256(context.request_digest())) {
    return Fail(error, "Work request_digest must use sha256:<lowercase-hex>");
  }
  if (!context.has_subject() || !IsValidSubject(context.subject())) {
    return Fail(error, "Work subject is missing or non-canonical");
  }
  *binding_index = active.binding_index;
  *endpoint = active.endpoint;
  return true;
}

void Client::Impl::HandleUnary(const DispatchJob& job, std::size_t binding_index) {
  const auto& binding = config_.capabilities()[binding_index];
  const auto* endpoint =
      FindEndpoint(binding.contract, job.unary.context().endpoint_id());
  if (endpoint == nullptr || !ValidateInput(*endpoint, job.unary.input())) {
    if (CommitTerminalFrame(
            job,
            RejectedDispatch(DispatchKind::kUnary, job.work_id, "input-schema-mismatch",
                             "unary input does not match its contract")) !=
        TerminalCommitResult::kCommitted) {
      SignalCurrentSessionFailure(
          "bounded Work transport could not retain unary input rejection");
    }
    return;
  }

  UnaryResult result;
  try {
    result = binding.callbacks.unary(job.unary, CancellationToken(job.cancellation));
  } catch (const std::exception& exception) {
    Log(LogLevel::kError, "unary handler threw: " + std::string(exception.what()));
    result =
        UnaryResult::Failure(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                             "handler-exception", "unary handler threw an exception");
  } catch (...) {
    Log(LogLevel::kError, "unary handler threw an unknown exception");
    result = UnaryResult::Failure(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                                  "handler-exception",
                                  "unary handler threw an unknown exception");
  }

  if (result.succeeded &&
      (HasError(result.error) || !OutputMatches(*endpoint, result.output))) {
    result = UnaryResult::Failure(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                                  "invalid-handler-result",
                                  "unary handler returned a malformed success result");
  } else if (!result.succeeded &&
             (HasPayload(result.output) || !internal::IsTypedError(result.error))) {
    result = UnaryResult::Failure(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                                  "invalid-handler-result",
                                  "unary handler returned a malformed failure result");
  } else if (result.succeeded && result.output.value().size() >
                                     endpoint->limits().maximum_response_bytes()) {
    result = UnaryResult::Failure(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED,
                                  "response-too-large",
                                  "unary output exceeds its contract limit");
  } else if (job.cancellation->requested.load() && result.succeeded) {
    result = UnaryResult::Failure(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "cancelled",
                                  "unary work was cancelled");
  }

  xgc::adapter::v1::UnaryResponse response;
  response.set_work_id(job.work_id);
  if (result.succeeded) {
    *response.mutable_output() = std::move(result.output);
  } else {
    *response.mutable_error() = std::move(result.error);
  }
  xgc::adapter::v1::WorkRequest frame;
  *frame.mutable_unary_response() = std::move(response);
  auto commit = CommitTerminalFrame(job, std::move(frame));
  if (commit == TerminalCommitResult::kTooLarge) {
    xgc::adapter::v1::WorkRequest fallback;
    auto* compact = fallback.mutable_unary_response();
    compact->set_work_id(job.work_id);
    *compact->mutable_error() =
        MakeError(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED,
                  "response-too-large", "unary result exceeds the Work frame limit");
    commit = CommitTerminalFrame(job, std::move(fallback));
  }
  if (commit != TerminalCommitResult::kCommitted) {
    SignalCurrentSessionFailure(
        "bounded Work transport could not commit the unary terminal result");
  }
}

void Client::Impl::HandleOperation(const DispatchJob& job, std::size_t binding_index) {
  const auto& binding = config_.capabilities()[binding_index];
  const auto* endpoint =
      FindEndpoint(binding.contract, job.operation.context().endpoint_id());
  if (endpoint == nullptr || !ValidateInput(*endpoint, job.operation.input())) {
    if (CommitTerminalFrame(
            job, RejectedDispatch(DispatchKind::kOperation, job.work_id,
                                  "input-schema-mismatch",
                                  "operation input does not match its contract")) !=
        TerminalCommitResult::kCommitted) {
      SignalCurrentSessionFailure(
          "bounded Work transport could not retain operation input rejection");
    }
    return;
  }
  if (!QueueWork(
          OperationFrame(job.work_id, xgc::adapter::v1::OPERATION_PHASE_ACCEPTED)) ||
      !QueueWork(
          OperationFrame(job.work_id, xgc::adapter::v1::OPERATION_PHASE_STARTED))) {
    SignalCurrentSessionFailure(
        "bounded Work transport could not send operation lifecycle events");
    return;
  }

  // DispatchLoop owns pre-dispatch cancellation. Once the native operation
  // callback runs, cancellation is cooperative input and its returned terminal
  // result is authoritative.
  OperationResult result;
  try {
    result =
        binding.callbacks.operation(job.operation, CancellationToken(job.cancellation));
  } catch (const std::exception& exception) {
    Log(LogLevel::kError, "operation handler threw: " + std::string(exception.what()));
    result = OperationResult::Failure(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                                      "handler-exception",
                                      "operation handler threw an exception");
  } catch (...) {
    Log(LogLevel::kError, "operation handler threw an unknown exception");
    result = OperationResult::Failure(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                                      "handler-exception",
                                      "operation handler threw an unknown exception");
  }

  if (!internal::IsTerminal(result.phase) ||
      (result.phase == xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED &&
       (HasError(result.error) ||
        (result.has_output && !OutputMatches(*endpoint, result.output)) ||
        (!result.has_output && HasPayload(result.output)))) ||
      (result.phase != xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED &&
       (result.has_output || HasPayload(result.output) ||
        !internal::IsTypedError(result.error) ||
        !internal::ErrorMatchesOperationPhase(result.phase, result.error.class_())))) {
    result = OperationResult::Failure(
        xgc::adapter::v1::ERROR_CLASS_PERMANENT, "invalid-handler-result",
        "operation handler returned a malformed terminal result");
  } else if (result.has_output && result.output.value().size() >
                                      endpoint->limits().maximum_response_bytes()) {
    result = OperationResult::Failure(xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED,
                                      "response-too-large",
                                      "operation output exceeds its contract limit");
  }

  xgc::adapter::v1::OperationEvent terminal;
  terminal.set_work_id(job.work_id);
  terminal.set_phase(result.phase);
  terminal.set_occurred_unix_nanos(UnixNanos());
  terminal.set_native_code(result.native_code);
  if (result.has_output) {
    *terminal.mutable_output() = std::move(result.output);
  }
  if (!result.error.code().empty() ||
      result.error.class_() != xgc::adapter::v1::ERROR_CLASS_UNSPECIFIED) {
    *terminal.mutable_error() = std::move(result.error);
  }
  xgc::adapter::v1::WorkRequest frame;
  *frame.mutable_operation_event() = std::move(terminal);
  auto commit = CommitTerminalFrame(job, std::move(frame));
  if (commit == TerminalCommitResult::kTooLarge) {
    xgc::adapter::v1::WorkRequest fallback;
    auto* compact = fallback.mutable_operation_event();
    compact->set_work_id(job.work_id);
    compact->set_phase(xgc::adapter::v1::OPERATION_PHASE_FAILED);
    compact->set_occurred_unix_nanos(UnixNanos());
    *compact->mutable_error() = MakeError(
        xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED, "response-too-large",
        "operation result exceeds the Work frame limit");
    commit = CommitTerminalFrame(job, std::move(fallback));
  }
  if (commit != TerminalCommitResult::kCommitted) {
    SignalCurrentSessionFailure(
        "bounded Work transport could not commit the operation terminal result");
  }
}

}  // namespace adapter_runtime
}  // namespace xgc2
