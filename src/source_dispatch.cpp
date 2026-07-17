#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::MakeError;
using internal::SchemaMatches;
using internal::UnixNanos;

namespace {

bool HasError(const xgc::adapter::v1::AdapterError& error) {
  return error.class_() != xgc::adapter::v1::ERROR_CLASS_UNSPECIFIED ||
         !error.code().empty() || !error.message().empty() ||
         error.retry_after_ms() != 0 || error.has_details();
}

bool HasPayload(const xgc::v1::Payload& payload) { return payload.ByteSizeLong() != 0; }

}  // namespace

void Client::Impl::HandleSourceOpen(const DispatchJob& job) {
  const SourceState initial_source = [&]() {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto source = source_streams_.find(job.work_id);
    return source == source_streams_.end() ? SourceState() : source->second;
  }();
  if (!initial_source.cancellation ||
      initial_source.connection_epoch != job.connection_epoch) {
    return;
  }

  const auto& binding = config_.capabilities()[initial_source.binding_index];
  const auto* endpoint =
      FindEndpoint(binding.contract, job.source_open.context().endpoint_id());
  SourceOpenDecision decision;
  if (job.cancellation->requested.load()) {
    decision =
        SourceOpenDecision::Reject(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "cancelled",
                                   "source open was cancelled before dispatch");
  } else {
    try {
      decision = binding.callbacks.source_open(job.source_open,
                                               CancellationToken(job.cancellation));
    } catch (const std::exception& exception) {
      Log(LogLevel::kError,
          "source open handler threw: " + std::string(exception.what()));
      decision = SourceOpenDecision::Reject(xgc::adapter::v1::ERROR_CLASS_PERMANENT,
                                            "handler-exception",
                                            "source open handler threw an exception");
    } catch (...) {
      Log(LogLevel::kError, "source open handler threw an unknown exception");
      decision = SourceOpenDecision::Reject(
          xgc::adapter::v1::ERROR_CLASS_PERMANENT, "handler-exception",
          "source open handler threw an unknown exception");
    }
  }

  if (job.cancellation->requested.load()) {
    decision = SourceOpenDecision::Reject(xgc::adapter::v1::ERROR_CLASS_CANCELLED,
                                          "cancelled", "source open was cancelled");
  } else if (decision.accepted &&
             (HasError(decision.error) || (!decision.has_initial_payload &&
                                           HasPayload(decision.initial_payload)))) {
    decision = SourceOpenDecision::Reject(
        xgc::adapter::v1::ERROR_CLASS_PERMANENT, "invalid-handler-result",
        "accepted source open also returned an error");
  } else if (!decision.accepted &&
             (decision.has_initial_payload || HasPayload(decision.initial_payload) ||
              !internal::IsTypedError(decision.error))) {
    decision = SourceOpenDecision::Reject(
        xgc::adapter::v1::ERROR_CLASS_PERMANENT, "invalid-handler-result",
        "rejected source open must return one typed error and no payload");
  } else if (decision.accepted && decision.has_initial_payload &&
             (endpoint == nullptr ||
              !internal::IsValidPayload(decision.initial_payload) ||
              !internal::SchemaMatches(endpoint->output_schema(),
                                       decision.initial_payload.schema()) ||
              (endpoint->limits().maximum_response_bytes() > 0 &&
               decision.initial_payload.value().size() >
                   endpoint->limits().maximum_response_bytes()))) {
    decision = SourceOpenDecision::Reject(
        xgc::adapter::v1::ERROR_CLASS_REJECTED, "initial-payload-invalid",
        "source initial payload violates its output contract");
  }

  SourceState closed_source;
  bool notify_source = false;
  bool fail_pair = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto source = source_streams_.find(job.work_id);
    if (source == source_streams_.end() ||
        source->second.connection_epoch != job.connection_epoch ||
        source->second.cancellation != job.cancellation) {
      return;
    }
    if (session_failed_ || stop_requested_ ||
        connection_epoch_counter_ != job.connection_epoch) {
      return;
    }
    if (work_queue_.size() >= config_.maximum_work_queue) {
      ++dropped_outbound_frames_;
      fail_pair = true;
    } else {
      xgc::adapter::v1::WorkRequest frame;
      auto* result = frame.mutable_source_open_result();
      result->set_stream_id(job.work_id);
      result->set_accepted(decision.accepted);
      if (decision.accepted) {
        if (decision.has_initial_payload) {
          *result->mutable_initial_payload() = std::move(decision.initial_payload);
        }
      } else {
        *result->mutable_error() = decision.error;
      }
      if (decision.accepted) {
        if (!WorkFrameFitsLocked(&frame) || !QueueWorkLocked(std::move(frame))) {
          fail_pair = true;
        } else {
          source->second.phase = SourceState::Phase::kOpen;
          source->second.message_credit = job.source_open.initial_credit().messages();
          source->second.byte_credit = job.source_open.initial_credit().bytes();
        }
      } else if (!CommitSourceTerminalLocked(job.work_id, source->second,
                                             std::move(frame))) {
        fail_pair = true;
      } else {
        if (decision.error.class_() == xgc::adapter::v1::ERROR_CLASS_CANCELLED) {
          closed_source = source->second;
          notify_source = true;
        }
        source_streams_.erase(source);
      }
    }
  }
  if (fail_pair) {
    SignalCurrentSessionFailure("bounded Work queue could not send SourceOpenResult");
  } else if (notify_source) {
    NotifySourceClosed(closed_source, decision.error);
  }
}

bool Client::Impl::ValidateInput(
    const xgc::adapter::v1::CapabilityEndpointContract& endpoint,
    const xgc::v1::Payload& input) const {
  if (!SchemaMatches(endpoint.input_schema(), input.schema())) {
    return false;
  }
  return internal::IsValidPayload(input) &&
         input.value().size() <= endpoint.limits().maximum_request_bytes();
}

void Client::Impl::CompleteCancelledJob(const DispatchJob& job) {
  if (CommitTerminalFrame(job,
                          RejectedDispatch(job.kind, job.work_id, "cancelled",
                                           "Work was cancelled before dispatch",
                                           xgc::adapter::v1::ERROR_CLASS_CANCELLED)) !=
      TerminalCommitResult::kCommitted) {
    SignalCurrentSessionFailure(
        "bounded Work transport could not retain cancellation terminal");
  }
  EraseWorkToken(job.work_id);
}

void Client::Impl::EraseWorkToken(const std::string& work_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  ReleaseWorkTokenLocked(work_id);
  MaybeQueueDrainStatusLocked();
}

xgc::adapter::v1::WorkRequest Client::Impl::RejectedDispatch(
    DispatchKind kind, const std::string& work_id, const std::string& code,
    const std::string& message, xgc::adapter::v1::ErrorClass error_class) const {
  xgc::adapter::v1::WorkRequest frame;
  const auto error = MakeError(error_class, code, message);
  if (kind == DispatchKind::kUnary) {
    auto* response = frame.mutable_unary_response();
    response->set_work_id(work_id);
    *response->mutable_error() = error;
  } else {
    auto* event = frame.mutable_operation_event();
    event->set_work_id(work_id);
    event->set_occurred_unix_nanos(UnixNanos());
    if (error_class == xgc::adapter::v1::ERROR_CLASS_CANCELLED) {
      event->set_phase(xgc::adapter::v1::OPERATION_PHASE_CANCELLED);
    } else if (error_class == xgc::adapter::v1::ERROR_CLASS_DEADLINE) {
      event->set_phase(xgc::adapter::v1::OPERATION_PHASE_EXPIRED);
    } else if (error_class == xgc::adapter::v1::ERROR_CLASS_REJECTED) {
      event->set_phase(xgc::adapter::v1::OPERATION_PHASE_REJECTED);
    } else {
      event->set_phase(xgc::adapter::v1::OPERATION_PHASE_FAILED);
    }
    *event->mutable_error() = error;
  }
  return frame;
}

xgc::adapter::v1::WorkRequest Client::Impl::OperationFrame(
    const std::string& work_id, xgc::adapter::v1::OperationPhase phase) const {
  xgc::adapter::v1::WorkRequest frame;
  auto* event = frame.mutable_operation_event();
  event->set_work_id(work_id);
  event->set_phase(phase);
  event->set_occurred_unix_nanos(UnixNanos());
  return frame;
}

}  // namespace adapter_runtime
}  // namespace xgc2
