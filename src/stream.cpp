#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::MakeError;
using internal::SaturatingAdd;

namespace {

bool AddOverflows(std::uint64_t left, std::uint64_t right) {
  return right > std::numeric_limits<std::uint64_t>::max() - left;
}

}  // namespace

SourceWriteResult Client::Impl::PublishSource(const std::string& stream_id,
                                              std::vector<std::string> items) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ != ClientState::kReady || !accepting_work_ || session_failed_) {
    return SourceWriteResult::kNotReady;
  }
  auto stream = source_streams_.find(stream_id);
  if (stream == source_streams_.end() ||
      stream->second.connection_epoch != connection_epoch_counter_) {
    return SourceWriteResult::kUnknownStream;
  }
  if (stream->second.phase != SourceState::Phase::kOpen) {
    return SourceWriteResult::kNotReady;
  }
  if (items.empty()) {
    return SourceWriteResult::kTooLarge;
  }
  std::uint64_t bytes = 0;
  for (const auto& item : items) {
    bytes = SaturatingAdd(bytes, static_cast<std::uint64_t>(item.size()));
  }
  if (bytes == 0) {
    return SourceWriteResult::kTooLarge;
  }
  const std::uint64_t messages = static_cast<std::uint64_t>(items.size());
  if ((stream->second.maximum_chunk_messages > 0 &&
       messages > stream->second.maximum_chunk_messages) ||
      (stream->second.maximum_chunk_bytes > 0 &&
       bytes > stream->second.maximum_chunk_bytes)) {
    return SourceWriteResult::kTooLarge;
  }
  if (stream->second.message_credit < messages || stream->second.byte_credit < bytes) {
    return SourceWriteResult::kNoCredit;
  }
  if (work_queue_.size() >= config_.maximum_work_queue) {
    ++dropped_outbound_frames_;
    return SourceWriteResult::kQueueFull;
  }

  xgc::adapter::v1::WorkRequest frame;
  auto* data = frame.mutable_source_data();
  data->set_stream_id(stream_id);
  data->set_sequence(stream->second.sequence + 1);
  for (auto& item : items) {
    data->add_items(std::move(item));
  }
  if (!WorkFrameFitsLocked(&frame)) {
    return SourceWriteResult::kTooLarge;
  }
  if (!QueueWorkLocked(std::move(frame))) {
    return SourceWriteResult::kQueueFull;
  }
  stream->second.message_credit -= messages;
  stream->second.byte_credit -= bytes;
  ++stream->second.sequence;
  return SourceWriteResult::kAccepted;
}

bool Client::Impl::CloseSource(const std::string& stream_id,
                               const xgc::adapter::v1::AdapterError* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto stream = source_streams_.find(stream_id);
  if (stream == source_streams_.end() ||
      stream->second.phase != SourceState::Phase::kOpen ||
      stream->second.connection_epoch != connection_epoch_counter_ || session_failed_ ||
      stop_requested_ || (error != nullptr && !internal::IsTypedError(*error)) ||
      work_queue_.size() >= config_.maximum_work_queue) {
    if (work_queue_.size() >= config_.maximum_work_queue) {
      ++dropped_outbound_frames_;
    }
    return false;
  }
  xgc::adapter::v1::WorkRequest frame;
  auto* close = frame.mutable_source_close();
  close->set_stream_id(stream_id);
  close->set_final_sequence(stream->second.sequence);
  if (error != nullptr) {
    *close->mutable_error() = *error;
  }
  if (!CommitSourceTerminalLocked(stream_id, stream->second, std::move(frame))) {
    return false;
  }
  source_streams_.erase(stream);
  return true;
}

void Client::Impl::GrantSourceCredit(const xgc::adapter::v1::SourceCredit& credit,
                                     std::uint64_t frame_sequence) {
  std::string protocol_error;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto stream = source_streams_.find(credit.stream_id());
    if (credit.stream_id().empty() || !credit.has_grant() ||
        credit.grant().messages() == 0 || credit.grant().bytes() == 0) {
      protocol_error =
          "SourceCredit grant must include positive message and byte credit";
    } else if (stream == source_streams_.end()) {
      const auto closed = closed_sources_.find(credit.stream_id());
      if (closed == closed_sources_.end() ||
          closed->second.connection_epoch != connection_epoch_counter_) {
        protocol_error = "SourceCredit references an unknown source stream";
      } else if (credit.acknowledged_sequence() <
                     closed->second.acknowledged_sequence ||
                 credit.acknowledged_sequence() > closed->second.final_sequence) {
        protocol_error = "late SourceCredit conflicts with the closed source";
      } else {
        closed->second.acknowledged_sequence = credit.acknowledged_sequence();
      }
    } else if (stream->second.phase != SourceState::Phase::kOpen ||
               stream->second.connection_epoch != connection_epoch_counter_) {
      protocol_error = "SourceCredit references an unopened source stream";
    } else if (credit.acknowledged_sequence() < stream->second.acknowledged_sequence ||
               credit.acknowledged_sequence() > stream->second.sequence) {
      protocol_error = "SourceCredit acknowledged_sequence is invalid";
    } else if (AddOverflows(stream->second.message_credit, credit.grant().messages()) ||
               AddOverflows(stream->second.byte_credit, credit.grant().bytes())) {
      protocol_error = "SourceCredit would overflow the available credit";
    } else {
      stream->second.acknowledged_sequence = credit.acknowledged_sequence();
      stream->second.message_credit += credit.grant().messages();
      stream->second.byte_credit += credit.grant().bytes();
      condition_.notify_all();
    }
  }
  if (!protocol_error.empty()) {
    QueueWorkProtocolError(frame_sequence, protocol_error);
  }
}

void Client::Impl::HandleTerminalAcknowledgement(
    const xgc::adapter::v1::TerminalAcknowledgement& acknowledgement,
    std::uint64_t frame_sequence) {
  std::string protocol_error;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!internal::IsCanonicalSha256(acknowledgement.terminal_digest())) {
      protocol_error = "TerminalAcknowledgement terminal_digest is non-canonical";
    } else if (acknowledgement.identity_case() ==
               xgc::adapter::v1::TerminalAcknowledgement::kWorkId) {
      const std::string& work_id = acknowledgement.work_id();
      if (work_id.empty()) {
        protocol_error = "TerminalAcknowledgement work_id must not be empty";
      } else if (work_tokens_.find(work_id) != work_tokens_.end() ||
                 source_streams_.find(work_id) != source_streams_.end()) {
        protocol_error = "TerminalAcknowledgement references an active identity";
      } else if (closed_sources_.find(work_id) != closed_sources_.end()) {
        protocol_error = "TerminalAcknowledgement used work_id for a source terminal";
      } else {
        const auto replay = replay_.find(work_id);
        if (replay != replay_.end()) {
          if (replay->second.terminal_digest != acknowledgement.terminal_digest()) {
            protocol_error =
                "TerminalAcknowledgement digest conflicts with retained Work";
          } else {
            terminal_replay_bytes_ -= replay->second.accounted_bytes;
            replay_.erase(replay);
          }
        }
        // An acknowledgement for an already released terminal is an
        // idempotent no-op. Active identity checks above still prevent a late
        // duplicate from releasing newly accepted Work with the same ID.
      }
    } else if (acknowledgement.identity_case() ==
               xgc::adapter::v1::TerminalAcknowledgement::kStreamId) {
      const std::string& stream_id = acknowledgement.stream_id();
      if (stream_id.empty()) {
        protocol_error = "TerminalAcknowledgement stream_id must not be empty";
      } else if (source_streams_.find(stream_id) != source_streams_.end() ||
                 work_tokens_.find(stream_id) != work_tokens_.end()) {
        protocol_error = "TerminalAcknowledgement references an active identity";
      } else if (replay_.find(stream_id) != replay_.end()) {
        protocol_error = "TerminalAcknowledgement used stream_id for a Work terminal";
      } else {
        const auto closed = closed_sources_.find(stream_id);
        if (closed != closed_sources_.end()) {
          if (closed->second.terminal_digest.empty() ||
              closed->second.terminal_digest != acknowledgement.terminal_digest()) {
            protocol_error =
                "TerminalAcknowledgement digest conflicts with retained source";
          } else {
            if (closed->second.accounted_bytes <= source_state_bytes_) {
              source_state_bytes_ -= closed->second.accounted_bytes;
            } else {
              source_state_bytes_ = 0;
            }
            closed_sources_.erase(closed);
          }
        }
      }
    } else {
      protocol_error = "TerminalAcknowledgement must identify exactly one terminal";
    }
    condition_.notify_all();
  }
  if (!protocol_error.empty()) {
    QueueWorkProtocolError(frame_sequence, protocol_error);
  }
}

void Client::Impl::HandleCancellation(
    const xgc::adapter::v1::WorkCancellation& cancellation,
    std::uint64_t frame_sequence) {
  const std::string reason = internal::IsCanonicalText(cancellation.reason())
                                 ? cancellation.reason()
                                 : std::string("Host cancelled Work");
  const xgc::adapter::v1::AdapterError error =
      MakeError(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "peer-cancelled", reason);
  SourceState closed_source;
  bool notify_source = false;
  bool fail_pair = false;
  std::string protocol_error;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (cancellation.identity_case() ==
        xgc::adapter::v1::WorkCancellation::IDENTITY_NOT_SET) {
      protocol_error =
          "WorkCancellation must identify exactly one work_id or stream_id";
    } else if (cancellation.identity_case() ==
               xgc::adapter::v1::WorkCancellation::kWorkId) {
      if (cancellation.work_id().empty()) {
        protocol_error = "WorkCancellation work_id must not be empty";
      }
      const auto token = work_tokens_.find(cancellation.work_id());
      if (protocol_error.empty() && token != work_tokens_.end()) {
        token->second.cancellation->requested.store(true);
      }
    } else if (cancellation.stream_id().empty()) {
      protocol_error = "WorkCancellation stream_id must not be empty";
    } else {
      const auto stream = source_streams_.find(cancellation.stream_id());
      if (stream == source_streams_.end()) {
        const auto closed = closed_sources_.find(cancellation.stream_id());
        if (closed == closed_sources_.end() ||
            closed->second.connection_epoch != connection_epoch_counter_) {
          protocol_error = "WorkCancellation references an unknown source stream";
        }
      } else if (stream->second.connection_epoch != connection_epoch_counter_) {
        protocol_error = "WorkCancellation references a stale source stream";
      } else {
        stream->second.cancellation->requested.store(true);
        if (stream->second.phase == SourceState::Phase::kOpen) {
          if (work_queue_.size() >= config_.maximum_work_queue) {
            ++dropped_outbound_frames_;
            fail_pair = true;
          } else {
            xgc::adapter::v1::WorkRequest frame;
            auto* close = frame.mutable_source_close();
            close->set_stream_id(cancellation.stream_id());
            close->set_final_sequence(stream->second.sequence);
            *close->mutable_error() = error;
            if (!CommitSourceTerminalLocked(cancellation.stream_id(), stream->second,
                                            std::move(frame))) {
              fail_pair = true;
            } else {
              closed_source = stream->second;
              source_streams_.erase(stream);
              notify_source = true;
            }
          }
        }
      }
    }
  }
  if (!protocol_error.empty()) {
    QueueWorkProtocolError(frame_sequence, protocol_error);
  } else if (fail_pair) {
    SignalCurrentSessionFailure(
        "bounded Work queue could not acknowledge source cancellation");
  } else if (notify_source) {
    NotifySourceClosed(closed_source, error);
  }
}

xgc::adapter::v1::WorkRequest Client::Impl::SourceOpenRejected(
    const std::string& stream_id, xgc::adapter::v1::AdapterError error) const {
  xgc::adapter::v1::WorkRequest frame;
  auto* result = frame.mutable_source_open_result();
  result->set_stream_id(stream_id);
  result->set_accepted(false);
  *result->mutable_error() = std::move(error);
  return frame;
}

void Client::Impl::NotifySourceClosed(
    const SourceState& source, const xgc::adapter::v1::AdapterError& error) const {
  const auto& callbacks = config_.capabilities()[source.binding_index].callbacks;
  if (callbacks.source_closed) {
    SafeSourceClosed(callbacks, source.request, error);
  }
}

void Client::Impl::RememberClosedSourceLocked(
    const std::string& stream_id, const SourceState& source,
    xgc::adapter::v1::WorkRequest terminal_frame, std::string terminal_digest) {
  ClosedSourceState closed;
  closed.connection_epoch = source.connection_epoch;
  closed.final_sequence = source.sequence;
  closed.acknowledged_sequence = source.acknowledged_sequence;
  closed.fingerprint = source.request.context().request_digest();
  closed.terminal_digest = std::move(terminal_digest);
  closed.terminal_frame = std::move(terminal_frame);
  closed.accounted_bytes = source.accounted_bytes;
  closed_sources_[stream_id] = closed;
}

bool Client::Impl::CommitSourceTerminalLocked(
    const std::string& stream_id, const SourceState& source,
    xgc::adapter::v1::WorkRequest terminal_frame) {
  if (stream_id.empty() ||
      (terminal_frame.has_source_open_result() &&
       terminal_frame.source_open_result().stream_id() != stream_id) ||
      (terminal_frame.has_source_close() &&
       terminal_frame.source_close().stream_id() != stream_id) ||
      (!terminal_frame.has_source_open_result() &&
       !terminal_frame.has_source_close())) {
    return false;
  }
  std::string terminal_digest;
  std::string digest_error;
  const bool digested =
      terminal_frame.has_source_open_result()
          ? internal::TerminalDigest(terminal_frame.source_open_result(),
                                     &terminal_digest, &digest_error)
          : internal::TerminalDigest(terminal_frame.source_close(), &terminal_digest,
                                     &digest_error);
  if (!digested || !WorkFrameFitsLocked(&terminal_frame) ||
      !QueueWorkLocked(terminal_frame)) {
    return false;
  }
  RememberClosedSourceLocked(stream_id, source, std::move(terminal_frame),
                             std::move(terminal_digest));
  return true;
}

bool Client::Impl::WorkFrameFitsLocked(xgc::adapter::v1::WorkRequest* frame) const {
  return maximum_work_frame_bytes_ != 0 &&
         WorkFrameBytesLocked(frame) <= maximum_work_frame_bytes_;
}

void Client::Impl::QueueWorkProtocolError(std::uint64_t rejected_sequence,
                                          const std::string& message) {
  xgc::adapter::v1::WorkRequest frame;
  auto* protocol_error = frame.mutable_protocol_error();
  protocol_error->set_rejected_frame_sequence(rejected_sequence);
  *protocol_error->mutable_error() =
      MakeError(xgc::adapter::v1::ERROR_CLASS_REJECTED, "invalid-work-frame", message);
  QueueWorkOrFail(std::move(frame));
}

void Client::Impl::BeginDrain() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == ClientState::kReady || state_ == ClientState::kApplyingSpec) {
    state_ = ClientState::kDraining;
  }
  accepting_work_ = false;
  drain_requested_ = true;
  MaybeQueueDrainStatusLocked();
  condition_.notify_all();
}

void Client::Impl::MaybeQueueDrainStatusLocked() {
  if (!drain_requested_ || drain_reported_ || in_flight_work_ != 0 ||
      active_dispatch_jobs_ != 0 || !dispatch_queue_.empty() ||
      control_queue_.size() >= config_.maximum_control_queue) {
    return;
  }
  xgc::adapter::v1::ControlRequest frame;
  frame.mutable_drain_status()->set_drained(true);
  frame.mutable_drain_status()->set_in_flight_work(0);
  if (QueueControlLocked(std::move(frame))) {
    drain_reported_ = true;
  } else {
    session_failed_ = true;
    accepting_work_ = false;
    state_ = ClientState::kReconnecting;
    last_error_ = "Control byte budget could not report drain completion";
    CancelAllWorkLocked();
    condition_.notify_all();
  }
}

void Client::Impl::RequestRemoteStop(const xgc::adapter::v1::StopRequest& request) {
  if (callbacks_.stop_requested) {
    try {
      callbacks_.stop_requested(request);
    } catch (const std::exception& exception) {
      Log(LogLevel::kError,
          "stop_requested callback threw: " + std::string(exception.what()));
    } catch (...) {
      Log(LogLevel::kError, "stop_requested callback threw an unknown exception");
    }
  }

  std::shared_ptr<grpc::ClientContext> control;
  std::shared_ptr<grpc::ClientContext> work;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = true;
    accepting_work_ = false;
    state_ = ClientState::kStopping;
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
}

}  // namespace adapter_runtime
}  // namespace xgc2
