#include <algorithm>
#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

namespace {

bool FitsByteBudget(std::size_t used, std::size_t added, std::size_t maximum) {
  return added <= maximum && used <= maximum - added;
}

}  // namespace

constexpr std::size_t Client::Impl::kNoBinding;

void Client::Impl::StampHeaderLocked(xgc::adapter::v1::SessionHeader* header,
                                     const SessionFence& fence,
                                     std::uint64_t connection_epoch,
                                     std::uint64_t sequence) const {
  header->set_instance_id(config_.registration().instance_id());
  header->set_session_id(fence.session_id);
  header->set_process_generation(config_.registration().process_generation());
  header->set_session_generation(fence.session_generation);
  header->set_runtime_epoch(fence.runtime_epoch);
  header->set_connection_epoch(connection_epoch);
  header->set_frame_sequence(sequence);
}

bool Client::Impl::ValidateControlHeader(const xgc::adapter::v1::SessionHeader& header,
                                         const SessionFence& fence,
                                         std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ValidateHeaderLocked(header, fence, fence.connection_epoch,
                              &control_in_sequence_, error);
}

bool Client::Impl::ValidateWorkHeader(const xgc::adapter::v1::SessionHeader& header,
                                      const SessionFence& fence, std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ValidateHeaderLocked(header, fence, fence.connection_epoch, &work_in_sequence_,
                              error);
}

bool Client::Impl::ValidateHeaderLocked(const xgc::adapter::v1::SessionHeader& header,
                                        const SessionFence& fence,
                                        std::uint64_t connection_epoch,
                                        std::uint64_t* previous_sequence,
                                        std::string* error) const {
  if (header.instance_id() != config_.registration().instance_id() ||
      header.session_id() != fence.session_id ||
      header.process_generation() != config_.registration().process_generation() ||
      header.session_generation() != fence.session_generation ||
      header.runtime_epoch() != fence.runtime_epoch ||
      header.connection_epoch() != connection_epoch) {
    return Fail(error, "identity/generation/epoch mismatch");
  }
  if (header.frame_sequence() == 0 || header.frame_sequence() <= *previous_sequence) {
    return Fail(error, "frame_sequence is zero or non-monotonic");
  }
  *previous_sequence = header.frame_sequence();
  return true;
}

bool Client::Impl::QueueControl(xgc::adapter::v1::ControlRequest request) {
  std::lock_guard<std::mutex> lock(mutex_);
  return QueueControlLocked(std::move(request));
}

bool Client::Impl::QueueControlLocked(xgc::adapter::v1::ControlRequest request) {
  const std::size_t frame_bytes = ControlFrameBytesLocked(&request);
  if (session_id_.empty() || session_failed_ || stop_requested_ ||
      control_queue_.size() >= config_.maximum_control_queue ||
      frame_bytes > maximum_control_frame_bytes_ ||
      !FitsByteBudget(control_queue_bytes_, frame_bytes,
                      config_.maximum_control_queue_bytes)) {
    if (control_queue_.size() >= config_.maximum_control_queue ||
        !FitsByteBudget(control_queue_bytes_, frame_bytes,
                        config_.maximum_control_queue_bytes)) {
      ++dropped_outbound_frames_;
    }
    return false;
  }
  QueuedControlFrame queued;
  queued.request = std::move(request);
  queued.accounted_bytes = frame_bytes;
  control_queue_.push_back(std::move(queued));
  control_queue_bytes_ += frame_bytes;
  condition_.notify_all();
  return true;
}

bool Client::Impl::QueueWork(xgc::adapter::v1::WorkRequest request) {
  std::lock_guard<std::mutex> lock(mutex_);
  return QueueWorkLocked(std::move(request));
}

bool Client::Impl::QueueWorkLocked(xgc::adapter::v1::WorkRequest request) {
  const std::size_t frame_bytes = WorkFrameBytesLocked(&request);
  if (session_id_.empty() || session_failed_ || stop_requested_ ||
      work_queue_.size() >= config_.maximum_work_queue ||
      frame_bytes > maximum_work_frame_bytes_ ||
      !FitsByteBudget(work_queue_bytes_, frame_bytes,
                      config_.maximum_work_queue_bytes)) {
    if (work_queue_.size() >= config_.maximum_work_queue ||
        !FitsByteBudget(work_queue_bytes_, frame_bytes,
                        config_.maximum_work_queue_bytes)) {
      ++dropped_outbound_frames_;
    }
    return false;
  }
  QueuedWorkFrame queued;
  queued.request = std::move(request);
  queued.accounted_bytes = frame_bytes;
  work_queue_.push_back(std::move(queued));
  work_queue_bytes_ += frame_bytes;
  condition_.notify_all();
  return true;
}

std::size_t Client::Impl::ControlFrameBytesLocked(
    xgc::adapter::v1::ControlRequest* frame) const {
  SessionFence fence;
  fence.session_id = session_id_;
  fence.session_generation = session_generation_;
  fence.runtime_epoch = runtime_epoch_;
  StampHeaderLocked(frame->mutable_header(), fence, connection_epoch_counter_,
                    std::numeric_limits<std::uint64_t>::max());
  const std::size_t bytes = frame->ByteSizeLong();
  frame->clear_header();
  return bytes;
}

std::size_t Client::Impl::WorkFrameBytesLocked(
    xgc::adapter::v1::WorkRequest* frame) const {
  SessionFence fence;
  fence.session_id = session_id_;
  fence.session_generation = session_generation_;
  fence.runtime_epoch = runtime_epoch_;
  StampHeaderLocked(frame->mutable_header(), fence, connection_epoch_counter_,
                    std::numeric_limits<std::uint64_t>::max());
  const std::size_t bytes = frame->ByteSizeLong();
  frame->clear_header();
  return bytes;
}

void Client::Impl::QueueWorkOrFail(xgc::adapter::v1::WorkRequest request) {
  if (!QueueWork(std::move(request))) {
    SignalCurrentSessionFailure("bounded outbound Work queue overflowed");
  }
}

Client::Impl::TerminalCommitResult Client::Impl::CommitTerminalFrame(
    const DispatchJob& job, xgc::adapter::v1::WorkRequest frame) {
  std::lock_guard<std::mutex> lock(mutex_);
  const std::size_t frame_bytes = WorkFrameBytesLocked(&frame);
  if (frame_bytes > maximum_work_frame_bytes_) {
    return TerminalCommitResult::kTooLarge;
  }
  if (session_id_.empty() || session_failed_ || stop_requested_ ||
      work_queue_.size() >= config_.maximum_work_queue ||
      !FitsByteBudget(work_queue_bytes_, frame_bytes,
                      config_.maximum_work_queue_bytes)) {
    if (work_queue_.size() >= config_.maximum_work_queue ||
        !FitsByteBudget(work_queue_bytes_, frame_bytes,
                        config_.maximum_work_queue_bytes)) {
      ++dropped_outbound_frames_;
    }
    return TerminalCommitResult::kUnavailable;
  }

  ReplayEntry entry;
  entry.kind = job.kind;
  entry.fingerprint = job.fingerprint;
  std::string digest_error;
  if (job.kind == DispatchKind::kUnary && frame.has_unary_response()) {
    entry.unary = frame.unary_response();
    if (!internal::TerminalDigest(entry.unary, &entry.terminal_digest, &digest_error)) {
      return TerminalCommitResult::kUnavailable;
    }
  } else if (job.kind == DispatchKind::kOperation && frame.has_operation_event()) {
    entry.operation = frame.operation_event();
    if (!internal::TerminalDigest(entry.operation, &entry.terminal_digest,
                                  &digest_error)) {
      return TerminalCommitResult::kUnavailable;
    }
  } else {
    return TerminalCommitResult::kUnavailable;
  }
  entry.accounted_bytes =
      frame_bytes + entry.fingerprint.size() + entry.terminal_digest.size();
  const auto previous = replay_.find(job.work_id);
  if (previous != replay_.end()) {
    return TerminalCommitResult::kUnavailable;
  }
  const auto token = work_tokens_.find(job.work_id);
  const bool converts_reservation = token != work_tokens_.end() &&
                                    token->second.cancellation == job.cancellation &&
                                    token->second.terminal_reservation_bytes != 0;
  const std::size_t reservation =
      converts_reservation ? token->second.terminal_reservation_bytes : 0;
  if (reservation > terminal_replay_bytes_) {
    return TerminalCommitResult::kUnavailable;
  }
  const std::size_t replay_without_reservation = terminal_replay_bytes_ - reservation;
  const std::size_t retained_identities = replay_.size() + work_tokens_.size();
  if ((!converts_reservation &&
       retained_identities >= config_.maximum_terminal_replay) ||
      !FitsByteBudget(replay_without_reservation, entry.accounted_bytes,
                      config_.maximum_terminal_replay_bytes)) {
    return TerminalCommitResult::kUnavailable;
  }
  if (converts_reservation) {
    terminal_replay_bytes_ = replay_without_reservation;
    work_tokens_.erase(token);
  }
  RememberReplayLocked(job.work_id, std::move(entry));
  QueuedWorkFrame queued;
  queued.request = std::move(frame);
  queued.accounted_bytes = frame_bytes;
  work_queue_.push_back(std::move(queued));
  work_queue_bytes_ += frame_bytes;
  condition_.notify_all();
  return TerminalCommitResult::kCommitted;
}

bool Client::Impl::ReserveTerminalReplayLocked(DispatchJob* job) {
  if (job == nullptr || job->work_id.empty() ||
      replay_.size() + work_tokens_.size() >= config_.maximum_terminal_replay) {
    return false;
  }
  const std::size_t metadata_bytes =
      job->fingerprint.size() + internal::kCanonicalSha256TextBytes;
  if (maximum_work_frame_bytes_ >
      std::numeric_limits<std::size_t>::max() - metadata_bytes) {
    return false;
  }
  const std::size_t reservation = maximum_work_frame_bytes_ + metadata_bytes;
  if (!FitsByteBudget(terminal_replay_bytes_, reservation,
                      config_.maximum_terminal_replay_bytes)) {
    return false;
  }
  job->terminal_reservation_bytes = reservation;
  terminal_replay_bytes_ += reservation;
  return true;
}

void Client::Impl::ReleaseWorkTokenLocked(const std::string& work_id) {
  const auto token = work_tokens_.find(work_id);
  if (token == work_tokens_.end()) {
    return;
  }
  if (token->second.terminal_reservation_bytes <= terminal_replay_bytes_) {
    terminal_replay_bytes_ -= token->second.terminal_reservation_bytes;
  } else {
    terminal_replay_bytes_ = 0;
  }
  work_tokens_.erase(token);
}

void Client::Impl::RememberReplayLocked(const std::string& work_id, ReplayEntry entry) {
  const auto previous = replay_.find(work_id);
  if (previous != replay_.end()) {
    terminal_replay_bytes_ -= previous->second.accounted_bytes;
  }
  terminal_replay_bytes_ += entry.accounted_bytes;
  replay_[work_id] = std::move(entry);
}

Client::Impl::ActiveEndpoint Client::Impl::FindActiveEndpointLocked(
    const std::string& capability_id, const std::string& endpoint_id) const {
  ActiveEndpoint result;
  const auto active = active_capabilities_.find(capability_id);
  if (active == active_capabilities_.end()) {
    return result;
  }
  if (std::find(active->second.grant.enabled_endpoint_ids().begin(),
                active->second.grant.enabled_endpoint_ids().end(),
                endpoint_id) == active->second.grant.enabled_endpoint_ids().end()) {
    return result;
  }
  const auto& contract = config_.capabilities()[active->second.binding_index].contract;
  const auto* endpoint = FindEndpoint(contract, endpoint_id);
  if (endpoint == nullptr) {
    return result;
  }
  result.valid = true;
  result.binding_index = active->second.binding_index;
  result.grant = &active->second.grant;
  result.endpoint = endpoint;
  return result;
}

std::size_t Client::Impl::CountSourceStreamsLocked(
    const std::string& capability_id, const std::string& endpoint_id) const {
  std::size_t count = 0;
  for (const auto& stream : source_streams_) {
    if (stream.second.request.context().capability_id() == capability_id &&
        stream.second.request.context().endpoint_id() == endpoint_id) {
      ++count;
    }
  }
  return count;
}

}  // namespace adapter_runtime
}  // namespace xgc2
