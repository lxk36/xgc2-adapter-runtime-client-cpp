#include <chrono>
#include <thread>
#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

namespace {

bool IsTerminalSpecFailure(const std::string& code) {
  return code == "invalid-instance-spec" || code == "spec-content-mismatch" ||
         code == "initial-spec-mismatch" || code == "ready-callback-failed";
}

}  // namespace

void Client::Impl::ControlStreamLoop(SessionFence fence) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto stream = stub_->Control(context.get());
  if (!stream) {
    SignalSessionFailure(fence.session_id, "Control stream could not be opened");
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ != fence.session_id || stop_requested_) {
      context->TryCancel();
    } else {
      active_control_context_ = context;
    }
  }
  std::thread writer(&Impl::ControlWriterLoop, this, stream.get(), fence);

  xgc::adapter::v1::ControlResponse response;
  while (!StopRequested() && stream->Read(&response)) {
    if (response.ByteSizeLong() > fence.maximum_control_frame_bytes) {
      SignalTerminalSessionFailure(fence.session_id,
                                   "Host Control frame exceeds the negotiated limit");
      break;
    }
    std::string header_error;
    if (!ValidateControlHeader(response.header(), fence, &header_error)) {
      SignalTerminalSessionFailure(fence.session_id,
                                   "invalid Control fence: " + header_error);
      break;
    }
    if (!HandleControlResponse(fence, response)) {
      break;
    }
  }
  context->TryCancel();
  condition_.notify_all();
  if (writer.joinable()) {
    writer.join();
  }
  stream->WritesDone();
  const grpc::Status status = stream->Finish();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_control_context_ == context) {
      active_control_context_.reset();
    }
  }
  if (!StopRequested() && IsTerminalSessionStatus(status)) {
    SignalTerminalSessionFailure(
        fence.session_id,
        "Control attachment was rejected: " + (status.error_message().empty()
                                                   ? std::string("terminal gRPC status")
                                                   : status.error_message()));
  } else if (!StopRequested() && CurrentSessionIs(fence.session_id)) {
    SignalSessionFailure(fence.session_id, status.ok() ? "Control stream closed"
                                                       : "Control stream failed: " +
                                                             status.error_message());
  }
}

void Client::Impl::ControlWriterLoop(ControlStream* stream, SessionFence fence) {
  auto next_heartbeat = std::chrono::steady_clock::now();
  while (true) {
    xgc::adapter::v1::ControlRequest request;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait_until(lock, next_heartbeat, [this, &fence] {
        return stop_requested_ || session_failed_ || session_id_ != fence.session_id ||
               !control_queue_.empty();
      });
      if (stop_requested_ || session_failed_ || session_id_ != fence.session_id) {
        return;
      }
      const auto now = std::chrono::steady_clock::now();
      if (now >= next_heartbeat) {
        *request.mutable_heartbeat() = BuildHeartbeatLocked(0);
        next_heartbeat = now + std::chrono::milliseconds(fence.heartbeat_interval_ms);
      } else if (!control_queue_.empty()) {
        control_queue_bytes_ -= control_queue_.front().accounted_bytes;
        request = std::move(control_queue_.front().request);
        control_queue_.pop_front();
      } else {
        continue;
      }
      StampHeaderLocked(request.mutable_header(), fence, fence.connection_epoch,
                        ++control_out_sequence_);
    }
    const bool is_spec_result = request.has_apply_instance_spec_result();
    const bool spec_applied =
        is_spec_result && request.apply_instance_spec_result().applied();
    const std::string spec_failure =
        is_spec_result && request.apply_instance_spec_result().has_error()
            ? request.apply_instance_spec_result().error().message()
            : std::string();
    const std::string spec_failure_code =
        is_spec_result && request.apply_instance_spec_result().has_error()
            ? request.apply_instance_spec_result().error().code()
            : std::string();
    if ((fence.maximum_control_frame_bytes > 0 &&
         request.ByteSizeLong() > fence.maximum_control_frame_bytes) ||
        !stream->Write(request)) {
      SignalSessionFailure(fence.session_id,
                           "Control outbound frame failed or exceeded limit");
      return;
    }
    if (spec_applied) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (session_id_ == fence.session_id && !session_failed_ && !stop_requested_ &&
            spec_revision_ != 0 && !spec_digest_.empty()) {
          work_attach_allowed_ = true;
          work_attach_connection_epoch_ = fence.connection_epoch;
        }
      }
      condition_.notify_all();
    } else if (is_spec_result) {
      const std::string failure =
          "exact instance spec was not applied" +
          (spec_failure.empty() ? std::string() : ": " + spec_failure);
      if (IsTerminalSpecFailure(spec_failure_code)) {
        SignalTerminalSessionFailure(fence.session_id, failure);
      } else {
        SignalSessionFailure(fence.session_id, failure);
      }
      return;
    }
  }
}

bool Client::Impl::HandleControlResponse(
    const SessionFence& fence, const xgc::adapter::v1::ControlResponse& response) {
  switch (response.frame_case()) {
    case xgc::adapter::v1::ControlResponse::kInstanceSpec: {
      xgc::adapter::v1::ApplyInstanceSpecResult result =
          ApplyInstanceSpec(fence.session_id, response.instance_spec());
      xgc::adapter::v1::ControlRequest request;
      *request.mutable_apply_instance_spec_result() = std::move(result);
      if (!QueueControl(std::move(request))) {
        SignalSessionFailure(fence.session_id, "Control queue rejected spec result");
        return false;
      }
      return true;
    }
    case xgc::adapter::v1::ControlResponse::kPing: {
      xgc::adapter::v1::ControlRequest request;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        *request.mutable_heartbeat() = BuildHeartbeatLocked(response.ping().nonce());
      }
      if (!QueueControl(std::move(request))) {
        SignalSessionFailure(fence.session_id, "Control queue rejected ping response");
        return false;
      }
      return true;
    }
    case xgc::adapter::v1::ControlResponse::kDrain:
      BeginDrain();
      return true;
    case xgc::adapter::v1::ControlResponse::kStop:
      RequestRemoteStop(response.stop());
      return false;
    case xgc::adapter::v1::ControlResponse::kProtocolError:
      SignalTerminalSessionFailure(fence.session_id,
                                   "Host reported Control protocol error: " +
                                       response.protocol_error().error().message());
      return false;
    case xgc::adapter::v1::ControlResponse::FRAME_NOT_SET:
      SignalSessionFailure(fence.session_id, "Host sent an empty Control frame");
      return false;
  }
  return false;
}

xgc::adapter::v1::Heartbeat Client::Impl::BuildHeartbeatLocked(
    std::uint64_t ping_nonce) const {
  xgc::adapter::v1::Heartbeat heartbeat;
  heartbeat.set_observed_unix_nanos(internal::UnixNanos());
  heartbeat.set_applied_spec_revision(spec_revision_);
  heartbeat.set_applied_spec_digest(spec_digest_);
  heartbeat.set_ping_nonce(ping_nonce);
  for (const auto& status : capability_statuses_) {
    *heartbeat.add_capabilities() = status;
  }
  return heartbeat;
}

}  // namespace adapter_runtime
}  // namespace xgc2
