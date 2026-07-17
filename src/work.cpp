#include <thread>
#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

void Client::Impl::WorkStreamLoop(SessionFence fence) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto stream = stub_->Work(context.get());
  if (!stream) {
    SignalSessionFailure(fence.session_id, "Work stream could not be opened");
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ != fence.session_id || stop_requested_) {
      context->TryCancel();
    } else {
      active_work_context_ = context;
    }
  }
  std::thread writer(&Impl::WorkWriterLoop, this, stream.get(), fence);

  xgc::adapter::v1::WorkResponse response;
  while (!StopRequested() && stream->Read(&response)) {
    if (response.ByteSizeLong() > fence.maximum_work_frame_bytes) {
      SignalTerminalSessionFailure(fence.session_id,
                                   "Host Work frame exceeds the negotiated limit");
      break;
    }
    std::string header_error;
    if (!ValidateWorkHeader(response.header(), fence, &header_error)) {
      SignalTerminalSessionFailure(fence.session_id,
                                   "invalid Work fence: " + header_error);
      break;
    }
    if (!HandleWorkResponse(fence, response)) {
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
    if (active_work_context_ == context) {
      active_work_context_.reset();
    }
  }
  if (!StopRequested() && IsTerminalSessionStatus(status)) {
    SignalTerminalSessionFailure(
        fence.session_id,
        "Work attachment was rejected: " + (status.error_message().empty()
                                                ? std::string("terminal gRPC status")
                                                : status.error_message()));
  } else if (!StopRequested() && CurrentSessionIs(fence.session_id)) {
    SignalSessionFailure(fence.session_id,
                         status.ok() ? "Work stream closed"
                                     : "Work stream failed: " + status.error_message());
  }
}

void Client::Impl::WorkWriterLoop(WorkStream* stream, SessionFence fence) {
  xgc::adapter::v1::WorkRequest attach;
  bool attach_allowed = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    attach_allowed = work_attach_allowed_ &&
                     work_attach_connection_epoch_ == fence.connection_epoch &&
                     session_id_ == fence.session_id && !session_failed_ &&
                     !stop_requested_;
    if (attach_allowed) {
      attach.mutable_attach()->set_applied_spec_revision(spec_revision_);
      attach.mutable_attach()->set_applied_spec_digest(spec_digest_);
      StampHeaderLocked(attach.mutable_header(), fence, fence.connection_epoch,
                        ++work_out_sequence_);
    }
  }
  if (!attach_allowed) {
    SignalSessionFailure(
        fence.session_id,
        "WorkAttach gate was lost before the paired Work stream started");
    return;
  }
  if ((fence.maximum_work_frame_bytes > 0 &&
       attach.ByteSizeLong() > fence.maximum_work_frame_bytes) ||
      !stream->Write(attach)) {
    SignalSessionFailure(fence.session_id, "mandatory WorkAttach could not be written");
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ == fence.session_id && !session_failed_ && !stop_requested_ &&
        work_attach_allowed_ &&
        work_attach_connection_epoch_ == fence.connection_epoch) {
      accepting_work_ = true;
      state_ = ClientState::kReady;
      ready_callbacks_complete_ = true;
      pair_ready_ = true;
    }
  }
  condition_.notify_all();

  while (true) {
    xgc::adapter::v1::WorkRequest request;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this, &fence] {
        return stop_requested_ || session_failed_ || session_id_ != fence.session_id ||
               !work_queue_.empty();
      });
      if (stop_requested_ || session_failed_ || session_id_ != fence.session_id) {
        return;
      }
      work_queue_bytes_ -= work_queue_.front().accounted_bytes;
      request = std::move(work_queue_.front().request);
      work_queue_.pop_front();
      StampHeaderLocked(request.mutable_header(), fence, fence.connection_epoch,
                        ++work_out_sequence_);
    }
    if ((fence.maximum_work_frame_bytes > 0 &&
         request.ByteSizeLong() > fence.maximum_work_frame_bytes) ||
        !stream->Write(request)) {
      SignalSessionFailure(fence.session_id,
                           "Work outbound frame failed or exceeded limit");
      return;
    }
  }
}

bool Client::Impl::HandleWorkResponse(const SessionFence& fence,
                                      const xgc::adapter::v1::WorkResponse& response) {
  switch (response.frame_case()) {
    case xgc::adapter::v1::WorkResponse::kUnaryRequest:
      EnqueueUnary(response.unary_request(), response.header().frame_sequence());
      return true;
    case xgc::adapter::v1::WorkResponse::kOperationRequest:
      EnqueueOperation(response.operation_request(),
                       response.header().frame_sequence());
      return true;
    case xgc::adapter::v1::WorkResponse::kSourceOpenRequest:
      EnqueueSourceOpen(response.source_open_request(),
                        response.header().frame_sequence());
      return true;
    case xgc::adapter::v1::WorkResponse::kSourceCredit:
      GrantSourceCredit(response.source_credit(), response.header().frame_sequence());
      return true;
    case xgc::adapter::v1::WorkResponse::kCancellation:
      HandleCancellation(response.cancellation(), response.header().frame_sequence());
      return true;
    case xgc::adapter::v1::WorkResponse::kTerminalAcknowledgement:
      HandleTerminalAcknowledgement(response.terminal_acknowledgement(),
                                    response.header().frame_sequence());
      return true;
    case xgc::adapter::v1::WorkResponse::kProtocolError:
      SignalTerminalSessionFailure(fence.session_id,
                                   "Host reported Work protocol error: " +
                                       response.protocol_error().error().message());
      return false;
    case xgc::adapter::v1::WorkResponse::FRAME_NOT_SET:
      QueueWorkProtocolError(response.header().frame_sequence(),
                             "Host sent an empty Work frame");
      return true;
  }
  return false;
}

}  // namespace adapter_runtime
}  // namespace xgc2
