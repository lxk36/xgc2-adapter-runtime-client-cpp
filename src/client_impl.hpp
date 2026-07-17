#pragma once

#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "internal.hpp"
#include "xgc/adapter/v1/adapter.grpc.pb.h"
#include "xgc2/adapter_runtime/client.hpp"

namespace xgc2 {
namespace adapter_runtime {

class Client::Impl {
 public:
  Impl(ClientConfig config, ClientCallbacks callbacks);
  ~Impl();

  bool Start(std::string* error);
  void Stop();
  SourceWriteResult PublishSource(const std::string& stream_id,
                                  std::vector<std::string> items);
  bool CloseSource(const std::string& stream_id,
                   const xgc::adapter::v1::AdapterError* error);
  SessionSnapshot session() const;

 private:
  using ControlStream = grpc::ClientReaderWriter<xgc::adapter::v1::ControlRequest,
                                                 xgc::adapter::v1::ControlResponse>;
  using WorkStream = grpc::ClientReaderWriter<xgc::adapter::v1::WorkRequest,
                                              xgc::adapter::v1::WorkResponse>;

  struct SessionFence {
    std::string host_id;
    std::string session_id;
    std::uint64_t session_generation = 0;
    std::uint64_t runtime_epoch = 0;
    std::uint32_t runtime_link_protocol_version = 0;
    std::uint32_t heartbeat_interval_ms = internal::kDefaultHeartbeatIntervalMs;
    std::uint32_t maximum_control_frame_bytes = 0;
    std::uint32_t maximum_work_frame_bytes = 0;
    std::uint64_t connection_epoch = 0;
  };

  struct ActiveCapability {
    std::size_t binding_index = 0;
    xgc::adapter::v1::EnabledCapability grant;
  };

  struct ActiveEndpoint {
    bool valid = false;
    std::size_t binding_index = 0;
    const xgc::adapter::v1::EnabledCapability* grant = nullptr;
    const xgc::adapter::v1::CapabilityEndpointContract* endpoint = nullptr;
  };

  enum class DispatchKind { kUnary, kOperation, kSourceOpen };

  enum class TerminalCommitResult { kCommitted, kTooLarge, kUnavailable };

  struct DispatchJob {
    DispatchKind kind = DispatchKind::kUnary;
    std::string work_id;
    std::string fingerprint;
    xgc::adapter::v1::UnaryRequest unary;
    xgc::adapter::v1::OperationRequest operation;
    xgc::adapter::v1::SourceOpenRequest source_open;
    std::uint64_t inbound_frame_sequence = 0;
    std::uint64_t connection_epoch = 0;
    std::size_t accounted_bytes = 0;
    std::size_t terminal_reservation_bytes = 0;
    std::shared_ptr<CancellationState> cancellation;
  };

  struct WorkTokenState {
    std::shared_ptr<CancellationState> cancellation;
    std::size_t terminal_reservation_bytes = 0;
  };

  struct ReplayEntry {
    DispatchKind kind = DispatchKind::kUnary;
    std::string fingerprint;
    xgc::adapter::v1::UnaryResponse unary;
    xgc::adapter::v1::OperationEvent operation;
    std::string terminal_digest;
    std::size_t accounted_bytes = 0;
  };

  struct QueuedControlFrame {
    xgc::adapter::v1::ControlRequest request;
    std::size_t accounted_bytes = 0;
  };

  struct QueuedWorkFrame {
    xgc::adapter::v1::WorkRequest request;
    std::size_t accounted_bytes = 0;
  };

  struct SourceState {
    enum class Phase { kOpening, kOpen };

    xgc::adapter::v1::SourceOpenRequest request;
    std::size_t binding_index = 0;
    std::shared_ptr<CancellationState> cancellation;
    Phase phase = Phase::kOpening;
    std::uint64_t connection_epoch = 0;
    std::uint64_t message_credit = 0;
    std::uint64_t byte_credit = 0;
    std::uint64_t sequence = 0;
    std::uint64_t acknowledged_sequence = 0;
    std::uint32_t maximum_chunk_bytes = 0;
    std::uint32_t maximum_chunk_messages = 0;
    std::size_t accounted_bytes = 0;
  };

  struct ClosedSourceState {
    std::uint64_t connection_epoch = 0;
    std::uint64_t final_sequence = 0;
    std::uint64_t acknowledged_sequence = 0;
    std::string fingerprint;
    std::string terminal_digest;
    xgc::adapter::v1::WorkRequest terminal_frame;
    std::size_t accounted_bytes = 0;
  };

  // Bootstrap and session supervision.
  bool ValidateConfig(std::string* error) const;
  void SupervisorLoop();
  bool Register(SessionFence* fence, std::string* error);
  void InstallSession(const SessionFence& fence);
  void PrepareConnectionPairLocked(const SessionFence& fence);
  void PreparePairReplacement();
  void ClearConnectionStateLocked();
  void ResetSessionLocked();
  void CancelActiveStreams();
  void SignalCurrentSessionFailure(const std::string& reason);
  void SignalSessionFailure(const std::string& expected_session,
                            const std::string& reason);
  void SignalTerminalSessionFailure(const std::string& expected_session,
                                    const std::string& reason);
  void EnterSessionLost(const std::string& reason);
  bool TerminalSessionFailureRequested() const;
  bool StopRequested() const;
  bool CurrentSessionIs(const std::string& session_id) const;
  void SetState(ClientState state);
  void RecordError(const std::string& error);
  void WaitFor(std::uint32_t milliseconds);
  std::uint32_t NextBackoff(std::uint32_t current) const;
  void Log(LogLevel level, const std::string& message) const noexcept;

  // Control plane and spec transaction.
  void ControlStreamLoop(SessionFence fence);
  void ControlWriterLoop(ControlStream* stream, SessionFence fence);
  bool HandleControlResponse(const SessionFence& fence,
                             const xgc::adapter::v1::ControlResponse& response);
  xgc::adapter::v1::ApplyInstanceSpecResult ApplyInstanceSpec(
      const std::string& expected_session,
      const xgc::adapter::v1::AdapterInstanceSpec& spec);
  bool ValidateInstanceSpec(const xgc::adapter::v1::AdapterInstanceSpec& spec,
                            std::vector<std::size_t>* binding_indices,
                            std::string* error) const;
  xgc::adapter::v1::Heartbeat BuildHeartbeatLocked(std::uint64_t ping_nonce) const;

  // Work transport and dispatch.
  void WorkStreamLoop(SessionFence fence);
  void WorkWriterLoop(WorkStream* stream, SessionFence fence);
  bool HandleWorkResponse(const SessionFence& fence,
                          const xgc::adapter::v1::WorkResponse& response);
  void EnqueueUnary(const xgc::adapter::v1::UnaryRequest& request,
                    std::uint64_t frame_sequence);
  void EnqueueOperation(const xgc::adapter::v1::OperationRequest& request,
                        std::uint64_t frame_sequence);
  void EnqueueSourceOpen(const xgc::adapter::v1::SourceOpenRequest& request,
                         std::uint64_t frame_sequence);
  void EnqueueDispatch(DispatchJob job);
  void DispatchLoop();
  bool ValidateWorkContextLocked(
      const xgc::adapter::v1::WorkContext& context, DispatchKind kind,
      std::size_t* binding_index,
      const xgc::adapter::v1::CapabilityEndpointContract** endpoint,
      std::string* error) const;
  void HandleUnary(const DispatchJob& job, std::size_t binding_index);
  void HandleOperation(const DispatchJob& job, std::size_t binding_index);
  void HandleSourceOpen(const DispatchJob& job);
  bool ValidateInput(const xgc::adapter::v1::CapabilityEndpointContract& endpoint,
                     const xgc::v1::Payload& input) const;
  void CompleteCancelledJob(const DispatchJob& job);
  void EraseWorkToken(const std::string& work_id);
  xgc::adapter::v1::WorkRequest RejectedDispatch(
      DispatchKind kind, const std::string& work_id, const std::string& code,
      const std::string& message,
      xgc::adapter::v1::ErrorClass error_class =
          xgc::adapter::v1::ERROR_CLASS_REJECTED) const;
  xgc::adapter::v1::WorkRequest OperationFrame(
      const std::string& work_id, xgc::adapter::v1::OperationPhase phase) const;

  std::string RequestFingerprint(const xgc::adapter::v1::WorkContext& context) const {
    return context.request_digest();
  }

  // Source streams, cancellation, drain, and stop.
  void GrantSourceCredit(const xgc::adapter::v1::SourceCredit& credit,
                         std::uint64_t frame_sequence);
  void HandleTerminalAcknowledgement(
      const xgc::adapter::v1::TerminalAcknowledgement& acknowledgement,
      std::uint64_t frame_sequence);
  void HandleCancellation(const xgc::adapter::v1::WorkCancellation& cancellation,
                          std::uint64_t frame_sequence);
  xgc::adapter::v1::WorkRequest SourceOpenRejected(
      const std::string& stream_id, xgc::adapter::v1::AdapterError error) const;
  void NotifySourceClosed(const SourceState& source,
                          const xgc::adapter::v1::AdapterError& error) const;
  void RememberClosedSourceLocked(const std::string& stream_id,
                                  const SourceState& source,
                                  xgc::adapter::v1::WorkRequest terminal_frame,
                                  std::string terminal_digest);
  bool CommitSourceTerminalLocked(const std::string& stream_id,
                                  const SourceState& source,
                                  xgc::adapter::v1::WorkRequest terminal_frame);
  bool WorkFrameFitsLocked(xgc::adapter::v1::WorkRequest* frame) const;
  void QueueWorkProtocolError(std::uint64_t rejected_sequence,
                              const std::string& message);
  void BeginDrain();
  void MaybeQueueDrainStatusLocked();
  void RequestRemoteStop(const xgc::adapter::v1::StopRequest& request);

  // Bounded outbound queues, fencing, replay, and capability lookup.
  void StampHeaderLocked(xgc::adapter::v1::SessionHeader* header,
                         const SessionFence& fence, std::uint64_t connection_epoch,
                         std::uint64_t sequence) const;
  bool ValidateControlHeader(const xgc::adapter::v1::SessionHeader& header,
                             const SessionFence& fence, std::string* error);
  bool ValidateWorkHeader(const xgc::adapter::v1::SessionHeader& header,
                          const SessionFence& fence, std::string* error);
  bool ValidateHeaderLocked(const xgc::adapter::v1::SessionHeader& header,
                            const SessionFence& fence, std::uint64_t connection_epoch,
                            std::uint64_t* previous_sequence, std::string* error) const;
  bool QueueControl(xgc::adapter::v1::ControlRequest request);
  bool QueueWork(xgc::adapter::v1::WorkRequest request);
  bool QueueControlLocked(xgc::adapter::v1::ControlRequest request);
  bool QueueWorkLocked(xgc::adapter::v1::WorkRequest request);
  std::size_t ControlFrameBytesLocked(xgc::adapter::v1::ControlRequest* frame) const;
  std::size_t WorkFrameBytesLocked(xgc::adapter::v1::WorkRequest* frame) const;
  void QueueWorkOrFail(xgc::adapter::v1::WorkRequest request);
  TerminalCommitResult CommitTerminalFrame(const DispatchJob& job,
                                           xgc::adapter::v1::WorkRequest frame);
  bool ReserveTerminalReplayLocked(DispatchJob* job);
  void ReleaseWorkTokenLocked(const std::string& work_id);
  void RememberReplayLocked(const std::string& work_id, ReplayEntry entry);
  ActiveEndpoint FindActiveEndpointLocked(const std::string& capability_id,
                                          const std::string& endpoint_id) const;
  std::size_t CountSourceStreamsLocked(const std::string& capability_id,
                                       const std::string& endpoint_id) const;
  std::size_t FindBindingIndex(const xgc::adapter::v1::EnabledCapability& grant) const;
  const xgc::adapter::v1::CapabilityEndpointContract* FindEndpoint(
      const xgc::adapter::v1::CapabilityContract& contract,
      const std::string& endpoint_id) const;
  std::vector<xgc::adapter::v1::CapabilityStatus> DisabledCapabilityStatuses() const;

  // Callback teardown and synchronization.
  void DeactivateApplication(const std::string& reason);
  void DeactivateApplicationUnlocked(const std::string& reason);
  void SafeStopCapability(std::size_t binding_index) const;
  void SafeClearInstanceSpec() const;
  void SafeSourceClosed(const CapabilityCallbacks& callbacks,
                        const xgc::adapter::v1::SourceOpenRequest& request,
                        const xgc::adapter::v1::AdapterError& error) const;
  void WaitForCallbacks();
  void CancelAllWorkLocked();

  static bool Fail(std::string* error, const std::string& message) {
    internal::SetError(error, message);
    return false;
  }

  static bool IsTerminalSessionStatus(const grpc::Status& status);

  static constexpr std::size_t kNoBinding = std::numeric_limits<std::size_t>::max();

  ClientConfig config_;
  ClientCallbacks callbacks_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<xgc::adapter::v1::AdapterRuntimeLinkService::Stub> stub_;

  mutable std::mutex mutex_;
  std::mutex transition_mutex_;
  std::condition_variable condition_;
  std::condition_variable callbacks_condition_;
  ClientState state_ = ClientState::kStopped;
  bool stop_requested_ = false;
  bool session_failed_ = false;
  bool accepting_work_ = false;
  bool drain_requested_ = false;
  bool drain_reported_ = false;
  bool ready_callbacks_complete_ = false;
  bool work_attach_allowed_ = false;
  bool pair_ready_ = false;
  bool terminal_session_failure_ = false;
  bool session_lost_reported_ = false;
  bool registration_attempted_ = false;
  std::string last_error_;
  std::string terminal_session_error_;

  std::string host_id_;
  std::string session_id_;
  std::uint64_t session_generation_ = 0;
  std::uint64_t runtime_epoch_ = 0;
  std::uint32_t runtime_link_protocol_version_ = 0;
  std::uint32_t heartbeat_interval_ms_ = 0;
  std::uint32_t maximum_control_frame_bytes_ = 0;
  std::uint32_t maximum_work_frame_bytes_ = 0;
  std::uint64_t connection_epoch_counter_ = 0;
  std::uint64_t control_out_sequence_ = 0;
  std::uint64_t work_out_sequence_ = 0;
  std::uint64_t control_in_sequence_ = 0;
  std::uint64_t work_in_sequence_ = 0;
  std::uint64_t work_attach_connection_epoch_ = 0;

  xgc::adapter::v1::AdapterInstanceSpec active_spec_;
  xgc::adapter::v1::AdapterInstanceSpec desired_spec_;
  std::uint64_t spec_revision_ = 0;
  std::string spec_digest_;
  std::map<std::string, ActiveCapability> active_capabilities_;
  std::vector<xgc::adapter::v1::CapabilityStatus> capability_statuses_;

  std::deque<QueuedControlFrame> control_queue_;
  std::deque<QueuedWorkFrame> work_queue_;
  std::deque<DispatchJob> dispatch_queue_;
  std::map<std::string, WorkTokenState> work_tokens_;
  std::size_t in_flight_work_ = 0;
  std::size_t active_dispatch_jobs_ = 0;
  std::map<std::string, ReplayEntry> replay_;
  std::map<std::string, SourceState> source_streams_;
  std::map<std::string, ClosedSourceState> closed_sources_;
  std::uint64_t rejected_dispatches_ = 0;
  std::uint64_t dropped_outbound_frames_ = 0;
  std::size_t control_queue_bytes_ = 0;
  std::size_t work_queue_bytes_ = 0;
  std::size_t dispatch_queue_bytes_ = 0;
  std::size_t terminal_replay_bytes_ = 0;
  std::size_t source_state_bytes_ = 0;

  std::shared_ptr<grpc::ClientContext> active_register_context_;
  std::shared_ptr<grpc::ClientContext> active_control_context_;
  std::shared_ptr<grpc::ClientContext> active_work_context_;
  std::thread supervisor_thread_;
  std::vector<std::thread> dispatch_threads_;
};

}  // namespace adapter_runtime
}  // namespace xgc2
