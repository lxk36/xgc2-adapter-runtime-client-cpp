#pragma once

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../src/internal.hpp"
#include "xgc/adapter/v1/adapter.grpc.pb.h"
#include "xgc2/adapter_runtime/client.hpp"
#include "xgc2/adapter_runtime/version.hpp"

namespace {

using RuntimeLink = xgc::adapter::v1::AdapterRuntimeLinkService;

constexpr std::uint64_t kProcessGeneration = 7;
constexpr std::uint64_t kSessionGeneration = 3;
constexpr std::uint64_t kRuntimeEpoch = 11;
constexpr std::uint64_t kSpecRevision = 4;
constexpr std::uint32_t kContractVersion = 1;
const char* const kInstanceId = "ari-11111111111111111111111111111111";
const char* const kSessionId = "adapter-session-test";
const char* const kCapabilityId = "test.native";
const char* const kContractDigest =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const char* const kDisabledContractDigest =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const char* const kDefinitionDigest =
    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const char* const kBuildDigest =
    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const char* const kManifestDigest =
    "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const char* const kSpecDigest =
    "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
const char* const kReplacementSpecDigest =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

std::string TestDigest(const std::string& discriminator) {
  std::uint64_t hash = 1469598103934665603ULL;
  for (const unsigned char byte : discriminator) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  char block[17] = {};
  std::snprintf(block, sizeof(block), "%016llx", static_cast<unsigned long long>(hash));
  return "sha256:" + std::string(block) + block + block + block;
}

std::int64_t NowNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

xgc::v1::SchemaReference TestSchema() {
  xgc::v1::SchemaReference schema;
  schema.set_message_id(1001);
  schema.set_type_name("xgc.test.v1.Value");
  schema.set_schema_version(1);
  schema.set_schema_fingerprint(0x1020304050607080ULL);
  return schema;
}

xgc::v1::Payload TestPayload(const std::string& value) {
  xgc::v1::Payload payload;
  *payload.mutable_schema() = TestSchema();
  payload.set_encoding(xgc::v1::PAYLOAD_ENCODING_PROTOBUF);
  payload.set_value(value);
  return payload;
}

xgc::adapter::v1::CapabilityContract TestContract(
    const std::string& capability_id = kCapabilityId,
    const std::string& digest = kContractDigest) {
  xgc::adapter::v1::CapabilityContract contract;
  contract.set_capability_id(capability_id);
  contract.set_contract_version(kContractVersion);
  contract.set_contract_digest(digest);

  auto* unary = contract.add_endpoints();
  unary->set_endpoint_id("query");
  unary->set_interaction_mode(xgc::adapter::v1::INTERACTION_MODE_UNARY);
  unary->set_side_effect_class(xgc::adapter::v1::SIDE_EFFECT_CLASS_READ_ONLY);
  unary->set_idempotency_mode(xgc::adapter::v1::IDEMPOTENCY_MODE_OPTIONAL);
  unary->set_deadline_required(true);
  unary->set_default_timeout_ms(1000);
  unary->set_maximum_timeout_ms(5000);
  *unary->mutable_input_schema() = TestSchema();
  *unary->mutable_output_schema() = TestSchema();
  unary->mutable_limits()->set_maximum_request_bytes(1024);
  unary->mutable_limits()->set_maximum_response_bytes(1024);
  unary->mutable_limits()->set_maximum_concurrency(4);

  auto* operation = contract.add_endpoints();
  operation->set_endpoint_id("command");
  operation->set_interaction_mode(xgc::adapter::v1::INTERACTION_MODE_OPERATION);
  operation->set_side_effect_class(xgc::adapter::v1::SIDE_EFFECT_CLASS_NON_IDEMPOTENT);
  operation->set_idempotency_mode(xgc::adapter::v1::IDEMPOTENCY_MODE_REQUIRED);
  operation->set_cancellation_supported(true);
  operation->set_deadline_required(true);
  operation->set_default_timeout_ms(1000);
  operation->set_maximum_timeout_ms(5000);
  *operation->mutable_input_schema() = TestSchema();
  *operation->mutable_output_schema() = TestSchema();
  operation->mutable_limits()->set_maximum_request_bytes(1024);
  operation->mutable_limits()->set_maximum_response_bytes(1024);
  operation->mutable_limits()->set_maximum_concurrency(2);

  auto* source = contract.add_endpoints();
  source->set_endpoint_id("events");
  source->set_interaction_mode(xgc::adapter::v1::INTERACTION_MODE_STREAM_SOURCE);
  source->set_side_effect_class(xgc::adapter::v1::SIDE_EFFECT_CLASS_READ_ONLY);
  source->set_idempotency_mode(xgc::adapter::v1::IDEMPOTENCY_MODE_NOT_SUPPORTED);
  source->set_default_timeout_ms(1000);
  source->set_maximum_timeout_ms(5000);
  *source->mutable_output_schema() = TestSchema();
  source->mutable_limits()->set_maximum_request_bytes(1024);
  source->mutable_limits()->set_maximum_response_bytes(32);
  source->mutable_limits()->set_maximum_concurrency(2);
  source->mutable_limits()->set_maximum_streams(8);
  source->mutable_limits()->set_maximum_stream_chunk_messages(2);
  source->mutable_limits()->set_maximum_stream_chunk_bytes(1024);
  return contract;
}

xgc::adapter::v1::AdapterProcessBootstrap TestBootstrap(
    const std::string& runtime_target) {
  xgc::adapter::v1::AdapterProcessBootstrap bootstrap;
  bootstrap.set_format_version(xgc2::adapter_runtime::kAdapterBootstrapFormatVersion);
  bootstrap.set_runtime_target(runtime_target);
  auto* registration = bootstrap.mutable_registration();
  registration->set_instance_id(kInstanceId);
  registration->set_process_generation(kProcessGeneration);
  registration->set_definition_id("test-adapter");
  registration->set_definition_digest(kDefinitionDigest);
  registration->set_software_version("1.0.0-test");
  registration->set_build_digest(kBuildDigest);
  registration->set_manifest_digest(kManifestDigest);
  registration->set_bootstrap_token("single-use-secret");
  registration->set_runtime_link_protocol_version(
      xgc2::adapter_runtime::kRuntimeLinkProtocolVersion);
  *registration->add_supported_capabilities() = TestContract();
  *registration->add_supported_capabilities() =
      TestContract("test.disabled", kDisabledContractDigest);

  auto* spec = bootstrap.mutable_initial_spec();
  spec->set_instance_id(kInstanceId);
  spec->set_process_generation(kProcessGeneration);
  spec->set_revision(kSpecRevision);
  spec->set_spec_digest(kSpecDigest);
  spec->mutable_scope()->set_kind("tenant-context");
  spec->mutable_scope()->set_key(
      "sha256:1111111111111111111111111111111111111111111111111111111111111111");
  (*spec->mutable_scope()->mutable_attributes())["target"] = "fleet-a";
  (*spec->mutable_scope()->mutable_attributes())["run"] = "run-1";
  (*spec->mutable_scope()->mutable_attributes())["provider"] = "sim";
  *spec->mutable_configuration() = TestPayload("instance-configuration");
  auto* secret = spec->add_secrets();
  secret->set_name("credential");
  secret->set_reference("secret://credential");
  secret->set_version("sha256:secret-version");
  auto* enabled = spec->add_capabilities();
  enabled->set_capability_id(kCapabilityId);
  enabled->set_contract_version(kContractVersion);
  enabled->set_contract_digest(kContractDigest);
  enabled->add_enabled_endpoint_ids("query");
  enabled->add_enabled_endpoint_ids("command");
  enabled->add_enabled_endpoint_ids("events");
  return bootstrap;
}

class FakeRuntimeLink final : public RuntimeLink::Service {
 public:
  enum class Scenario {
    kFullExchange,
    kPreCancelledOperation,
    kCallbackCancellationRace,
    kPairReconnects,
    kSpecReplacement,
    kSourceSemantics,
    kSourceFrameLimit,
    kHandlerResultValidation,
    kTerminalAcknowledgement,
    kLegacyProtocolSelection,
  };

  explicit FakeRuntimeLink(xgc::adapter::v1::AdapterInstanceSpec spec,
                           Scenario scenario = Scenario::kFullExchange)
      : spec_(std::move(spec)), replacement_spec_(spec_), scenario_(scenario) {
    replacement_spec_.set_revision(spec_.revision() + 1);
    replacement_spec_.set_spec_digest(kReplacementSpecDigest);
    *replacement_spec_.mutable_configuration() =
        TestPayload("replacement-instance-configuration");
  }

  grpc::Status Register(grpc::ServerContext*,
                        const xgc::adapter::v1::RegisterRequest* request,
                        xgc::adapter::v1::RegisterResponse* response) override {
    if (registration_attempts_.fetch_add(1) != 0) {
      return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                          "bootstrap token was already consumed");
    }
    if (request->instance_id() != kInstanceId ||
        request->process_generation() != kProcessGeneration ||
        request->definition_digest() != kDefinitionDigest ||
        request->build_digest() != kBuildDigest ||
        request->manifest_digest() != kManifestDigest ||
        request->bootstrap_token() != "single-use-secret" ||
        request->sdk_version() != xgc2::adapter_runtime::kClientVersion ||
        request->runtime_link_protocol_version() !=
            xgc2::adapter_runtime::kRuntimeLinkProtocolVersion ||
        request->supported_capabilities_size() != 2) {
      return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                          "registration proof mismatch");
    }
    response->set_accepted(true);
    response->set_host_id("test-host");
    response->set_session_id(kSessionId);
    response->set_process_generation(kProcessGeneration);
    response->set_session_generation(kSessionGeneration);
    response->set_runtime_epoch(kRuntimeEpoch);
    response->set_runtime_link_protocol_version(
        scenario_ == Scenario::kLegacyProtocolSelection
            ? xgc2::adapter_runtime::kRuntimeLinkProtocolVersion - 1U
            : xgc2::adapter_runtime::kRuntimeLinkProtocolVersion);
    response->set_current_spec_revision(kSpecRevision);
    response->set_heartbeat_interval_ms(25);
    response->set_maximum_control_frame_bytes(1024 * 1024);
    response->set_maximum_work_frame_bytes(
        scenario_ == Scenario::kSourceFrameLimit
            ? 512
            : (scenario_ == Scenario::kHandlerResultValidation ? 512 : 1024 * 1024));
    ++registrations_;
    return grpc::Status::OK;
  }

  grpc::Status Control(
      grpc::ServerContext*,
      grpc::ServerReaderWriter<xgc::adapter::v1::ControlResponse,
                               xgc::adapter::v1::ControlRequest>* stream) override {
    xgc::adapter::v1::ControlRequest first;
    if (!stream->Read(&first) || !first.has_heartbeat()) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "missing fenced initial heartbeat");
    }
    const std::uint64_t connection_epoch = first.header().connection_epoch();
    if (!ValidateHeader(first.header(), connection_epoch, 0)) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "invalid initial Control header");
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (connection_epoch != last_control_epoch_ + 1) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "Control connection_epoch did not advance by one");
      }
      last_control_epoch_ = connection_epoch;
      connection_epoch_ = connection_epoch;
      control_epochs_.push_back(connection_epoch);
      spec_applied_epoch_ = 0;
      control_attached_ = true;
    }
    condition_.notify_all();

    xgc::adapter::v1::ControlResponse desired;
    *desired.mutable_header() = HostHeader(1, connection_epoch);
    *desired.mutable_instance_spec() = spec_;
    if (!stream->Write(desired)) {
      return grpc::Status::CANCELLED;
    }

    xgc::adapter::v1::ControlRequest request;
    std::uint64_t last_control_sequence = 1;
    while (stream->Read(&request)) {
      if (!ValidateHeader(request.header(), connection_epoch, last_control_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid Control header");
      }
      last_control_sequence = request.header().frame_sequence();
      bool send_replacement = false;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (request.has_apply_instance_spec_result()) {
          const auto& applied = request.apply_instance_spec_result();
          const auto& expected = replacement_sent_ ? replacement_spec_ : spec_;
          const bool applied_exactly =
              applied.applied() && applied.revision() == expected.revision() &&
              applied.spec_digest() == expected.spec_digest() &&
              HasReadyContract(applied.capabilities());
          spec_applied_ = applied_exactly;
          spec_applied_epoch_ = applied_exactly ? connection_epoch : 0;
          if (applied_exactly) {
            ++spec_applications_;
          }
          if (applied.has_error() &&
              applied.error().code() == "ready-callback-failed") {
            bool advertised_ready = false;
            for (const auto& status : applied.capabilities()) {
              advertised_ready =
                  advertised_ready ||
                  status.state() == xgc::adapter::v1::CAPABILITY_STATE_READY;
            }
            ready_failure_result_ =
                !applied.applied() && !advertised_ready &&
                applied.error().class_() == xgc::adapter::v1::ERROR_CLASS_PERMANENT &&
                applied.error().message() ==
                    "capability ready callback failed after instance spec commit";
          }
        }
        if (request.has_heartbeat() &&
            request.heartbeat().applied_spec_revision() == kSpecRevision &&
            HasReadyContract(request.heartbeat().capabilities())) {
          ready_heartbeat_ = true;
        }
        if (scenario_ == Scenario::kSpecReplacement && work_attach_first_ &&
            !replacement_sent_) {
          replacement_sent_ = true;
          send_replacement = true;
        }
      }
      condition_.notify_all();
      if (send_replacement) {
        xgc::adapter::v1::ControlResponse replacement;
        *replacement.mutable_header() = HostHeader(2, connection_epoch);
        *replacement.mutable_instance_spec() = replacement_spec_;
        if (!stream->Write(replacement)) {
          return grpc::Status::CANCELLED;
        }
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status Work(
      grpc::ServerContext*,
      grpc::ServerReaderWriter<xgc::adapter::v1::WorkResponse,
                               xgc::adapter::v1::WorkRequest>* stream) override {
    xgc::adapter::v1::WorkRequest attach;
    if (!stream->Read(&attach) || !attach.has_attach()) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "WorkAttach was not the first Work frame");
    }
    const std::uint64_t connection_epoch = attach.header().connection_epoch();
    if (!ValidateHeader(attach.header(), connection_epoch, 0) ||
        attach.attach().applied_spec_revision() != spec_.revision() ||
        attach.attach().applied_spec_digest() != spec_.spec_digest()) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "WorkAttach did not carry the exact applied spec");
    }
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (connection_epoch != connection_epoch_) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "Work and Control epochs are not paired");
      }
      if (!condition_.wait_for(lock, std::chrono::seconds(5), [this, connection_epoch] {
            return spec_applied_epoch_ == connection_epoch;
          })) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "Work started before Control applied the exact spec");
      }
      work_attach_first_ = true;
      work_connection_epoch_ = connection_epoch;
      paired_epochs_.push_back(connection_epoch);
    }
    condition_.notify_all();

    if (scenario_ == Scenario::kPairReconnects) {
      if (connection_epoch == 1 && !WriteSourceOpen(stream, 1, "reconnect-source")) {
        return grpc::Status::CANCELLED;
      }
      std::uint64_t last_work_sequence = 1;
      xgc::adapter::v1::WorkRequest frame;
      while (stream->Read(&frame)) {
        if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
          return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                              "invalid Work header after reconnect");
        }
        last_work_sequence = frame.header().frame_sequence();
        if (connection_epoch == 1 && frame.has_source_open_result()) {
          std::lock_guard<std::mutex> lock(mutex_);
          source_opened_ = frame.source_open_result().accepted() &&
                           frame.source_open_result().stream_id() == "reconnect-source";
          condition_.notify_all();
        }
      }
      return grpc::Status::OK;
    }
    if (scenario_ == Scenario::kSpecReplacement) {
      std::uint64_t last_work_sequence = 1;
      xgc::adapter::v1::WorkRequest frame;
      while (stream->Read(&frame)) {
        if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
          return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                              "invalid Work header during spec replacement");
        }
        last_work_sequence = frame.header().frame_sequence();
      }
      return grpc::Status::OK;
    }

    if (scenario_ == Scenario::kPreCancelledOperation) {
      return RunPreCancelledOperation(stream, connection_epoch);
    }
    if (scenario_ == Scenario::kCallbackCancellationRace) {
      return RunCallbackCancellationRace(stream, connection_epoch);
    }
    if (scenario_ == Scenario::kSourceSemantics) {
      return RunSourceSemantics(stream, connection_epoch);
    }
    if (scenario_ == Scenario::kSourceFrameLimit) {
      return RunSourceFrameLimit(stream, connection_epoch);
    }
    if (scenario_ == Scenario::kHandlerResultValidation) {
      return RunHandlerResultValidation(stream, connection_epoch);
    }
    if (scenario_ == Scenario::kTerminalAcknowledgement) {
      return RunTerminalAcknowledgement(stream, connection_epoch);
    }

    if (!WriteUnary(stream, 1) || !WriteOperation(stream, 2) ||
        !WriteSourceOpen(stream, 3, "source-1")) {
      return grpc::Status::CANCELLED;
    }

    std::uint64_t host_sequence = 3;
    std::uint64_t last_work_sequence = 1;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      if (frame.has_unary_response()) {
        std::lock_guard<std::mutex> lock(mutex_);
        unary_value_ = frame.unary_response().output().value();
        condition_.notify_all();
      } else if (frame.has_operation_event()) {
        const auto phase = frame.operation_event().phase();
        if (phase == xgc::adapter::v1::OPERATION_PHASE_STARTED && !cancellation_sent_) {
          xgc::adapter::v1::WorkResponse cancellation;
          *cancellation.mutable_header() = HostHeader(++host_sequence);
          cancellation.mutable_cancellation()->set_work_id("operation-1");
          cancellation.mutable_cancellation()->set_reason("test cancellation");
          cancellation_sent_ = stream->Write(cancellation);
        }
        if (phase == xgc::adapter::v1::OPERATION_PHASE_CANCELLED) {
          bool send_replay = false;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            ++operation_terminals_;
            send_replay = operation_terminals_ == 1;
            condition_.notify_all();
          }
          if (send_replay && !WriteOperation(stream, ++host_sequence)) {
            return grpc::Status::CANCELLED;
          }
        }
      } else if (frame.has_source_open_result()) {
        {
          std::unique_lock<std::mutex> lock(mutex_);
          source_opened_ = frame.source_open_result().stream_id() == "source-1" &&
                           frame.source_open_result().accepted() &&
                           !frame.source_open_result().has_initial_payload();
          condition_.notify_all();
          if (!condition_.wait_for(lock, std::chrono::seconds(5),
                                   [this] { return release_source_credit_; })) {
            return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                                "source credit was not released by test");
          }
        }
        xgc::adapter::v1::WorkResponse credit;
        *credit.mutable_header() = HostHeader(++host_sequence);
        credit.mutable_source_credit()->set_stream_id("source-1");
        credit.mutable_source_credit()->mutable_grant()->set_messages(2);
        credit.mutable_source_credit()->mutable_grant()->set_bytes(32);
        if (!stream->Write(credit)) {
          return grpc::Status::CANCELLED;
        }
        {
          std::lock_guard<std::mutex> lock(mutex_);
          source_credit_sent_ = true;
          condition_.notify_all();
        }
      } else if (frame.has_source_data()) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!source_opened_) {
          return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                              "SourceData preceded accepted SourceOpenResult");
        }
        source_items_ += frame.source_data().items_size();
        condition_.notify_all();
      } else if (frame.has_source_close()) {
        std::lock_guard<std::mutex> lock(mutex_);
        source_closed_ = true;
        condition_.notify_all();
      }
    }
    return grpc::Status::OK;
  }

  bool WaitForCompleteExchange() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [this] {
      return spec_applied_ && ready_heartbeat_ && unary_value_ == "unary-result" &&
             operation_terminals_ >= 2 && source_items_ == 2;
    });
  }

  bool WaitForSourceCredit() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return source_credit_sent_; });
  }

  void ReleaseSourceCredit() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      release_source_credit_ = true;
    }
    condition_.notify_all();
  }

  bool WaitForSourceClosed() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return source_closed_; });
  }

  bool WaitForSourceSemantics() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] {
                                 return source_semantics_complete_ ||
                                        !source_semantics_error_.empty();
                               }) &&
           source_semantics_complete_ && source_semantics_error_.empty();
  }

  std::string source_semantics_error() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return source_semantics_error_;
  }

  bool WaitForSourceFrameOpen() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return source_frame_opened_; });
  }

  bool WaitForSourceFrameExchange() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [this] {
      return source_frame_data_ && source_frame_closed_;
    });
  }

  bool WaitForHandlerResultValidation() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] {
                                 return handler_result_validation_complete_ ||
                                        !handler_result_validation_error_.empty();
                               }) &&
           handler_result_validation_complete_ &&
           handler_result_validation_error_.empty();
  }

  std::string handler_result_validation_error() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return handler_result_validation_error_;
  }

  bool WaitForTerminalAcknowledgement() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] {
                                 return terminal_acknowledgement_complete_ ||
                                        !terminal_acknowledgement_error_.empty();
                               }) &&
           terminal_acknowledgement_complete_ &&
           terminal_acknowledgement_error_.empty();
  }

  bool WaitForReadyFailureResult() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return ready_failure_result_; });
  }

  std::string terminal_acknowledgement_error() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return terminal_acknowledgement_error_;
  }

  bool WaitForScenarioCancellation() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return scenario_cancellation_sent_; });
  }

  bool WaitForPreCancelledTerminal() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return pre_cancelled_terminal_received_; });
  }

  bool WaitForRaceTerminal() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return race_terminal_received_; });
  }

  xgc::adapter::v1::OperationPhase pre_cancelled_terminal() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pre_cancelled_terminal_;
  }

  xgc::adapter::v1::OperationPhase race_terminal() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return race_terminal_;
  }

  bool paired_epoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return control_attached_ && work_attach_first_ && connection_epoch_ != 0 &&
           connection_epoch_ == work_connection_epoch_;
  }

  bool WaitForPairAttachments(std::size_t count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [this, count] {
      return paired_epochs_.size() >= count;
    });
  }

  std::vector<std::uint64_t> paired_epochs() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return paired_epochs_;
  }

  std::vector<std::uint64_t> control_epochs() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return control_epochs_;
  }

  int spec_applications() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return spec_applications_;
  }

  bool WaitForSpecApplications(int count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5),
                               [this, count] { return spec_applications_ >= count; });
  }

  int registrations() const { return registrations_.load(); }
  int registration_attempts() const { return registration_attempts_.load(); }

 private:
  using WorkStream = grpc::ServerReaderWriter<xgc::adapter::v1::WorkResponse,
                                              xgc::adapter::v1::WorkRequest>;

  static bool IsTerminalOperationPhase(xgc::adapter::v1::OperationPhase phase) {
    return phase == xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED ||
           phase == xgc::adapter::v1::OPERATION_PHASE_FAILED ||
           phase == xgc::adapter::v1::OPERATION_PHASE_CANCELLED ||
           phase == xgc::adapter::v1::OPERATION_PHASE_REJECTED;
  }

  grpc::Status RunPreCancelledOperation(WorkStream* stream,
                                        std::uint64_t connection_epoch) {
    if (!WriteOperation(stream, 1, "blocking-operation", "blocking-digest")) {
      return grpc::Status::CANCELLED;
    }

    std::uint64_t last_work_sequence = 1;
    bool cancellations_sent = false;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      if (!cancellations_sent && frame.has_operation_event() &&
          frame.operation_event().work_id() == "blocking-operation" &&
          frame.operation_event().phase() ==
              xgc::adapter::v1::OPERATION_PHASE_STARTED) {
        if (!WriteOperation(stream, 2, "pre-cancelled-operation",
                            "pre-cancelled-digest")) {
          return grpc::Status::CANCELLED;
        }
        xgc::adapter::v1::WorkResponse cancellation;
        *cancellation.mutable_header() = HostHeader(3);
        cancellation.mutable_cancellation()->set_work_id("pre-cancelled-operation");
        cancellation.mutable_cancellation()->set_reason(
            "cancelled while waiting for dispatch");
        if (!stream->Write(cancellation)) {
          return grpc::Status::CANCELLED;
        }
        xgc::adapter::v1::WorkResponse dispatch_barrier;
        *dispatch_barrier.mutable_header() = HostHeader(4);
        dispatch_barrier.mutable_cancellation()->set_work_id("blocking-operation");
        dispatch_barrier.mutable_cancellation()->set_reason(
            "prove preceding cancellation was consumed");
        if (!stream->Write(dispatch_barrier)) {
          return grpc::Status::CANCELLED;
        }
        cancellations_sent = true;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          scenario_cancellation_sent_ = true;
        }
        condition_.notify_all();
      }
      if (!frame.has_operation_event() ||
          frame.operation_event().work_id() != "pre-cancelled-operation" ||
          !IsTerminalOperationPhase(frame.operation_event().phase())) {
        continue;
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        pre_cancelled_terminal_ = frame.operation_event().phase();
        pre_cancelled_terminal_received_ = true;
      }
      condition_.notify_all();
    }
    return grpc::Status::OK;
  }

  grpc::Status RunCallbackCancellationRace(WorkStream* stream,
                                           std::uint64_t connection_epoch) {
    if (!WriteOperation(stream, 1, "racing-operation", "racing-digest")) {
      return grpc::Status::CANCELLED;
    }

    std::uint64_t last_work_sequence = 1;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      if (!frame.has_operation_event() ||
          frame.operation_event().work_id() != "racing-operation") {
        continue;
      }
      if (frame.operation_event().phase() ==
              xgc::adapter::v1::OPERATION_PHASE_STARTED &&
          !scenario_cancellation_sent_) {
        xgc::adapter::v1::WorkResponse cancellation;
        *cancellation.mutable_header() = HostHeader(2);
        cancellation.mutable_cancellation()->set_work_id("racing-operation");
        cancellation.mutable_cancellation()->set_reason(
            "race with native callback completion");
        if (!stream->Write(cancellation)) {
          return grpc::Status::CANCELLED;
        }
        {
          std::lock_guard<std::mutex> lock(mutex_);
          scenario_cancellation_sent_ = true;
        }
        condition_.notify_all();
      }
      if (!IsTerminalOperationPhase(frame.operation_event().phase())) {
        continue;
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        race_terminal_ = frame.operation_event().phase();
        race_terminal_received_ = true;
      }
      condition_.notify_all();
    }
    return grpc::Status::OK;
  }

  grpc::Status RunHandlerResultValidation(WorkStream* stream,
                                          std::uint64_t connection_epoch) {
    if (!WriteUnary(stream, 1, "unary-malformed", "unary-malformed") ||
        !WriteUnary(stream, 2, "unary-frame-large", "unary-frame-large") ||
        !WriteUnary(stream, 3, "unary-invalid-digest", "not-a-digest", false) ||
        !WriteOperation(stream, 4, "operation-malformed", "operation-malformed") ||
        !WriteOperation(stream, 5, "operation-phase-mismatch",
                        "operation-phase-mismatch") ||
        !WriteOperation(stream, 6, "operation-frame-large", "operation-frame-large")) {
      return grpc::Status::CANCELLED;
    }

    bool unary_malformed = false;
    bool unary_frame_large = false;
    bool unary_invalid_digest = false;
    bool operation_malformed = false;
    bool operation_phase_mismatch = false;
    bool operation_frame_large = false;
    std::uint64_t last_work_sequence = 1;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid handler-validation Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      std::string failure;
      if (frame.has_protocol_error()) {
        failure = "unexpected handler-validation protocol error";
      } else if (frame.has_unary_response()) {
        const auto& response = frame.unary_response();
        if (!response.has_error()) {
          failure = "handler-validation unary response did not carry an error";
        } else if (response.work_id() == "unary-malformed") {
          unary_malformed =
              response.error().class_() == xgc::adapter::v1::ERROR_CLASS_PERMANENT &&
              response.error().code() == "invalid-handler-result";
          if (!unary_malformed) {
            failure = "malformed unary result was not normalized";
          }
        } else if (response.work_id() == "unary-frame-large") {
          unary_frame_large = response.error().class_() ==
                                  xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED &&
                              response.error().code() == "response-too-large";
          if (!unary_frame_large) {
            failure = "oversized unary frame was not compacted";
          }
        } else if (response.work_id() == "unary-invalid-digest") {
          unary_invalid_digest =
              response.error().class_() == xgc::adapter::v1::ERROR_CLASS_REJECTED &&
              response.error().code() == "invalid-work-context";
          if (!unary_invalid_digest) {
            failure = "malformed request digest was not rejected";
          }
        }
      } else if (frame.has_operation_event() &&
                 IsTerminalOperationPhase(frame.operation_event().phase())) {
        const auto& event = frame.operation_event();
        if (!event.has_error() ||
            event.phase() != xgc::adapter::v1::OPERATION_PHASE_FAILED) {
          failure = "handler-validation operation terminal is malformed";
        } else if (event.work_id() == "operation-malformed") {
          operation_malformed =
              event.error().class_() == xgc::adapter::v1::ERROR_CLASS_PERMANENT &&
              event.error().code() == "invalid-handler-result";
          if (!operation_malformed) {
            failure = "malformed operation success was not normalized";
          }
        } else if (event.work_id() == "operation-phase-mismatch") {
          operation_phase_mismatch =
              event.error().class_() == xgc::adapter::v1::ERROR_CLASS_PERMANENT &&
              event.error().code() == "invalid-handler-result";
          if (!operation_phase_mismatch) {
            failure = "operation phase/error mismatch was not normalized";
          }
        } else if (event.work_id() == "operation-frame-large") {
          operation_frame_large =
              event.error().class_() ==
                  xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED &&
              event.error().code() == "response-too-large";
          if (!operation_frame_large) {
            failure = "oversized operation frame was not compacted";
          }
        }
      }
      const bool complete = unary_malformed && unary_frame_large &&
                            unary_invalid_digest && operation_malformed &&
                            operation_phase_mismatch && operation_frame_large;
      if (!failure.empty() || complete) {
        std::lock_guard<std::mutex> lock(mutex_);
        handler_result_validation_error_ = failure;
        handler_result_validation_complete_ = complete && failure.empty();
        condition_.notify_all();
      }
      if (!failure.empty()) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, failure);
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status RunTerminalAcknowledgement(WorkStream* stream,
                                          std::uint64_t connection_epoch) {
    if (!WriteUnary(stream, 1, "ack-active", "ack-active")) {
      return grpc::Status::CANCELLED;
    }
    xgc::adapter::v1::WorkResponse active_acknowledgement;
    *active_acknowledgement.mutable_header() = HostHeader(2);
    auto* active = active_acknowledgement.mutable_terminal_acknowledgement();
    active->set_work_id("ack-active");
    active->set_terminal_digest(
        "sha256:0000000000000000000000000000000000000000000000000000000000000000");
    if (!stream->Write(active_acknowledgement)) {
      return grpc::Status::CANCELLED;
    }

    std::uint64_t host_sequence = 2;
    std::uint64_t last_work_sequence = 1;
    std::string first_digest;
    int stage = 0;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid terminal acknowledgement Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      std::string failure;
      if (stage == 0 && frame.has_protocol_error()) {
        if (frame.protocol_error().rejected_frame_sequence() != host_sequence ||
            frame.protocol_error().error().code() != "invalid-work-frame") {
          failure = "active terminal acknowledgement was not rejected exactly";
        } else {
          stage = 1;
        }
      } else if (stage == 1 && frame.has_unary_response() &&
                 frame.unary_response().work_id() == "ack-active") {
        std::string active_digest;
        if (!xgc2::adapter_runtime::internal::TerminalDigest(
                frame.unary_response(), &active_digest, &failure)) {
          failure = "could not digest active Work terminal: " + failure;
        } else {
          xgc::adapter::v1::WorkResponse acknowledgement;
          *acknowledgement.mutable_header() = HostHeader(++host_sequence);
          auto* accepted = acknowledgement.mutable_terminal_acknowledgement();
          accepted->set_work_id("ack-active");
          accepted->set_terminal_digest(active_digest);
          if (!stream->Write(acknowledgement) ||
              !WriteUnary(stream, ++host_sequence, "ack-work-1", "ack-work-1")) {
            return grpc::Status::CANCELLED;
          }
          stage = 2;
        }
      } else if (stage == 2 && frame.has_unary_response() &&
                 frame.unary_response().work_id() == "ack-work-1") {
        if (!xgc2::adapter_runtime::internal::TerminalDigest(frame.unary_response(),
                                                             &first_digest, &failure)) {
          failure = "could not digest first Work terminal: " + failure;
        } else {
          xgc::adapter::v1::WorkResponse wrong;
          *wrong.mutable_header() = HostHeader(++host_sequence);
          auto* acknowledgement = wrong.mutable_terminal_acknowledgement();
          acknowledgement->set_work_id("ack-work-1");
          acknowledgement->set_terminal_digest(
              "sha256:"
              "0000000000000000000000000000000000000000000000000000000000000000");
          if (!stream->Write(wrong)) {
            return grpc::Status::CANCELLED;
          }
          stage = 3;
        }
      } else if (stage == 3 && frame.has_protocol_error()) {
        if (frame.protocol_error().rejected_frame_sequence() != host_sequence ||
            frame.protocol_error().error().code() != "invalid-work-frame") {
          failure = "wrong terminal digest did not produce an exact protocol error";
        } else {
          xgc::adapter::v1::WorkResponse acknowledgement;
          *acknowledgement.mutable_header() = HostHeader(++host_sequence);
          auto* accepted = acknowledgement.mutable_terminal_acknowledgement();
          accepted->set_work_id("ack-work-1");
          accepted->set_terminal_digest(first_digest);
          if (!stream->Write(acknowledgement) ||
              !WriteUnary(stream, ++host_sequence, "ack-work-2", "ack-work-2")) {
            return grpc::Status::CANCELLED;
          }
          stage = 4;
        }
      } else if (stage == 4 && frame.has_unary_response() &&
                 frame.unary_response().work_id() == "ack-work-2") {
        std::string second_digest;
        if (!xgc2::adapter_runtime::internal::TerminalDigest(
                frame.unary_response(), &second_digest, &failure)) {
          failure = "could not digest second Work terminal: " + failure;
        } else {
          xgc::adapter::v1::WorkResponse second_acknowledgement;
          *second_acknowledgement.mutable_header() = HostHeader(++host_sequence);
          auto* accepted = second_acknowledgement.mutable_terminal_acknowledgement();
          accepted->set_work_id("ack-work-2");
          accepted->set_terminal_digest(second_digest);

          xgc::adapter::v1::WorkResponse duplicate;
          *duplicate.mutable_header() = HostHeader(++host_sequence);
          auto* duplicate_ack = duplicate.mutable_terminal_acknowledgement();
          duplicate_ack->set_work_id("ack-work-1");
          duplicate_ack->set_terminal_digest(first_digest);
          if (!stream->Write(second_acknowledgement) || !stream->Write(duplicate) ||
              !WriteSourceOpen(stream, ++host_sequence, "ack-source-1")) {
            return grpc::Status::CANCELLED;
          }
          stage = 5;
        }
      } else if (stage == 5 && frame.has_source_open_result() &&
                 frame.source_open_result().stream_id() == "ack-source-1") {
        std::string source_digest;
        if (frame.source_open_result().accepted() ||
            !xgc2::adapter_runtime::internal::TerminalDigest(
                frame.source_open_result(), &source_digest, &failure)) {
          failure = failure.empty() ? "first source terminal was not a typed rejection"
                                    : failure;
        } else {
          xgc::adapter::v1::WorkResponse acknowledgement;
          *acknowledgement.mutable_header() = HostHeader(++host_sequence);
          auto* accepted = acknowledgement.mutable_terminal_acknowledgement();
          accepted->set_stream_id("ack-source-1");
          accepted->set_terminal_digest(source_digest);
          if (!stream->Write(acknowledgement) ||
              !WriteSourceOpen(stream, ++host_sequence, "ack-source-2")) {
            return grpc::Status::CANCELLED;
          }
          stage = 6;
        }
      } else if (stage == 6 && frame.has_source_open_result() &&
                 frame.source_open_result().stream_id() == "ack-source-2") {
        if (frame.source_open_result().accepted()) {
          failure = "second source terminal was not a typed rejection";
        } else {
          std::lock_guard<std::mutex> lock(mutex_);
          terminal_acknowledgement_complete_ = true;
          condition_.notify_all();
          stage = 7;
        }
      } else if (frame.has_protocol_error()) {
        failure = "unexpected terminal acknowledgement protocol error: " +
                  frame.protocol_error().error().message();
      }

      if (!failure.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        terminal_acknowledgement_error_ = failure;
        condition_.notify_all();
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, failure);
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status RunSourceSemantics(WorkStream* stream, std::uint64_t connection_epoch) {
    if (!WriteSourceOpen(stream, 1, "source-initial")) {
      return grpc::Status::CANCELLED;
    }

    std::uint64_t host_sequence = 1;
    std::uint64_t last_work_sequence = 1;
    int stage = 0;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid source semantics Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      std::string failure;
      if (frame.has_protocol_error() && stage != 3 && stage != 8) {
        failure = "unexpected Adapter WorkProtocolError: " +
                  frame.protocol_error().error().message();
      } else if (stage == 0 && frame.has_source_open_result()) {
        const auto& result = frame.source_open_result();
        if (result.stream_id() != "source-initial" || !result.accepted() ||
            !result.has_initial_payload() ||
            result.initial_payload().value() != "initial-value") {
          failure = "accepted initial SourceOpenResult was malformed";
        } else if (!WriteSourceOpen(stream, ++host_sequence,
                                    "source-initial-too-large")) {
          return grpc::Status::CANCELLED;
        } else {
          stage = 1;
        }
      } else if (stage == 1 && frame.has_source_open_result()) {
        const auto& result = frame.source_open_result();
        if (result.stream_id() != "source-initial-too-large" || result.accepted() ||
            result.error().code() != "initial-payload-invalid") {
          failure = "maximum_response_bytes did not reject initial payload";
        } else if (!WriteSourceOpen(stream, ++host_sequence, "source-reject")) {
          return grpc::Status::CANCELLED;
        } else {
          stage = 2;
        }
      } else if (stage == 2 && frame.has_source_open_result()) {
        const auto& result = frame.source_open_result();
        if (result.stream_id() != "source-reject" || result.accepted() ||
            result.error().class_() != xgc::adapter::v1::ERROR_CLASS_TRANSIENT ||
            result.error().code() != "app-reject") {
          failure = "typed rejected SourceOpenResult was malformed";
        } else {
          xgc::adapter::v1::WorkResponse mismatch;
          *mismatch.mutable_header() = HostHeader(++host_sequence);
          auto* request = mismatch.mutable_source_open_request();
          FillContext(request->mutable_context(), "", "events", "source-empty-key",
                      "source-empty-digest");
          request->mutable_initial_credit()->set_messages(1);
          request->mutable_initial_credit()->set_bytes(8);
          if (!stream->Write(mismatch)) {
            return grpc::Status::CANCELLED;
          }
          stage = 3;
        }
      } else if (stage == 3 && frame.has_protocol_error()) {
        if (frame.protocol_error().rejected_frame_sequence() != host_sequence ||
            frame.protocol_error().error().code() != "invalid-work-frame") {
          failure = "empty source context.work_id was not rejected exactly";
        } else {
          xgc::adapter::v1::WorkResponse invalid;
          *invalid.mutable_header() = HostHeader(++host_sequence);
          auto* request = invalid.mutable_source_open_request();
          FillContext(request->mutable_context(), "source-invalid", "events",
                      "source-invalid-key", "source-invalid-digest");
          request->mutable_context()->mutable_subject()->set_key("wrong-scope");
          request->mutable_initial_credit()->set_messages(1);
          request->mutable_initial_credit()->set_bytes(8);
          if (!stream->Write(invalid)) {
            return grpc::Status::CANCELLED;
          }
          stage = 4;
        }
      } else if (stage == 4 && frame.has_source_open_result()) {
        const auto& result = frame.source_open_result();
        if (result.stream_id() != "source-invalid" || result.accepted() ||
            result.error().code() != "invalid-work-context") {
          failure = "invalid Host source request was not rejected by the SDK";
        } else if (!WriteSourceOpen(stream, ++host_sequence, "source-cancel")) {
          return grpc::Status::CANCELLED;
        } else {
          stage = 5;
        }
      } else if (stage == 5 && frame.has_source_open_result()) {
        const auto& result = frame.source_open_result();
        if (result.stream_id() != "source-cancel" || !result.accepted() ||
            result.has_initial_payload()) {
          failure = "empty successful SourceOpenResult was malformed";
        } else {
          xgc::adapter::v1::WorkResponse cancellation;
          *cancellation.mutable_header() = HostHeader(++host_sequence);
          cancellation.mutable_cancellation()->set_stream_id("source-cancel");
          cancellation.mutable_cancellation()->set_reason("source test cancel");
          if (!stream->Write(cancellation)) {
            return grpc::Status::CANCELLED;
          }
          stage = 6;
        }
      } else if (stage == 6 && frame.has_source_close()) {
        const auto& close = frame.source_close();
        if (close.stream_id() != "source-cancel" || close.final_sequence() != 0 ||
            close.error().class_() != xgc::adapter::v1::ERROR_CLASS_CANCELLED) {
          failure = "source cancellation was not acknowledged with SourceClose";
        } else {
          xgc::adapter::v1::WorkResponse late_credit;
          *late_credit.mutable_header() = HostHeader(++host_sequence);
          auto* credit = late_credit.mutable_source_credit();
          credit->set_stream_id("source-cancel");
          credit->mutable_grant()->set_messages(1);
          credit->mutable_grant()->set_bytes(8);
          credit->set_acknowledged_sequence(0);
          if (!stream->Write(late_credit) ||
              !WriteSourceOpen(stream, ++host_sequence, "source-after-late")) {
            return grpc::Status::CANCELLED;
          }
          stage = 7;
        }
      } else if (stage == 7 && frame.has_source_open_result()) {
        const auto& result = frame.source_open_result();
        if (result.stream_id() != "source-after-late" || !result.accepted()) {
          failure = "late credit corrupted the next source open";
        } else {
          xgc::adapter::v1::WorkResponse invalid_credit;
          *invalid_credit.mutable_header() = HostHeader(++host_sequence);
          auto* credit = invalid_credit.mutable_source_credit();
          credit->set_stream_id("source-after-late");
          credit->mutable_grant()->set_bytes(8);
          if (!stream->Write(invalid_credit)) {
            return grpc::Status::CANCELLED;
          }
          stage = 8;
        }
      } else if (stage == 8 && frame.has_protocol_error()) {
        if (frame.protocol_error().rejected_frame_sequence() != host_sequence ||
            frame.protocol_error().error().code() != "invalid-work-frame") {
          failure = "invalid SourceCredit did not produce an exact protocol error";
        } else {
          std::lock_guard<std::mutex> lock(mutex_);
          source_semantics_complete_ = true;
          condition_.notify_all();
          stage = 9;
        }
      }
      if (!failure.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        source_semantics_error_ = failure;
        condition_.notify_all();
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, failure);
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status RunSourceFrameLimit(WorkStream* stream, std::uint64_t connection_epoch) {
    if (!WriteSourceOpen(stream, 1, "source-frame-limit", 2, 2048)) {
      return grpc::Status::CANCELLED;
    }
    std::uint64_t last_work_sequence = 1;
    bool cancellation_sent = false;
    xgc::adapter::v1::WorkRequest frame;
    while (stream->Read(&frame)) {
      bool send_cancellation = false;
      if (!ValidateHeader(frame.header(), connection_epoch, last_work_sequence)) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "invalid frame-limit Work header");
      }
      last_work_sequence = frame.header().frame_sequence();
      if (frame.has_protocol_error()) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "unexpected frame-limit protocol error");
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (frame.has_source_open_result() &&
            frame.source_open_result().stream_id() == "source-frame-limit" &&
            frame.source_open_result().accepted()) {
          source_frame_opened_ = true;
        } else if (frame.has_source_data() &&
                   frame.source_data().stream_id() == "source-frame-limit" &&
                   frame.source_data().items_size() == 1 &&
                   frame.source_data().items(0).size() == 32) {
          source_frame_data_ = true;
          if (!cancellation_sent) {
            cancellation_sent = true;
            send_cancellation = true;
          }
        } else if (frame.has_source_close() &&
                   frame.source_close().stream_id() == "source-frame-limit") {
          source_frame_closed_ = true;
        }
      }
      if (send_cancellation) {
        xgc::adapter::v1::WorkResponse cancellation;
        *cancellation.mutable_header() = HostHeader(2, connection_epoch);
        cancellation.mutable_cancellation()->set_stream_id("source-frame-limit");
        cancellation.mutable_cancellation()->set_reason("raced local SourceClose");
        if (!stream->Write(cancellation)) {
          return grpc::Status::CANCELLED;
        }
      }
      condition_.notify_all();
    }
    return grpc::Status::OK;
  }

  xgc::adapter::v1::SessionHeader HostHeader(std::uint64_t sequence) const {
    std::uint64_t connection_epoch = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      connection_epoch =
          connection_epoch_ == 0 ? work_connection_epoch_ : connection_epoch_;
    }
    return HostHeader(sequence, connection_epoch);
  }

  xgc::adapter::v1::SessionHeader HostHeader(std::uint64_t sequence,
                                             std::uint64_t connection_epoch) const {
    xgc::adapter::v1::SessionHeader header;
    header.set_instance_id(kInstanceId);
    header.set_session_id(kSessionId);
    header.set_process_generation(kProcessGeneration);
    header.set_session_generation(kSessionGeneration);
    header.set_runtime_epoch(kRuntimeEpoch);
    header.set_connection_epoch(connection_epoch);
    header.set_frame_sequence(sequence);
    return header;
  }

  bool ValidateHeader(const xgc::adapter::v1::SessionHeader& header,
                      std::uint64_t expected_connection_epoch,
                      std::uint64_t previous_sequence) const {
    return header.instance_id() == kInstanceId && header.session_id() == kSessionId &&
           header.process_generation() == kProcessGeneration &&
           header.session_generation() == kSessionGeneration &&
           header.runtime_epoch() == kRuntimeEpoch && expected_connection_epoch != 0 &&
           header.connection_epoch() == expected_connection_epoch &&
           header.frame_sequence() == previous_sequence + 1;
  }

  template <typename RepeatedStatuses>
  bool HasReadyContract(const RepeatedStatuses& statuses) const {
    for (const auto& status : statuses) {
      if (status.capability_id() == kCapabilityId &&
          status.contract_version() == kContractVersion &&
          status.contract_digest() == kContractDigest &&
          status.state() == xgc::adapter::v1::CAPABILITY_STATE_READY) {
        return true;
      }
    }
    return false;
  }

  bool WriteUnary(grpc::ServerReaderWriter<xgc::adapter::v1::WorkResponse,
                                           xgc::adapter::v1::WorkRequest>* stream,
                  std::uint64_t sequence, const std::string& work_id = "unary-1",
                  const std::string& request_digest = "unary-digest",
                  bool canonical_digest = true) {
    xgc::adapter::v1::WorkResponse response;
    *response.mutable_header() = HostHeader(sequence);
    auto* request = response.mutable_unary_request();
    FillContext(request->mutable_context(), work_id, "query", work_id + "-key",
                request_digest);
    if (!canonical_digest) {
      request->mutable_context()->set_request_digest(request_digest);
    }
    *request->mutable_input() = TestPayload("unary-input");
    return stream->Write(response);
  }

  bool WriteOperation(grpc::ServerReaderWriter<xgc::adapter::v1::WorkResponse,
                                               xgc::adapter::v1::WorkRequest>* stream,
                      std::uint64_t sequence,
                      const std::string& work_id = "operation-1",
                      const std::string& request_digest = "operation-digest",
                      bool canonical_digest = true) {
    xgc::adapter::v1::WorkResponse response;
    *response.mutable_header() = HostHeader(sequence);
    auto* request = response.mutable_operation_request();
    FillContext(request->mutable_context(), work_id, "command", work_id + "-key",
                request_digest);
    if (!canonical_digest) {
      request->mutable_context()->set_request_digest(request_digest);
    }
    *request->mutable_input() = TestPayload("operation-input");
    return stream->Write(response);
  }

  bool WriteSourceOpen(grpc::ServerReaderWriter<xgc::adapter::v1::WorkResponse,
                                                xgc::adapter::v1::WorkRequest>* stream,
                       std::uint64_t sequence, const std::string& work_id,
                       std::uint64_t messages = 1, std::uint64_t bytes = 8) {
    xgc::adapter::v1::WorkResponse response;
    *response.mutable_header() = HostHeader(sequence);
    auto* request = response.mutable_source_open_request();
    FillContext(request->mutable_context(), work_id, "events", work_id + "-key",
                work_id + "-digest");
    request->mutable_initial_credit()->set_messages(messages);
    request->mutable_initial_credit()->set_bytes(bytes);
    return stream->Write(response);
  }

  void FillContext(xgc::adapter::v1::WorkContext* context, const std::string& work_id,
                   const std::string& endpoint_id, const std::string& idempotency_key,
                   const std::string& request_digest) const {
    context->set_work_id(work_id);
    context->set_capability_id(kCapabilityId);
    context->set_contract_version(kContractVersion);
    context->set_contract_digest(kContractDigest);
    context->set_endpoint_id(endpoint_id);
    context->set_spec_revision(kSpecRevision);
    context->mutable_deadline()->set_deadline_unix_nanos(NowNanos() + 5000000000LL);
    context->mutable_deadline()->set_ttl_ms(1000);
    context->set_idempotency_key(idempotency_key);
    context->set_request_digest(TestDigest(request_digest));
    context->mutable_subject()->set_kind("native-resource");
    context->mutable_subject()->set_key(
        "sha256:2222222222222222222222222222222222222222222222222222222222222222");
    (*context->mutable_subject()->mutable_attributes())["resource-id"] = "resource-1";
  }

  xgc::adapter::v1::AdapterInstanceSpec spec_;
  xgc::adapter::v1::AdapterInstanceSpec replacement_spec_;
  Scenario scenario_ = Scenario::kFullExchange;
  std::atomic<int> registration_attempts_{0};
  std::atomic<int> registrations_{0};
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::uint64_t connection_epoch_ = 0;
  std::uint64_t work_connection_epoch_ = 0;
  std::uint64_t last_control_epoch_ = 0;
  std::uint64_t spec_applied_epoch_ = 0;
  std::vector<std::uint64_t> control_epochs_;
  std::vector<std::uint64_t> paired_epochs_;
  bool control_attached_ = false;
  bool work_attach_first_ = false;
  bool spec_applied_ = false;
  bool ready_heartbeat_ = false;
  bool cancellation_sent_ = false;
  bool scenario_cancellation_sent_ = false;
  bool pre_cancelled_terminal_received_ = false;
  bool race_terminal_received_ = false;
  bool replacement_sent_ = false;
  bool source_opened_ = false;
  bool release_source_credit_ = false;
  bool source_credit_sent_ = false;
  bool source_closed_ = false;
  bool source_semantics_complete_ = false;
  std::string source_semantics_error_;
  bool source_frame_opened_ = false;
  bool source_frame_data_ = false;
  bool source_frame_closed_ = false;
  bool handler_result_validation_complete_ = false;
  std::string handler_result_validation_error_;
  bool terminal_acknowledgement_complete_ = false;
  std::string terminal_acknowledgement_error_;
  bool ready_failure_result_ = false;
  std::string unary_value_;
  xgc::adapter::v1::OperationPhase pre_cancelled_terminal_ =
      xgc::adapter::v1::OPERATION_PHASE_UNSPECIFIED;
  xgc::adapter::v1::OperationPhase race_terminal_ =
      xgc::adapter::v1::OPERATION_PHASE_UNSPECIFIED;
  int operation_terminals_ = 0;
  int source_items_ = 0;
  int spec_applications_ = 0;
};

class AdapterRuntimeClientTest : public testing::Test {
 protected:
  virtual FakeRuntimeLink::Scenario scenario() const {
    return FakeRuntimeLink::Scenario::kFullExchange;
  }

  void SetUp() override {
    const std::string suffix = std::to_string(static_cast<long long>(::getpid()));
    socket_path_ = "/tmp/xgc2-adapter-runtime-test-" + suffix + ".sock";
    bootstrap_path_ = "/tmp/xgc2-adapter-runtime-test-" + suffix + ".bootstrap.pb";
    std::remove(socket_path_.c_str());
    std::remove(bootstrap_path_.c_str());
    bootstrap_ = TestBootstrap("unix:" + socket_path_);

    std::ofstream output(bootstrap_path_, std::ios::out | std::ios::binary);
    ASSERT_TRUE(bootstrap_.SerializeToOstream(&output));
    output.close();
    ASSERT_EQ(::chmod(bootstrap_path_.c_str(), 0600), 0);

    service_.reset(new FakeRuntimeLink(bootstrap_.initial_spec(), scenario()));
    StartRuntimeServer();
  }

  void RestartRuntimeServer() {
    StopRuntimeServer();
    StartRuntimeServer();
  }

  void StopRuntimeServer() {
    ASSERT_NE(server_, nullptr);
    server_->Shutdown(std::chrono::system_clock::now());
    server_->Wait();
    server_.reset();
    std::remove(socket_path_.c_str());
  }

  void StartRuntimeServer() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("unix:" + socket_path_, grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    std::remove(socket_path_.c_str());
    std::remove(bootstrap_path_.c_str());
  }

  xgc::adapter::v1::AdapterProcessBootstrap bootstrap_;
  std::unique_ptr<FakeRuntimeLink> service_;
  std::unique_ptr<grpc::Server> server_;
  std::string socket_path_;
  std::string bootstrap_path_;
};

}  // namespace
