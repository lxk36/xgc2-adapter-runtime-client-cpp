#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "xgc/adapter/v1/adapter.grpc.pb.h"
#include "xgc2/adapter_link/client.hpp"

namespace {

using xgc::adapter::v1::AdapterLink;

constexpr std::uint64_t kRegistryFingerprint = 0x12345678ULL;
constexpr std::uint64_t kPlanRevision = 7;
constexpr std::uint32_t kMessageId = 42;

xgc::adapter::v1::ProfileAdvertisement TestProfile() {
  xgc::adapter::v1::ProfileAdvertisement profile;
  profile.set_profile_id("test.robot.v1");
  profile.set_profile_digest("profile-digest");
  auto* channel = profile.add_channels();
  channel->set_channel_id("robot.command");
  channel->set_kind(xgc::adapter::v1::CHANNEL_KIND_OPERATION);
  channel->set_input_message_id(kMessageId);
  return profile;
}

class FakeAdapterLink final : public AdapterLink::Service {
 public:
  grpc::Status RegisterAdapter(
      grpc::ServerContext*, const xgc::adapter::v1::RegisterAdapterRequest* request,
      xgc::adapter::v1::RegisterAdapterResponse* response) override {
    if (reject_registrations_after_first_.load() && registrations_.load() > 0) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                          "test rejected replacement registration");
    }
    if (request->bootstrap_token() != "secret-token" ||
        request->registry_fingerprint() != kRegistryFingerprint) {
      return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "bad registration");
    }
    response->set_accepted(true);
    response->set_core_id("test-core");
    response->set_session_id("test-session");
    response->set_selected_protocol_version(1);
    response->set_registry_fingerprint(kRegistryFingerprint);
    response->set_plan_revision(current_revision_.load());
    response->set_heartbeat_interval_ms(25);
    response->set_max_batch_bytes(1024 * 1024);
    ++registrations_;
    condition_.notify_all();
    return grpc::Status::OK;
  }

  grpc::Status Heartbeat(grpc::ServerContext*,
                         const xgc::adapter::v1::HeartbeatRequest* request,
                         xgc::adapter::v1::HeartbeatResponse* response) override {
    response->set_accepted(request->session_id() == "test-session");
    const auto revision = current_revision_.load();
    if (request->applied_plan_revision() == revision && revision != kPlanRevision &&
        reload_ack_delay_ms_.load() > 0) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(reload_ack_delay_ms_.load()));
    }
    response->set_current_plan_revision(revision);
    response->set_reload_plan(request->applied_plan_revision() != revision);
    ++heartbeats_;
    return grpc::Status::OK;
  }

  grpc::Status GetAdapterPlan(grpc::ServerContext*,
                              const xgc::adapter::v1::GetAdapterPlanRequest*,
                              xgc::adapter::v1::AdapterPlan* response) override {
    if (current_revision_.load() == 8 && block_revision_eight_plan_.load()) {
      std::unique_lock<std::mutex> lock(plan_mutex_);
      revision_eight_plan_entered_ = true;
      plan_condition_.notify_all();
      plan_condition_.wait(lock, [this] { return release_revision_eight_plan_; });
    }
    response->set_accepted(true);
    response->set_revision(current_revision_.load());
    response->set_asset_digest("asset-digest");
    auto* robot = response->add_robots();
    robot->set_robot_id("robot-1");
    robot->set_profile_id("test.robot.v1");
    robot->set_profile_digest("profile-digest");
    auto* channel = robot->add_channels();
    channel->set_channel_id("robot.command");
    channel->set_enabled(true);
    return grpc::Status::OK;
  }

  grpc::Status PushTelemetry(grpc::ServerContext*,
                             const xgc::adapter::v1::TelemetryBatch* request,
                             xgc::adapter::v1::BatchAck* response) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      telemetry_count_ += request->messages_size();
    }
    response->set_accepted(true);
    response->set_batch_id(request->batch_id());
    if (partial_telemetry_ack_.load()) {
      response->set_accepted_count(0);
      response->set_rejected_count(request->messages_size());
    } else {
      response->set_accepted_count(request->messages_size());
    }
    condition_.notify_all();
    return grpc::Status::OK;
  }

  grpc::Status StreamOperations(
      grpc::ServerContext* context, const xgc::adapter::v1::OperationStreamRequest*,
      grpc::ServerWriter<xgc::adapter::v1::OperationRequest>* writer) override {
    while (!context->IsCancelled()) {
      xgc::adapter::v1::OperationRequest operation;
      bool have_operation = false;
      {
        std::unique_lock<std::mutex> lock(operation_mutex_);
        operation_condition_.wait_for(lock, std::chrono::milliseconds(5),
                                      [this] { return !queued_operations_.empty(); });
        if (!queued_operations_.empty()) {
          operation = std::move(queued_operations_.front());
          queued_operations_.pop_front();
          have_operation = true;
        }
      }
      if (have_operation) {
        if (!writer->Write(operation)) {
          return grpc::Status::CANCELLED;
        }
        ++operations_written_;
        condition_.notify_all();
      }
    }
    return grpc::Status::CANCELLED;
  }

  grpc::Status ReportOperationEvents(
      grpc::ServerContext*, const xgc::adapter::v1::OperationEventBatch* request,
      xgc::adapter::v1::BatchAck* response) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto& event : request->events()) {
        phases_.push_back(event.phase());
      }
    }
    response->set_accepted(true);
    response->set_batch_id(request->batch_id());
    response->set_accepted_count(request->events_size());
    condition_.notify_all();
    return grpc::Status::OK;
  }

  bool WaitForTelemetryAndTerminal() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [this] {
      if (telemetry_count_ < 1) {
        return false;
      }
      for (const auto phase : phases_) {
        if (phase == xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED) {
          return true;
        }
      }
      return false;
    });
  }

  int heartbeats() const { return heartbeats_.load(); }

  void QueueOperation(const std::string& operation_id, std::uint64_t revision) {
    xgc::adapter::v1::OperationRequest operation;
    operation.set_operation_id(operation_id);
    operation.set_issued_unix_nanos(NowNanos());
    operation.set_ttl_ms(5000);
    operation.set_plan_revision(revision);
    operation.mutable_message()->set_robot_id("robot-1");
    operation.mutable_message()->set_channel_id("robot.command");
    operation.mutable_message()->set_message_id(kMessageId);
    operation.mutable_message()->set_payload("payload");
    {
      std::lock_guard<std::mutex> lock(operation_mutex_);
      queued_operations_.push_back(std::move(operation));
    }
    operation_condition_.notify_all();
  }

  bool WaitForOperationsWritten(int count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [this, count] {
      return operations_written_.load() >= count;
    });
  }

  bool WaitForRegistrations(int count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [this, count] {
      return registrations_.load() >= count;
    });
  }

  void SetPartialTelemetryAck(bool enabled) { partial_telemetry_ack_.store(enabled); }

  void SetRevision(std::uint64_t revision) { current_revision_.store(revision); }

  void SetReloadAckDelay(std::uint32_t milliseconds) {
    reload_ack_delay_ms_.store(milliseconds);
  }

  void RejectRegistrationsAfterFirst(bool enabled) {
    reject_registrations_after_first_.store(enabled);
  }

  void BlockRevisionEightPlan() { block_revision_eight_plan_.store(true); }

  bool WaitForRevisionEightPlanRequest() {
    std::unique_lock<std::mutex> lock(plan_mutex_);
    return plan_condition_.wait_for(lock, std::chrono::seconds(5),
                                    [this] { return revision_eight_plan_entered_; });
  }

  void ReleaseRevisionEightPlan() {
    {
      std::lock_guard<std::mutex> lock(plan_mutex_);
      release_revision_eight_plan_ = true;
    }
    plan_condition_.notify_all();
  }

 private:
  static std::int64_t NowNanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  std::atomic<int> heartbeats_{0};
  std::atomic<int> registrations_{0};
  std::atomic<int> operations_written_{0};
  std::atomic<std::uint64_t> current_revision_{kPlanRevision};
  std::atomic<std::uint32_t> reload_ack_delay_ms_{0};
  std::atomic<bool> partial_telemetry_ack_{false};
  std::atomic<bool> reject_registrations_after_first_{false};
  std::atomic<bool> block_revision_eight_plan_{false};
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::mutex operation_mutex_;
  std::condition_variable operation_condition_;
  std::deque<xgc::adapter::v1::OperationRequest> queued_operations_;
  std::mutex plan_mutex_;
  std::condition_variable plan_condition_;
  bool revision_eight_plan_entered_ = false;
  bool release_revision_eight_plan_ = false;
  int telemetry_count_ = 0;
  std::vector<xgc::adapter::v1::OperationPhase> phases_;
};

class AdapterLinkClientTest : public testing::Test {
 protected:
  void SetUp() override {
    const auto suffix = std::to_string(static_cast<long long>(::getpid()));
    socket_path_ = "/tmp/xgc2-adapter-client-test-" + suffix + ".sock";
    token_path_ = "/tmp/xgc2-adapter-client-test-" + suffix + ".token";
    std::remove(socket_path_.c_str());
    std::ofstream token(token_path_);
    token << "  secret-token\n";
    token.close();

    grpc::ServerBuilder builder;
    builder.AddListeningPort("unix:" + socket_path_, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    std::remove(socket_path_.c_str());
    std::remove(token_path_.c_str());
  }

  FakeAdapterLink service_;
  std::unique_ptr<grpc::Server> server_;
  std::string socket_path_;
  std::string token_path_;
};

TEST_F(AdapterLinkClientTest, RunsCompleteSessionAndStopsDeterministically) {
  xgc2::adapter_link::ClientConfig config;
  config.adapter_id = "test-adapter";
  config.socket_path = socket_path_;
  config.bootstrap_token_file = token_path_;
  config.native_protocol = "test";
  config.software_version = "0.1.0-test";
  config.supported_protocol_versions = {1};
  config.registry_fingerprint = kRegistryFingerprint;
  config.supported_profiles = {TestProfile()};
  config.initial_connect_timeout_ms = 2000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> applied{0};
  std::atomic<int> cleared{0};
  std::atomic<int> handled{0};
  xgc2::adapter_link::ClientCallbacks callbacks;
  callbacks.validate_and_apply_plan = [&](const xgc::adapter::v1::AdapterPlan& plan,
                                          std::string*) {
    ++applied;
    return plan.robots_size() == 1;
  };
  callbacks.clear_plan = [&] { ++cleared; };
  callbacks.handle_operation = [&](const xgc::adapter::v1::OperationRequest&) {
    ++handled;
    return xgc2::adapter_link::OperationExecutionResult::Succeeded("done");
  };

  xgc2::adapter_link::Client client(std::move(config), std::move(callbacks));
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  EXPECT_EQ(client.session().state, xgc2::adapter_link::ClientState::kReady);
  EXPECT_EQ(client.session().plan_revision, kPlanRevision);

  xgc::v1::Message telemetry;
  telemetry.set_robot_id("robot-1");
  telemetry.set_channel_id("robot.telemetry");
  telemetry.set_payload("value");
  EXPECT_TRUE(client.Publish(kPlanRevision, std::move(telemetry)));
  service_.QueueOperation("operation-1", kPlanRevision);
  ASSERT_TRUE(service_.WaitForTelemetryAndTerminal());
  EXPECT_EQ(applied.load(), 1);
  EXPECT_EQ(handled.load(), 1);
  EXPECT_GE(service_.heartbeats(), 1);

  client.Stop();
  EXPECT_EQ(client.session().state, xgc2::adapter_link::ClientState::kStopped);
  EXPECT_EQ(cleared.load(), 1);
  client.Stop();
  EXPECT_EQ(cleared.load(), 1);
}

TEST_F(AdapterLinkClientTest, PartialTelemetryAckInvalidatesAndReconnects) {
  xgc2::adapter_link::ClientConfig config;
  config.adapter_id = "test-adapter";
  config.socket_path = socket_path_;
  config.bootstrap_token_file = token_path_;
  config.native_protocol = "test";
  config.software_version = "0.1.0-test";
  config.supported_protocol_versions = {1};
  config.registry_fingerprint = kRegistryFingerprint;
  config.supported_profiles = {TestProfile()};
  config.rpc_timeout_ms = 1000;

  std::atomic<int> applied{0};
  std::atomic<int> cleared{0};
  xgc2::adapter_link::ClientCallbacks callbacks;
  callbacks.validate_and_apply_plan = [&](const xgc::adapter::v1::AdapterPlan&,
                                          std::string*) {
    ++applied;
    return true;
  };
  callbacks.clear_plan = [&] { ++cleared; };

  xgc2::adapter_link::Client client(std::move(config), std::move(callbacks));
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  service_.SetPartialTelemetryAck(true);
  xgc::v1::Message telemetry;
  telemetry.set_robot_id("robot-1");
  telemetry.set_channel_id("robot.telemetry");
  telemetry.set_payload("value");
  ASSERT_TRUE(client.Publish(kPlanRevision, std::move(telemetry)));
  ASSERT_TRUE(service_.WaitForRegistrations(2));
  const auto reconnect_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (applied.load() < 2 && std::chrono::steady_clock::now() < reconnect_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_GE(applied.load(), 2);
  EXPECT_GE(cleared.load(), 1);
  client.Stop();
}

TEST_F(AdapterLinkClientTest, ReloadIsAtomicAgainstOldPlanOperations) {
  xgc2::adapter_link::ClientConfig config;
  config.adapter_id = "test-adapter";
  config.socket_path = socket_path_;
  config.bootstrap_token_file = token_path_;
  config.native_protocol = "test";
  config.software_version = "0.1.0-test";
  config.supported_protocol_versions = {1};
  config.registry_fingerprint = kRegistryFingerprint;
  config.supported_profiles = {TestProfile()};
  config.rpc_timeout_ms = 1000;

  std::mutex apply_mutex;
  std::condition_variable apply_condition;
  bool reload_apply_entered = false;
  bool release_reload_apply = false;
  std::atomic<int> handled{0};
  xgc2::adapter_link::ClientCallbacks callbacks;
  callbacks.validate_and_apply_plan = [&](const xgc::adapter::v1::AdapterPlan& plan,
                                          std::string*) {
    if (plan.revision() != 8) {
      return true;
    }
    std::unique_lock<std::mutex> lock(apply_mutex);
    reload_apply_entered = true;
    apply_condition.notify_all();
    apply_condition.wait(lock, [&] { return release_reload_apply; });
    return true;
  };
  callbacks.handle_operation = [&](const xgc::adapter::v1::OperationRequest&) {
    ++handled;
    return xgc2::adapter_link::OperationExecutionResult::Succeeded();
  };

  xgc2::adapter_link::Client client(std::move(config), std::move(callbacks));
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  service_.SetReloadAckDelay(250);
  service_.SetRevision(8);
  {
    std::unique_lock<std::mutex> lock(apply_mutex);
    ASSERT_TRUE(apply_condition.wait_for(lock, std::chrono::seconds(5),
                                         [&] { return reload_apply_entered; }));
  }
  service_.QueueOperation("stale-operation", kPlanRevision);
  ASSERT_TRUE(service_.WaitForOperationsWritten(1));
  {
    std::lock_guard<std::mutex> lock(apply_mutex);
    release_reload_apply = true;
  }
  apply_condition.notify_all();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(handled.load(), 0);

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (client.session().plan_revision != 8 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_EQ(client.session().plan_revision, 8U);
  EXPECT_EQ(handled.load(), 0);
  client.Stop();
}

TEST_F(AdapterLinkClientTest, StaleReloadDoesNotApplyAfterSessionInvalidation) {
  xgc2::adapter_link::ClientConfig config;
  config.adapter_id = "test-adapter";
  config.socket_path = socket_path_;
  config.bootstrap_token_file = token_path_;
  config.native_protocol = "test";
  config.software_version = "0.1.0-test";
  config.supported_protocol_versions = {1};
  config.registry_fingerprint = kRegistryFingerprint;
  config.supported_profiles = {TestProfile()};
  config.rpc_timeout_ms = 1000;

  std::atomic<int> revision_eight_applies{0};
  xgc2::adapter_link::ClientCallbacks callbacks;
  callbacks.validate_and_apply_plan = [&](const xgc::adapter::v1::AdapterPlan& plan,
                                          std::string*) {
    if (plan.revision() == 8) {
      ++revision_eight_applies;
    }
    return true;
  };

  xgc2::adapter_link::Client client(std::move(config), std::move(callbacks));
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  service_.RejectRegistrationsAfterFirst(true);
  service_.BlockRevisionEightPlan();
  service_.SetRevision(8);
  ASSERT_TRUE(service_.WaitForRevisionEightPlanRequest());

  service_.SetPartialTelemetryAck(true);
  xgc::v1::Message telemetry;
  telemetry.set_robot_id("robot-1");
  telemetry.set_channel_id("robot.telemetry");
  telemetry.set_payload("value");
  ASSERT_TRUE(client.Publish(kPlanRevision, std::move(telemetry)));
  const auto invalidation_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!client.session().session_id.empty() &&
         std::chrono::steady_clock::now() < invalidation_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(client.session().session_id.empty());

  service_.ReleaseRevisionEightPlan();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(revision_eight_applies.load(), 0);
  client.Stop();
}

TEST(AdapterLinkTokenTest, RejectsRelativeAndEmptyTokenFiles) {
  EXPECT_THROW(xgc2::adapter_link::ReadBootstrapTokenFile("relative.token"),
               std::invalid_argument);
}

}  // namespace
