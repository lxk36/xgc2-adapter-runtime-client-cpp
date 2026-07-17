#include <map>

#include "adapter_runtime_client_test_support.hpp"

namespace {

class SourceSemanticsTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kSourceSemantics;
  }
};

TEST_F(SourceSemanticsTest, HostOwnsOpenIdentityAndLateCreditCannotCorruptThePair) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> source_open_calls{0};
  std::atomic<int> invalid_source_calls{0};
  std::mutex close_mutex;
  std::map<std::string, int> close_counts;

  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [](const xgc::adapter::v1::OperationRequest&,
                         const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::OperationResult::Success();
  };
  enabled.source_open = [&](const xgc::adapter::v1::SourceOpenRequest& request,
                            const xgc2::adapter_runtime::CancellationToken&) {
    ++source_open_calls;
    EXPECT_EQ(request.context().capability_id(), kCapabilityId);
    EXPECT_EQ(request.context().contract_version(), kContractVersion);
    EXPECT_EQ(request.context().contract_digest(), kContractDigest);
    EXPECT_EQ(request.context().endpoint_id(), "events");
    EXPECT_EQ(request.context().spec_revision(), kSpecRevision);
    EXPECT_EQ(request.context().subject().kind(), "native-resource");
    EXPECT_EQ(request.context().subject().attributes().at("resource-id"), "resource-1");
    EXPECT_NE(request.context().subject().key(),
              bootstrap_.initial_spec().scope().key());
    EXPECT_EQ(request.initial_credit().messages(), 1U);
    EXPECT_EQ(request.initial_credit().bytes(), 8U);
    if (request.context().work_id() == "source-initial") {
      return xgc2::adapter_runtime::SourceOpenDecision::Accept(
          TestPayload("initial-value"));
    }
    if (request.context().work_id() == "source-initial-too-large") {
      return xgc2::adapter_runtime::SourceOpenDecision::Accept(
          TestPayload(std::string(33, 'x')));
    }
    if (request.context().work_id() == "source-reject") {
      return xgc2::adapter_runtime::SourceOpenDecision::Reject(
          xgc::adapter::v1::ERROR_CLASS_TRANSIENT, "app-reject",
          "native source is temporarily unavailable", 25);
    }
    if (request.context().work_id() == "source-cancel" ||
        request.context().work_id() == "source-after-late") {
      return xgc2::adapter_runtime::SourceOpenDecision::Accept();
    }
    ++invalid_source_calls;
    return xgc2::adapter_runtime::SourceOpenDecision::Reject(
        xgc::adapter::v1::ERROR_CLASS_PERMANENT, "unexpected-source",
        "invalid source reached the application callback");
  };
  enabled.source_closed = [&](const xgc::adapter::v1::SourceOpenRequest& request,
                              const xgc::adapter::v1::AdapterError&) {
    std::lock_guard<std::mutex> lock(close_mutex);
    ++close_counts[request.context().work_id()];
  };

  std::string bind_error;
  ASSERT_TRUE(config.BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                    std::move(enabled), &bind_error))
      << bind_error;
  ASSERT_TRUE(config.BindCapability("test.disabled", kContractVersion,
                                    kDisabledContractDigest, {}, &bind_error))
      << bind_error;

  xgc2::adapter_runtime::Client client(std::move(config), {});
  std::string start_error;
  ASSERT_TRUE(client.Start(&start_error)) << start_error;
  ASSERT_TRUE(service_->WaitForSourceSemantics()) << service_->source_semantics_error();
  EXPECT_EQ(source_open_calls.load(), 5);
  EXPECT_EQ(invalid_source_calls.load(), 0);
  {
    std::lock_guard<std::mutex> lock(close_mutex);
    EXPECT_EQ(close_counts["source-cancel"], 1);
    EXPECT_EQ(close_counts["source-reject"], 0);
    EXPECT_EQ(close_counts["source-initial-too-large"], 0);
    EXPECT_EQ(close_counts["source-invalid"], 0);
  }

  client.Stop();
  {
    std::lock_guard<std::mutex> lock(close_mutex);
    EXPECT_EQ(close_counts["source-initial"], 1);
    EXPECT_EQ(close_counts["source-cancel"], 1);
    EXPECT_EQ(close_counts["source-after-late"], 1);
    EXPECT_EQ(close_counts["source-reject"], 0);
    EXPECT_EQ(close_counts["source-initial-too-large"], 0);
    EXPECT_EQ(close_counts["source-invalid"], 0);
  }
}

class SourceFrameLimitTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kSourceFrameLimit;
  }
};

TEST_F(SourceFrameLimitTest, PreflightsCompleteFramesBeforeCreditOrLifecycleCommit) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> source_closes{0};
  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [](const xgc::adapter::v1::OperationRequest&,
                         const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::OperationResult::Success();
  };
  enabled.source_open = [](const xgc::adapter::v1::SourceOpenRequest& request,
                           const xgc2::adapter_runtime::CancellationToken&) {
    EXPECT_EQ(request.context().work_id(), "source-frame-limit");
    return xgc2::adapter_runtime::SourceOpenDecision::Accept();
  };
  enabled.source_closed = [&](const xgc::adapter::v1::SourceOpenRequest&,
                              const xgc::adapter::v1::AdapterError&) {
    ++source_closes;
  };
  std::string bind_error;
  ASSERT_TRUE(config.BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                    std::move(enabled), &bind_error))
      << bind_error;
  ASSERT_TRUE(config.BindCapability("test.disabled", kContractVersion,
                                    kDisabledContractDigest, {}, &bind_error))
      << bind_error;

  xgc2::adapter_runtime::Client client(std::move(config), {});
  std::string start_error;
  ASSERT_TRUE(client.Start(&start_error)) << start_error;
  ASSERT_TRUE(service_->WaitForSourceFrameOpen());

  EXPECT_EQ(client.PublishSource("source-frame-limit", {""}),
            xgc2::adapter_runtime::SourceWriteResult::kTooLarge);
  EXPECT_EQ(client.PublishSource("source-frame-limit", {std::string(1024, 'x')}),
            xgc2::adapter_runtime::SourceWriteResult::kTooLarge);
  EXPECT_EQ(client.PublishSource("source-frame-limit", {std::string(32, 'x')}),
            xgc2::adapter_runtime::SourceWriteResult::kAccepted);

  xgc::adapter::v1::AdapterError untyped;
  untyped.set_code("untyped");
  EXPECT_FALSE(client.CloseSource("source-frame-limit", &untyped));

  xgc::adapter::v1::AdapterError oversized;
  oversized.set_class_(xgc::adapter::v1::ERROR_CLASS_PERMANENT);
  oversized.set_code("native-failure");
  oversized.set_message(std::string(1024, 'x'));
  EXPECT_FALSE(client.CloseSource("source-frame-limit", &oversized));

  EXPECT_TRUE(client.CloseSource("source-frame-limit"));
  EXPECT_TRUE(service_->WaitForSourceFrameExchange());
  EXPECT_EQ(source_closes.load(), 0);
  client.Stop();
  EXPECT_EQ(source_closes.load(), 0);
}

}  // namespace
