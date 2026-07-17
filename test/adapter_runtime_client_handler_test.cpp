#include "adapter_runtime_client_test_support.hpp"

namespace {

TEST(TerminalDigestTest, DomainSeparatesDifferentProtobufTypesWithIdenticalWireBytes) {
  xgc::adapter::v1::UnaryResponse unary;
  unary.set_work_id("same");
  xgc::adapter::v1::SourceClose source;
  source.set_stream_id("same");
  ASSERT_EQ(unary.SerializeAsString(), source.SerializeAsString());

  std::string unary_digest;
  std::string source_digest;
  std::string error;
  ASSERT_TRUE(
      xgc2::adapter_runtime::internal::TerminalDigest(unary, &unary_digest, &error))
      << error;
  ASSERT_TRUE(
      xgc2::adapter_runtime::internal::TerminalDigest(source, &source_digest, &error))
      << error;
  EXPECT_EQ(unary_digest,
            "sha256:4a74ba9392db1972827641904973d90e1d95404cdde45a1515d20fe2dab0a191");
  EXPECT_EQ(source_digest,
            "sha256:184ab3cd9e20d8434ff14077ea5b5f7e39fb917c8c3530a2a517f1320c2f8f4b");
  EXPECT_NE(unary_digest, source_digest);
}

class HandlerResultValidationTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kHandlerResultValidation;
  }
};

TEST_F(HandlerResultValidationTest,
       NormalizesMalformedResultsAndPreflightsTerminalFrames) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> unary_calls{0};
  std::atomic<int> operation_calls{0};
  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.unary = [&](const xgc::adapter::v1::UnaryRequest& request,
                      const xgc2::adapter_runtime::CancellationToken&) {
    ++unary_calls;
    if (request.context().work_id() == "unary-malformed") {
      auto output = TestPayload("{");
      output.set_encoding(xgc::v1::PAYLOAD_ENCODING_JSON);
      return xgc2::adapter_runtime::UnaryResult::Success(std::move(output));
    }
    return xgc2::adapter_runtime::UnaryResult::Success(
        TestPayload(std::string(1024, 'u')));
  };
  enabled.operation = [&](const xgc::adapter::v1::OperationRequest& request,
                          const xgc2::adapter_runtime::CancellationToken&) {
    ++operation_calls;
    if (request.context().work_id() == "operation-malformed") {
      auto result = xgc2::adapter_runtime::OperationResult::Success();
      result.error.set_class_(xgc::adapter::v1::ERROR_CLASS_PERMANENT);
      result.error.set_code("unexpected-error");
      result.error.set_message("success must not contain an error");
      return result;
    }
    if (request.context().work_id() == "operation-phase-mismatch") {
      auto result = xgc2::adapter_runtime::OperationResult::Rejected(
          "application-rejected", "application rejected operation");
      result.error.set_class_(xgc::adapter::v1::ERROR_CLASS_TRANSIENT);
      return result;
    }
    return xgc2::adapter_runtime::OperationResult::Success(
        TestPayload(std::string(1024, 'o')), true);
  };
  enabled.source_open = [](const xgc::adapter::v1::SourceOpenRequest&,
                           const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::SourceOpenDecision::Accept();
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
  EXPECT_TRUE(service_->WaitForHandlerResultValidation())
      << service_->handler_result_validation_error();
  EXPECT_EQ(unary_calls.load(), 2);
  EXPECT_EQ(operation_calls.load(), 3);
  client.Stop();
}

class TerminalAcknowledgementTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kTerminalAcknowledgement;
  }
};

TEST_F(TerminalAcknowledgementTest,
       ReleasesExactWorkAndSourceTerminalsWithoutEviction) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;
  config.maximum_terminal_replay = 1;
  config.maximum_terminal_replay_bytes = 1024U * 1024U + 142U;
  config.maximum_source_streams = 1;
  config.maximum_source_tombstones = 1;
  config.maximum_source_state_bytes = 2U * 1024U * 1024U + 142U;

  std::atomic<int> unary_calls{0};
  std::atomic<int> source_calls{0};
  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.start = [](const xgc::adapter::v1::AdapterInstanceSpec&,
                     const xgc::adapter::v1::EnabledCapability&,
                     std::string*) { return true; };
  enabled.unary = [&](const xgc::adapter::v1::UnaryRequest& request,
                      const xgc2::adapter_runtime::CancellationToken&) {
    ++unary_calls;
    if (request.context().work_id() == "ack-active") {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return xgc2::adapter_runtime::UnaryResult::Success(TestPayload("ack-result"));
  };
  enabled.operation = [](const xgc::adapter::v1::OperationRequest&,
                         const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::OperationResult::Success();
  };
  enabled.source_open = [&](const xgc::adapter::v1::SourceOpenRequest&,
                            const xgc2::adapter_runtime::CancellationToken&) {
    ++source_calls;
    return xgc2::adapter_runtime::SourceOpenDecision::Reject(
        xgc::adapter::v1::ERROR_CLASS_REJECTED, "test-rejection",
        "terminal acknowledgement fixture rejection");
  };
  std::string bind_error;
  ASSERT_TRUE(config.BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                    std::move(enabled), &bind_error))
      << bind_error;
  ASSERT_TRUE(config.BindCapability("test.disabled", kContractVersion,
                                    kDisabledContractDigest, {}, &bind_error))
      << bind_error;

  xgc2::adapter_runtime::Client client(std::move(config), {});
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  EXPECT_TRUE(service_->WaitForTerminalAcknowledgement())
      << service_->terminal_acknowledgement_error();
  EXPECT_EQ(unary_calls.load(), 3);
  EXPECT_EQ(source_calls.load(), 2);
  client.Stop();
}

}  // namespace
