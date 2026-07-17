#include "adapter_runtime_client_test_support.hpp"

namespace {

class PreCancelledOperationTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kPreCancelledOperation;
  }
};

TEST_F(PreCancelledOperationTest, DoesNotInvokeCallbackCancelledBeforeDispatch) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;
  config.dispatch_workers = 1;

  std::mutex callback_mutex;
  std::condition_variable callback_condition;
  bool blocking_callback_started = false;
  bool blocking_callback_observed_cancellation = false;
  bool release_blocking_callback = false;
  std::atomic<int> pre_cancelled_calls{0};

  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [&](const xgc::adapter::v1::OperationRequest& request,
                          const xgc2::adapter_runtime::CancellationToken& token) {
    if (request.context().work_id() == "pre-cancelled-operation") {
      ++pre_cancelled_calls;
      return xgc2::adapter_runtime::OperationResult::Success();
    }
    std::unique_lock<std::mutex> lock(callback_mutex);
    blocking_callback_started = true;
    callback_condition.notify_all();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!token.IsCancellationRequested() && !release_blocking_callback &&
           std::chrono::steady_clock::now() < deadline) {
      callback_condition.wait_for(lock, std::chrono::milliseconds(1));
    }
    blocking_callback_observed_cancellation = token.IsCancellationRequested();
    callback_condition.notify_all();
    return xgc2::adapter_runtime::OperationResult::Success();
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
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;

  bool callback_started = false;
  {
    std::unique_lock<std::mutex> lock(callback_mutex);
    callback_started = callback_condition.wait_for(
        lock, std::chrono::seconds(5), [&] { return blocking_callback_started; });
  }
  const bool cancellation_sent = service_->WaitForScenarioCancellation();
  bool cancellation_consumed = false;
  {
    std::unique_lock<std::mutex> lock(callback_mutex);
    cancellation_consumed = callback_condition.wait_for(
        lock, std::chrono::seconds(5),
        [&] { return blocking_callback_observed_cancellation; });
  }
  {
    std::lock_guard<std::mutex> lock(callback_mutex);
    release_blocking_callback = true;
  }
  callback_condition.notify_all();
  const bool terminal_received = service_->WaitForPreCancelledTerminal();

  EXPECT_TRUE(callback_started);
  EXPECT_TRUE(cancellation_sent);
  EXPECT_TRUE(cancellation_consumed);
  EXPECT_TRUE(terminal_received);
  EXPECT_EQ(pre_cancelled_calls.load(), 0);
  EXPECT_EQ(service_->pre_cancelled_terminal(),
            xgc::adapter::v1::OPERATION_PHASE_CANCELLED);
  client.Stop();
}

class CallbackCancellationRaceTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kCallbackCancellationRace;
  }
};

TEST_F(CallbackCancellationRaceTest,
       PreservesSuccessfulCallbackResultWhenCancellationRacesCompletion) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> operation_calls{0};
  std::atomic<bool> callback_observed_cancellation{false};
  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [&](const xgc::adapter::v1::OperationRequest&,
                          const xgc2::adapter_runtime::CancellationToken& token) {
    ++operation_calls;
    const auto successful_result = xgc2::adapter_runtime::OperationResult::Success();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!token.IsCancellationRequested() &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    callback_observed_cancellation.store(token.IsCancellationRequested());
    return successful_result;
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
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  const bool cancellation_sent = service_->WaitForScenarioCancellation();
  const bool terminal_received = service_->WaitForRaceTerminal();

  EXPECT_TRUE(cancellation_sent);
  EXPECT_TRUE(terminal_received);
  EXPECT_EQ(operation_calls.load(), 1);
  EXPECT_TRUE(callback_observed_cancellation.load());
  EXPECT_EQ(service_->race_terminal(), xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED);
  client.Stop();
}

}  // namespace
