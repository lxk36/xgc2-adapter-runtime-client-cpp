#include <google/protobuf/util/message_differencer.h>

#include "adapter_runtime_client_test_support.hpp"

namespace {

xgc2::adapter_runtime::CapabilityCallbacks ProtocolFixtureCallbacks() {
  xgc2::adapter_runtime::CapabilityCallbacks callbacks;
  callbacks.unary = [](const xgc::adapter::v1::UnaryRequest&,
                       const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success(TestPayload("protocol-fixture"));
  };
  callbacks.operation = [](const xgc::adapter::v1::OperationRequest&,
                           const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::OperationResult::Success();
  };
  callbacks.source_open = [](const xgc::adapter::v1::SourceOpenRequest&,
                             const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::SourceOpenDecision::Reject(
        xgc::adapter::v1::ERROR_CLASS_REJECTED, "protocol-fixture",
        "protocol fixture does not open sources");
  };
  return callbacks;
}

void BindProtocolFixture(xgc2::adapter_runtime::ClientConfig* config) {
  ASSERT_NE(config, nullptr);
  std::string error;
  ASSERT_TRUE(config->BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                     ProtocolFixtureCallbacks(), &error))
      << error;
  ASSERT_TRUE(config->BindCapability("test.disabled", kContractVersion,
                                     kDisabledContractDigest, {}, &error))
      << error;
}

}  // namespace

TEST_F(AdapterRuntimeClientTest,
       RunsFencedDualStreamsCapabilityGatingReplayAndSourceCredit) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> enabled_started{0};
  std::atomic<int> enabled_ready{0};
  std::atomic<int> enabled_stopped{0};
  std::atomic<int> disabled_started{0};
  std::atomic<int> unary_calls{0};
  std::atomic<int> operation_calls{0};
  std::atomic<bool> deadline_normalized{false};
  std::atomic<bool> applied_scope_and_secret{false};
  std::atomic<int> cleared{0};
  std::atomic<xgc2::adapter_runtime::SourceWriteResult> precredit_result{
      xgc2::adapter_runtime::SourceWriteResult::kNotReady};
  xgc2::adapter_runtime::Client* client_ptr = nullptr;

  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.start = [&](const xgc::adapter::v1::AdapterInstanceSpec&,
                      const xgc::adapter::v1::EnabledCapability&, std::string*) {
    ++enabled_started;
    return true;
  };
  enabled.ready = [&] { ++enabled_ready; };
  enabled.stop = [&] { ++enabled_stopped; };
  enabled.unary = [&](const xgc::adapter::v1::UnaryRequest& request,
                      const xgc2::adapter_runtime::CancellationToken&) {
    ++unary_calls;
    deadline_normalized.store(request.context().deadline().ttl_ms() == 0 &&
                              request.context().deadline().deadline_unix_nanos() >
                                  NowNanos());
    return xgc2::adapter_runtime::UnaryResult::Success(TestPayload("unary-result"));
  };
  enabled.operation = [&](const xgc::adapter::v1::OperationRequest&,
                          const xgc2::adapter_runtime::CancellationToken& token) {
    ++operation_calls;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!token.IsCancellationRequested() &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return xgc2::adapter_runtime::OperationResult::Cancelled();
  };
  enabled.source_open = [&](const xgc::adapter::v1::SourceOpenRequest& request,
                            const xgc2::adapter_runtime::CancellationToken&) {
    EXPECT_EQ(request.context().work_id(), "source-1");
    EXPECT_EQ(request.context().capability_id(), kCapabilityId);
    EXPECT_EQ(request.context().endpoint_id(), "events");
    EXPECT_EQ(request.initial_credit().messages(), 1U);
    EXPECT_EQ(request.initial_credit().bytes(), 8U);
    precredit_result.store(
        client_ptr->PublishSource(request.context().work_id(), {"before-accept"}));
    return xgc2::adapter_runtime::SourceOpenDecision::Accept();
  };
  std::string bind_error;
  ASSERT_TRUE(config.BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                    std::move(enabled), &bind_error))
      << bind_error;

  xgc2::adapter_runtime::CapabilityCallbacks disabled;
  disabled.start = [&](const xgc::adapter::v1::AdapterInstanceSpec&,
                       const xgc::adapter::v1::EnabledCapability&, std::string*) {
    ++disabled_started;
    return true;
  };
  ASSERT_TRUE(config.BindCapability("test.disabled", kContractVersion,
                                    kDisabledContractDigest, std::move(disabled),
                                    &bind_error))
      << bind_error;

  xgc2::adapter_runtime::ClientCallbacks callbacks;
  callbacks.apply_instance_spec = [&](const xgc::adapter::v1::AdapterInstanceSpec& spec,
                                      std::string*) {
    applied_scope_and_secret.store(
        spec.scope().kind() == "tenant-context" &&
        spec.scope().attributes().at("target") == "fleet-a" &&
        spec.secrets(0).version() == "sha256:secret-version");
    return true;
  };
  callbacks.clear_instance_spec = [&] { ++cleared; };

  xgc2::adapter_runtime::Client client(std::move(config), std::move(callbacks));
  client_ptr = &client;
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  EXPECT_EQ(client.session().state, xgc2::adapter_runtime::ClientState::kReady);
  EXPECT_EQ(client.session().spec_revision, kSpecRevision);
  EXPECT_EQ(enabled_started.load(), 1);
  EXPECT_EQ(enabled_ready.load(), 1);
  EXPECT_EQ(disabled_started.load(), 0);
  EXPECT_TRUE(applied_scope_and_secret.load());
  EXPECT_EQ(precredit_result.load(),
            xgc2::adapter_runtime::SourceWriteResult::kNotReady);

  service_->ReleaseSourceCredit();
  ASSERT_TRUE(service_->WaitForSourceCredit());
  xgc2::adapter_runtime::SourceWriteResult publish =
      xgc2::adapter_runtime::SourceWriteResult::kNoCredit;
  const auto publish_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (publish == xgc2::adapter_runtime::SourceWriteResult::kNoCredit &&
         std::chrono::steady_clock::now() < publish_deadline) {
    publish = client.PublishSource("source-1", {"one", "two"});
    if (publish == xgc2::adapter_runtime::SourceWriteResult::kNoCredit) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  EXPECT_EQ(publish, xgc2::adapter_runtime::SourceWriteResult::kAccepted);
  if (publish != xgc2::adapter_runtime::SourceWriteResult::kAccepted) {
    ADD_FAILURE() << "Runtime Link state=" << static_cast<int>(client.session().state)
                  << " last_error=" << client.session().last_error;
  }
  ASSERT_TRUE(service_->WaitForCompleteExchange());
  EXPECT_TRUE(service_->paired_epoch());
  EXPECT_EQ(service_->registrations(), 1);
  EXPECT_EQ(unary_calls.load(), 1);
  EXPECT_EQ(operation_calls.load(), 1);
  EXPECT_TRUE(deadline_normalized.load());

  ASSERT_TRUE(client.CloseSource("source-1"));
  EXPECT_TRUE(service_->WaitForSourceClosed());
  client.Stop();
  EXPECT_EQ(client.session().state, xgc2::adapter_runtime::ClientState::kStopped);
  EXPECT_EQ(enabled_stopped.load(), 1);
  EXPECT_EQ(cleared.load(), 1);
}

TEST_F(AdapterRuntimeClientTest, RejectsInsecureBootstrapAndUntrustedHandler) {
  ASSERT_EQ(::chmod(bootstrap_path_.c_str(), 0644), 0);
  EXPECT_THROW(xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_),
               std::runtime_error);
  ASSERT_EQ(::chmod(bootstrap_path_.c_str(), 0600), 0);
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  std::string error;
  EXPECT_FALSE(config.BindCapability("not-trusted", 1, "digest", {}, &error));
  EXPECT_FALSE(error.empty());
}

TEST_F(AdapterRuntimeClientTest, RejectsLegacyBootstrapProtocolBeforeRegister) {
  bootstrap_.mutable_registration()->set_runtime_link_protocol_version(
      xgc2::adapter_runtime::kRuntimeLinkProtocolVersion - 1U);
  std::ofstream output(bootstrap_path_,
                       std::ios::out | std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(bootstrap_.SerializeToOstream(&output));
  output.close();
  ASSERT_EQ(::chmod(bootstrap_path_.c_str(), 0600), 0);

  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  BindProtocolFixture(&config);
  xgc2::adapter_runtime::Client client(std::move(config), {});
  std::string error;
  EXPECT_FALSE(client.Start(&error));
  EXPECT_NE(error.find("protocol version 2 exclusively"), std::string::npos) << error;
  EXPECT_EQ(service_->registration_attempts(), 0);
}

class LegacyProtocolSelectionTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kLegacyProtocolSelection;
  }
};

TEST_F(LegacyProtocolSelectionTest, RejectsHostSelectedLegacyProtocol) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  BindProtocolFixture(&config);
  xgc2::adapter_runtime::Client client(std::move(config), {});
  std::string error;
  EXPECT_FALSE(client.Start(&error));
  EXPECT_NE(error.find("protocol version 2"), std::string::npos) << error;
  EXPECT_EQ(service_->registrations(), 1);
}

TEST_F(AdapterRuntimeClientTest,
       RejectsNegotiatedFrameLimitThatCannotFitTerminalMetadata) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  config.maximum_terminal_replay_bytes = 1024U * 1024U;
  BindProtocolFixture(&config);
  xgc2::adapter_runtime::Client client(std::move(config), {});
  std::string error;
  EXPECT_FALSE(client.Start(&error));
  EXPECT_NE(error.find("byte budgets"), std::string::npos) << error;
  EXPECT_EQ(service_->registrations(), 1);
}

TEST_F(AdapterRuntimeClientTest,
       ReadyCallbackFailureIsReportedBeforeTerminalSessionLoss) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 3000;
  auto enabled = ProtocolFixtureCallbacks();
  std::atomic<int> stops{0};
  std::atomic<int> clears{0};
  enabled.ready = [] { throw std::runtime_error("private native failure detail"); };
  enabled.stop = [&] { ++stops; };
  std::string bind_error;
  ASSERT_TRUE(config.BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                    std::move(enabled), &bind_error))
      << bind_error;
  ASSERT_TRUE(config.BindCapability("test.disabled", kContractVersion,
                                    kDisabledContractDigest, {}, &bind_error))
      << bind_error;

  xgc2::adapter_runtime::ClientCallbacks lifecycle;
  lifecycle.apply_instance_spec = [](const xgc::adapter::v1::AdapterInstanceSpec&,
                                     std::string*) { return true; };
  lifecycle.clear_instance_spec = [&] { ++clears; };
  xgc2::adapter_runtime::Client client(std::move(config), std::move(lifecycle));
  std::string error;
  EXPECT_FALSE(client.Start(&error));
  EXPECT_TRUE(service_->WaitForReadyFailureResult());
  EXPECT_NE(error.find("capability ready callback failed"), std::string::npos) << error;
  EXPECT_EQ(error.find("private native failure detail"), std::string::npos) << error;
  EXPECT_EQ(stops.load(), 1);
  EXPECT_EQ(clears.load(), 1);
  EXPECT_EQ(client.session().state, xgc2::adapter_runtime::ClientState::kStopped);
}

TEST_F(AdapterRuntimeClientTest, BootstrapReadIsNoFollowAndSizeBounded) {
  const std::string symlink_path = bootstrap_path_ + ".symlink";
  const std::string empty_path = bootstrap_path_ + ".empty";
  const std::string oversized_path = bootstrap_path_ + ".oversized";
  std::remove(symlink_path.c_str());
  std::remove(empty_path.c_str());
  std::remove(oversized_path.c_str());

  ASSERT_EQ(::symlink(bootstrap_path_.c_str(), symlink_path.c_str()), 0);
  EXPECT_THROW(xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(symlink_path),
               std::runtime_error);

  { std::ofstream empty(empty_path, std::ios::out | std::ios::binary); }
  ASSERT_EQ(::chmod(empty_path.c_str(), 0600), 0);
  EXPECT_THROW(xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(empty_path),
               std::runtime_error);

  {
    std::ofstream oversized(oversized_path,
                            std::ios::out | std::ios::binary | std::ios::trunc);
    oversized.seekp(4 * 1024 * 1024);
    oversized.put('x');
  }
  ASSERT_EQ(::chmod(oversized_path.c_str(), 0600), 0);
  EXPECT_THROW(xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(oversized_path),
               std::runtime_error);

  std::remove(symlink_path.c_str());
  std::remove(empty_path.c_str());
  std::remove(oversized_path.c_str());
}

TEST_F(AdapterRuntimeClientTest, RejectsNonUnixAndNonCanonicalRuntimeTargets) {
  const auto reject_target = [&](const std::string& runtime_target) {
    bootstrap_.set_runtime_target(runtime_target);
    std::ofstream output(bootstrap_path_,
                         std::ios::out | std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(bootstrap_.SerializeToOstream(&output));
    output.close();
    ASSERT_EQ(::chmod(bootstrap_path_.c_str(), 0600), 0);

    try {
      (void)xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
      FAIL() << "untrusted Runtime target was accepted: " << runtime_target;
    } catch (const std::invalid_argument& error) {
      EXPECT_NE(std::string(error.what()).find("unix"), std::string::npos)
          << error.what();
    }
  };

  reject_target("dns:///runtime.example:443");
  reject_target("127.0.0.1:50051");
  reject_target("unix:relative/runtime.sock");
  reject_target("unix:/run/xgc2/../runtime.sock");
  reject_target("unix:/run//xgc2/runtime.sock");
}

class PairReconnectTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kPairReconnects;
  }
};

TEST_F(PairReconnectTest, RegistersOnceAndReplacesThePairAtStrictEpochsTwoAndThree) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 5000;
  config.rpc_timeout_ms = 1000;
  config.reconnect_initial_delay_ms = 50;
  config.reconnect_max_delay_ms = 50;
  config.maximum_pair_reconnect_attempts = 1;

  std::atomic<int> starts{0};
  std::atomic<int> readies{0};
  std::atomic<int> stops{0};
  std::atomic<int> applies{0};
  std::atomic<int> clears{0};
  std::atomic<int> session_losses{0};
  std::atomic<int> source_closes{0};
  std::mutex source_mutex;
  std::condition_variable source_condition;
  bool opening_source_started = false;
  bool opening_source_cancelled = false;
  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.start = [&](const xgc::adapter::v1::AdapterInstanceSpec& spec,
                      const xgc::adapter::v1::EnabledCapability&, std::string*) {
    EXPECT_EQ(spec.revision(), kSpecRevision);
    EXPECT_EQ(spec.spec_digest(), kSpecDigest);
    ++starts;
    return true;
  };
  enabled.ready = [&] { ++readies; };
  enabled.stop = [&] { ++stops; };
  enabled.source_open = [&](const xgc::adapter::v1::SourceOpenRequest&,
                            const xgc2::adapter_runtime::CancellationToken& token) {
    {
      std::lock_guard<std::mutex> lock(source_mutex);
      opening_source_started = true;
    }
    source_condition.notify_all();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!token.IsCancellationRequested() &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
      std::lock_guard<std::mutex> lock(source_mutex);
      opening_source_cancelled = token.IsCancellationRequested();
    }
    source_condition.notify_all();
    return xgc2::adapter_runtime::SourceOpenDecision::Accept();
  };
  enabled.source_closed = [&](const xgc::adapter::v1::SourceOpenRequest& request,
                              const xgc::adapter::v1::AdapterError& close_error) {
    EXPECT_EQ(request.context().work_id(), "reconnect-source");
    EXPECT_EQ(close_error.code(), "connection-pair-replaced");
    ++source_closes;
  };
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [](const xgc::adapter::v1::OperationRequest&,
                         const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::OperationResult::Success();
  };
  std::string bind_error;
  ASSERT_TRUE(config.BindCapability(kCapabilityId, kContractVersion, kContractDigest,
                                    std::move(enabled), &bind_error))
      << bind_error;
  ASSERT_TRUE(config.BindCapability("test.disabled", kContractVersion,
                                    kDisabledContractDigest, {}, &bind_error))
      << bind_error;

  xgc2::adapter_runtime::ClientCallbacks callbacks;
  callbacks.apply_instance_spec = [&](const xgc::adapter::v1::AdapterInstanceSpec& spec,
                                      std::string*) {
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        spec, bootstrap_.initial_spec()));
    ++applies;
    return true;
  };
  callbacks.clear_instance_spec = [&] { ++clears; };
  callbacks.session_lost = [&](const std::string&) { ++session_losses; };

  xgc2::adapter_runtime::Client client(std::move(config), std::move(callbacks));
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  ASSERT_TRUE(service_->WaitForPairAttachments(1));
  {
    std::unique_lock<std::mutex> lock(source_mutex);
    ASSERT_TRUE(source_condition.wait_for(lock, std::chrono::seconds(5),
                                          [&] { return opening_source_started; }));
  }
  RestartRuntimeServer();
  {
    std::unique_lock<std::mutex> lock(source_mutex);
    ASSERT_TRUE(source_condition.wait_for(lock, std::chrono::seconds(5),
                                          [&] { return opening_source_cancelled; }));
  }
  ASSERT_TRUE(service_->WaitForPairAttachments(2));
  RestartRuntimeServer();
  ASSERT_TRUE(service_->WaitForPairAttachments(3));

  const std::vector<std::uint64_t> expected_epochs{1, 2, 3};
  EXPECT_EQ(service_->control_epochs(), expected_epochs);
  EXPECT_EQ(service_->paired_epochs(), expected_epochs);
  EXPECT_EQ(service_->registration_attempts(), 1);
  EXPECT_EQ(service_->registrations(), 1);
  EXPECT_EQ(service_->spec_applications(), 3);
  EXPECT_EQ(applies.load(), 1);
  EXPECT_EQ(starts.load(), 1);
  EXPECT_EQ(readies.load(), 1);
  EXPECT_EQ(stops.load(), 0);
  EXPECT_EQ(clears.load(), 0);
  EXPECT_EQ(source_closes.load(), 1);
  EXPECT_EQ(session_losses.load(), 0);

  const auto snapshot = client.session();
  EXPECT_EQ(snapshot.state, xgc2::adapter_runtime::ClientState::kReady);
  EXPECT_EQ(snapshot.session_id, kSessionId);
  EXPECT_EQ(snapshot.process_generation, kProcessGeneration);
  EXPECT_EQ(snapshot.session_generation, kSessionGeneration);
  EXPECT_EQ(snapshot.runtime_epoch, kRuntimeEpoch);
  EXPECT_EQ(snapshot.spec_revision, kSpecRevision);
  EXPECT_EQ(snapshot.spec_digest, kSpecDigest);

  client.Stop();
  EXPECT_EQ(stops.load(), 1);
  EXPECT_EQ(clears.load(), 1);

  std::string restart_error;
  EXPECT_FALSE(client.Start(&restart_error));
  EXPECT_NE(restart_error.find("single-use"), std::string::npos);
  EXPECT_EQ(service_->registration_attempts(), 1);
}

TEST_F(PairReconnectTest, ExhaustedPairBudgetReportsOneTerminalSessionLoss) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 5000;
  config.rpc_timeout_ms = 1000;
  config.reconnect_initial_delay_ms = 50;
  config.reconnect_max_delay_ms = 50;
  config.maximum_pair_reconnect_attempts = 1;

  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [](const xgc::adapter::v1::OperationRequest&,
                         const xgc2::adapter_runtime::CancellationToken&) {
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

  std::mutex loss_mutex;
  std::condition_variable loss_condition;
  int loss_count = 0;
  std::string loss_reason;
  xgc2::adapter_runtime::ClientCallbacks callbacks;
  callbacks.session_lost = [&](const std::string& reason) {
    {
      std::lock_guard<std::mutex> lock(loss_mutex);
      ++loss_count;
      loss_reason = reason;
    }
    loss_condition.notify_all();
  };

  xgc2::adapter_runtime::Client client(std::move(config), std::move(callbacks));
  std::string start_error;
  ASSERT_TRUE(client.Start(&start_error)) << start_error;
  ASSERT_TRUE(service_->WaitForPairAttachments(1));
  StopRuntimeServer();
  {
    std::unique_lock<std::mutex> lock(loss_mutex);
    ASSERT_TRUE(loss_condition.wait_for(lock, std::chrono::seconds(5),
                                        [&] { return loss_count == 1; }));
    EXPECT_NE(loss_reason.find("reconnect budget exhausted"), std::string::npos);
  }

  EXPECT_EQ(service_->control_epochs(), (std::vector<std::uint64_t>{1}));
  EXPECT_EQ(service_->paired_epochs(), (std::vector<std::uint64_t>{1}));
  EXPECT_EQ(service_->registration_attempts(), 1);
  EXPECT_EQ(service_->registrations(), 1);
  EXPECT_EQ(client.session().last_error.find("reconnect budget exhausted") !=
                std::string::npos,
            true);
  EXPECT_EQ(client.session().state, xgc2::adapter_runtime::ClientState::kSessionLost);
  client.Stop();
}

class SpecReplacementTest : public AdapterRuntimeClientTest {
 protected:
  FakeRuntimeLink::Scenario scenario() const override {
    return FakeRuntimeLink::Scenario::kSpecReplacement;
  }
};

TEST_F(SpecReplacementTest, OnlyANewSpecRestartsNativeApplicationState) {
  auto config = xgc2::adapter_runtime::ClientConfig::FromBootstrapFile(bootstrap_path_);
  config.initial_connect_timeout_ms = 5000;
  config.rpc_timeout_ms = 1000;

  std::atomic<int> starts{0};
  std::atomic<int> readies{0};
  std::atomic<int> stops{0};
  std::atomic<int> applies{0};
  std::atomic<int> clears{0};
  std::atomic<std::uint64_t> latest_revision{0};

  xgc2::adapter_runtime::CapabilityCallbacks enabled;
  enabled.start = [&](const xgc::adapter::v1::AdapterInstanceSpec& spec,
                      const xgc::adapter::v1::EnabledCapability&, std::string*) {
    latest_revision.store(spec.revision());
    ++starts;
    return true;
  };
  enabled.ready = [&] { ++readies; };
  enabled.stop = [&] { ++stops; };
  enabled.unary = [](const xgc::adapter::v1::UnaryRequest&,
                     const xgc2::adapter_runtime::CancellationToken&) {
    return xgc2::adapter_runtime::UnaryResult::Success();
  };
  enabled.operation = [](const xgc::adapter::v1::OperationRequest&,
                         const xgc2::adapter_runtime::CancellationToken&) {
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

  xgc2::adapter_runtime::ClientCallbacks callbacks;
  callbacks.apply_instance_spec = [&](const xgc::adapter::v1::AdapterInstanceSpec& spec,
                                      std::string*) {
    latest_revision.store(spec.revision());
    ++applies;
    return true;
  };
  callbacks.clear_instance_spec = [&] { ++clears; };

  xgc2::adapter_runtime::Client client(std::move(config), std::move(callbacks));
  std::string error;
  ASSERT_TRUE(client.Start(&error)) << error;
  ASSERT_TRUE(service_->WaitForSpecApplications(2));

  EXPECT_EQ(service_->registration_attempts(), 1);
  EXPECT_EQ(service_->registrations(), 1);
  EXPECT_EQ(service_->control_epochs(), (std::vector<std::uint64_t>{1}));
  EXPECT_EQ(applies.load(), 2);
  EXPECT_EQ(starts.load(), 2);
  EXPECT_EQ(readies.load(), 2);
  EXPECT_EQ(stops.load(), 1);
  EXPECT_EQ(clears.load(), 1);
  EXPECT_EQ(latest_revision.load(), kSpecRevision + 1);
  EXPECT_EQ(client.session().spec_revision, kSpecRevision + 1);
  EXPECT_EQ(client.session().spec_digest, kReplacementSpecDigest);

  client.Stop();
  EXPECT_EQ(stops.load(), 2);
  EXPECT_EQ(clears.load(), 2);
}
