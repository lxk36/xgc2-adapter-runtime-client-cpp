#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <set>
#include <utility>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::ContractKey;
using internal::MakeError;

namespace {

template <typename Message>
bool SemanticallyEqual(const Message& left, const Message& right) {
  return google::protobuf::util::MessageDifferencer::Equals(left, right);
}

bool IsCanonicalScope(const xgc::adapter::v1::ScopeReference& scope) {
  if (!internal::IsCanonicalIdentifier(scope.kind()) ||
      !internal::IsCanonicalSha256(scope.key()) || scope.attributes().empty()) {
    return false;
  }
  for (const auto& attribute : scope.attributes()) {
    if (!internal::IsCanonicalIdentifier(attribute.first) ||
        !internal::IsCanonicalText(attribute.second)) {
      return false;
    }
  }
  return true;
}

}  // namespace

xgc::adapter::v1::ApplyInstanceSpecResult Client::Impl::ApplyInstanceSpec(
    const std::string& expected_session,
    const xgc::adapter::v1::AdapterInstanceSpec& spec) {
  xgc::adapter::v1::ApplyInstanceSpecResult result;
  result.set_revision(spec.revision());
  result.set_spec_digest(spec.spec_digest());

  std::string apply_error;
  std::vector<std::size_t> binding_indices;
  if (!ValidateInstanceSpec(spec, &binding_indices, &apply_error)) {
    *result.mutable_error() = MakeError(xgc::adapter::v1::ERROR_CLASS_REJECTED,
                                        "invalid-instance-spec", apply_error);
    for (const auto& status : DisabledCapabilityStatuses()) {
      *result.add_capabilities() = status;
    }
    return result;
  }

  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (session_id_ != expected_session || stop_requested_ || session_failed_) {
      *result.mutable_error() =
          MakeError(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "stale-session",
                    "instance spec belongs to a stale or stopping session");
      return result;
    }
    if (spec.revision() == spec_revision_ && spec.spec_digest() == spec_digest_) {
      if (!SemanticallyEqual(spec, active_spec_)) {
        *result.mutable_error() = MakeError(
            xgc::adapter::v1::ERROR_CLASS_REJECTED, "spec-content-mismatch",
            "Host changed instance spec content without changing its identity");
        return result;
      }
      result.set_applied(true);
      for (const auto& status : capability_statuses_) {
        *result.add_capabilities() = status;
      }
      return result;
    }
    if (spec_revision_ != 0 && spec.revision() <= spec_revision_) {
      *result.mutable_error() =
          MakeError(xgc::adapter::v1::ERROR_CLASS_REJECTED, "stale-spec-revision",
                    "instance spec revision did not advance");
      return result;
    }
    if (spec_revision_ == 0 && !SemanticallyEqual(spec, desired_spec_)) {
      *result.mutable_error() =
          MakeError(xgc::adapter::v1::ERROR_CLASS_REJECTED, "initial-spec-mismatch",
                    "Host spec does not match the exact desired spec for this "
                    "registered session");
      return result;
    }
    // Retain a validated replacement independently from transport state. If
    // this application transition is interrupted after the old spec is
    // deactivated, the next Control pair must retry this exact desired spec.
    desired_spec_ = spec;
    state_ = ClientState::kApplyingSpec;
    accepting_work_ = false;
    CancelAllWorkLocked();
  }
  condition_.notify_all();
  WaitForCallbacks();

  std::lock_guard<std::mutex> transition_lock(transition_mutex_);
  if (!CurrentSessionIs(expected_session) || StopRequested()) {
    *result.mutable_error() =
        MakeError(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "stale-session",
                  "session changed while applying instance spec");
    return result;
  }

  DeactivateApplicationUnlocked("instance spec replaced");
  bool top_level_applied = true;
  if (callbacks_.apply_instance_spec) {
    try {
      top_level_applied = callbacks_.apply_instance_spec(spec, &apply_error);
    } catch (const std::exception& exception) {
      top_level_applied = false;
      apply_error = "apply_instance_spec threw: " + std::string(exception.what());
    } catch (...) {
      top_level_applied = false;
      apply_error = "apply_instance_spec threw an unknown exception";
    }
    if (!top_level_applied) {
      if (!internal::IsCanonicalText(apply_error)) {
        apply_error = "application rejected the instance spec";
      }
      SafeClearInstanceSpec();
    }
  }

  std::vector<std::size_t> started;
  std::string failed_contract;
  if (top_level_applied) {
    for (std::size_t index = 0; index < binding_indices.size(); ++index) {
      const std::size_t binding_index = binding_indices[index];
      const auto& binding = config_.capabilities()[binding_index];
      const auto& grant = spec.capabilities(static_cast<int>(index));
      bool started_ok = true;
      if (binding.callbacks.start) {
        try {
          started_ok = binding.callbacks.start(spec, grant, &apply_error);
        } catch (const std::exception& exception) {
          started_ok = false;
          apply_error = "capability start threw: " + std::string(exception.what());
        } catch (...) {
          started_ok = false;
          apply_error = "capability start threw an unknown exception";
        }
      }
      if (!started_ok) {
        failed_contract = ContractKey(grant);
        if (!internal::IsCanonicalText(apply_error)) {
          apply_error = "capability start rejected " + grant.capability_id();
        }
        break;
      }
      started.push_back(binding_index);
    }
  }

  if (!top_level_applied || started.size() != binding_indices.size()) {
    for (auto iterator = started.rbegin(); iterator != started.rend(); ++iterator) {
      SafeStopCapability(*iterator);
    }
    if (top_level_applied) {
      SafeClearInstanceSpec();
    }
    std::vector<xgc::adapter::v1::CapabilityStatus> statuses =
        DisabledCapabilityStatuses();
    for (auto& status : statuses) {
      if (!failed_contract.empty() &&
          ContractKey(status.capability_id(), status.contract_version(),
                      status.contract_digest()) == failed_contract) {
        status.set_state(xgc::adapter::v1::CAPABILITY_STATE_FAILED);
        status.set_detail(apply_error);
      }
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      capability_statuses_ = statuses;
      state_ = ClientState::kRegistered;
    }
    *result.mutable_error() =
        MakeError(xgc::adapter::v1::ERROR_CLASS_PERMANENT, "spec-apply-failed",
                  apply_error.empty() ? "instance spec apply failed" : apply_error);
    for (const auto& status : statuses) {
      *result.add_capabilities() = status;
    }
    condition_.notify_all();
    return result;
  }

  std::map<std::string, ActiveCapability> active;
  std::vector<xgc::adapter::v1::CapabilityStatus> statuses =
      DisabledCapabilityStatuses();
  for (std::size_t index = 0; index < binding_indices.size(); ++index) {
    const auto& grant = spec.capabilities(static_cast<int>(index));
    active.emplace(grant.capability_id(),
                   ActiveCapability{binding_indices[index], grant});
    for (auto& status : statuses) {
      if (ContractKey(status.capability_id(), status.contract_version(),
                      status.contract_digest()) == ContractKey(grant)) {
        status.set_state(xgc::adapter::v1::CAPABILITY_STATE_READY);
        status.set_detail("capability grant is active");
      }
    }
  }

  bool stale_before_commit = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ != expected_session || stop_requested_ || session_failed_) {
      stale_before_commit = true;
    } else {
      active_capabilities_ = std::move(active);
      active_spec_ = spec;
      spec_revision_ = spec.revision();
      spec_digest_ = spec.spec_digest();
      capability_statuses_ = statuses;
      for (auto& status : capability_statuses_) {
        if (status.state() == xgc::adapter::v1::CAPABILITY_STATE_READY) {
          status.set_state(xgc::adapter::v1::CAPABILITY_STATE_CONFIGURING);
          status.set_detail("capability ready callback is pending");
        }
      }
      accepting_work_ = false;
      state_ = ClientState::kApplyingSpec;
      ready_callbacks_complete_ = false;
    }
  }
  if (stale_before_commit) {
    for (auto iterator = started.rbegin(); iterator != started.rend(); ++iterator) {
      SafeStopCapability(*iterator);
    }
    SafeClearInstanceSpec();
    *result.mutable_error() =
        MakeError(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "stale-session",
                  "session changed before instance spec commit");
    return result;
  }

  bool ready_failed = false;
  std::size_t ready_failure_binding = kNoBinding;
  for (const auto binding_index : started) {
    const auto& ready = config_.capabilities()[binding_index].callbacks.ready;
    if (!ready) {
      continue;
    }
    try {
      ready();
    } catch (const std::exception& exception) {
      Log(LogLevel::kError,
          "capability ready callback threw: " + std::string(exception.what()));
      ready_failed = true;
      ready_failure_binding = binding_index;
      break;
    } catch (...) {
      Log(LogLevel::kError, "capability ready callback threw an unknown exception");
      ready_failed = true;
      ready_failure_binding = binding_index;
      break;
    }
  }
  if (ready_failed) {
    std::vector<xgc::adapter::v1::CapabilityStatus> failure_statuses =
        DisabledCapabilityStatuses();
    if (ready_failure_binding != kNoBinding) {
      const auto& failed = config_.capabilities()[ready_failure_binding].contract;
      for (auto& status : failure_statuses) {
        if (ContractKey(status.capability_id(), status.contract_version(),
                        status.contract_digest()) == ContractKey(failed)) {
          status.set_state(xgc::adapter::v1::CAPABILITY_STATE_FAILED);
          status.set_detail("capability ready callback failed");
        }
      }
    }
    DeactivateApplicationUnlocked("capability ready callback failed");
    {
      std::lock_guard<std::mutex> lock(mutex_);
      capability_statuses_ = failure_statuses;
      state_ = ClientState::kRegistered;
      accepting_work_ = false;
      ready_callbacks_complete_ = false;
    }
    *result.mutable_error() =
        MakeError(xgc::adapter::v1::ERROR_CLASS_PERMANENT, "ready-callback-failed",
                  "capability ready callback failed after instance spec commit");
    for (const auto& status : failure_statuses) {
      *result.add_capabilities() = status;
    }
    return result;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_id_ == expected_session && !session_failed_ && !stop_requested_) {
      capability_statuses_ = statuses;
      accepting_work_ = true;
      state_ = ClientState::kReady;
      ready_callbacks_complete_ = true;
    }
  }

  result.set_applied(true);
  for (const auto& status : statuses) {
    *result.add_capabilities() = status;
  }
  condition_.notify_all();
  Log(LogLevel::kInfo,
      "full instance spec applied at revision " + std::to_string(spec.revision()));
  return result;
}

bool Client::Impl::ValidateInstanceSpec(
    const xgc::adapter::v1::AdapterInstanceSpec& spec,
    std::vector<std::size_t>* binding_indices, std::string* error) const {
  if (spec.instance_id() != config_.registration().instance_id() ||
      spec.process_generation() != config_.registration().process_generation()) {
    return Fail(error, "instance spec identity or process generation mismatch");
  }
  if (spec.revision() == 0 || !internal::IsCanonicalSha256(spec.spec_digest())) {
    return Fail(error, "instance spec revision and digest are required");
  }
  if (!spec.has_scope() || !IsCanonicalScope(spec.scope())) {
    return Fail(error, "instance spec scope is missing or non-canonical");
  }
  if (!config_.initial_spec().has_scope() ||
      !SemanticallyEqual(spec.scope(), config_.initial_spec().scope())) {
    return Fail(error, "instance scope is immutable for the Adapter instance");
  }
  if (!spec.has_configuration() || !internal::IsValidPayload(spec.configuration())) {
    return Fail(error, "instance configuration must be one valid typed payload");
  }
  std::set<std::string> secret_names;
  for (const auto& secret : spec.secrets()) {
    if (!internal::IsCanonicalIdentifier(secret.name()) ||
        !internal::IsCanonicalText(secret.reference()) ||
        !internal::IsCanonicalText(secret.version()) ||
        !secret_names.insert(secret.name()).second) {
      return Fail(error, "secret references must be canonical and uniquely named");
    }
  }

  std::set<std::string> capability_ids;
  for (const auto& grant : spec.capabilities()) {
    if (!internal::IsCanonicalIdentifier(grant.capability_id()) ||
        !capability_ids.insert(grant.capability_id()).second) {
      return Fail(error, "instance spec contains an empty or duplicate capability");
    }
    const std::size_t binding_index = FindBindingIndex(grant);
    if (binding_index == kNoBinding) {
      return Fail(error, "instance spec grants an unsupported contract: " +
                             grant.capability_id());
    }
    const auto& binding = config_.capabilities()[binding_index];
    if (grant.has_configuration() && !internal::IsValidPayload(grant.configuration())) {
      return Fail(error, "enabled capability configuration is malformed");
    }
    if (grant.enabled_endpoint_ids().empty()) {
      return Fail(error, "enabled capability grants no endpoints");
    }
    std::set<std::string> endpoints;
    for (const auto& endpoint_id : grant.enabled_endpoint_ids()) {
      const auto* endpoint = FindEndpoint(binding.contract, endpoint_id);
      if (!internal::IsCanonicalIdentifier(endpoint_id) ||
          !endpoints.insert(endpoint_id).second || endpoint == nullptr) {
        return Fail(error, "instance spec grants an unknown or duplicate endpoint");
      }
      if (endpoint->interaction_mode() == xgc::adapter::v1::INTERACTION_MODE_UNARY &&
          !binding.callbacks.unary) {
        return Fail(error, "enabled unary endpoint has no bound handler");
      }
      if (endpoint->interaction_mode() ==
              xgc::adapter::v1::INTERACTION_MODE_OPERATION &&
          !binding.callbacks.operation) {
        return Fail(error, "enabled operation endpoint has no bound handler");
      }
      if (endpoint->interaction_mode() ==
              xgc::adapter::v1::INTERACTION_MODE_STREAM_SOURCE &&
          !binding.callbacks.source_open) {
        return Fail(error, "enabled source endpoint has no bound open handler");
      }
    }
    binding_indices->push_back(binding_index);
  }
  return true;
}

std::size_t Client::Impl::FindBindingIndex(
    const xgc::adapter::v1::EnabledCapability& grant) const {
  const std::string key = ContractKey(grant);
  for (std::size_t index = 0; index < config_.capabilities().size(); ++index) {
    if (ContractKey(config_.capabilities()[index].contract) == key) {
      return index;
    }
  }
  return kNoBinding;
}

const xgc::adapter::v1::CapabilityEndpointContract* Client::Impl::FindEndpoint(
    const xgc::adapter::v1::CapabilityContract& contract,
    const std::string& endpoint_id) const {
  for (const auto& endpoint : contract.endpoints()) {
    if (endpoint.endpoint_id() == endpoint_id) {
      return &endpoint;
    }
  }
  return nullptr;
}

std::vector<xgc::adapter::v1::CapabilityStatus>
Client::Impl::DisabledCapabilityStatuses() const {
  std::vector<xgc::adapter::v1::CapabilityStatus> statuses;
  statuses.reserve(config_.capabilities().size());
  for (const auto& binding : config_.capabilities()) {
    xgc::adapter::v1::CapabilityStatus status;
    status.set_capability_id(binding.contract.capability_id());
    status.set_contract_version(binding.contract.contract_version());
    status.set_contract_digest(binding.contract.contract_digest());
    status.set_state(xgc::adapter::v1::CAPABILITY_STATE_DISABLED);
    status.set_detail("capability contract is not enabled by the current spec");
    statuses.push_back(std::move(status));
  }
  return statuses;
}

void Client::Impl::DeactivateApplication(const std::string& reason) {
  std::lock_guard<std::mutex> transition_lock(transition_mutex_);
  DeactivateApplicationUnlocked(reason);
}

void Client::Impl::DeactivateApplicationUnlocked(const std::string& reason) {
  std::vector<std::size_t> bindings;
  std::vector<SourceState> streams;
  bool had_spec = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_work_ = false;
    CancelAllWorkLocked();
    for (const auto& active : active_capabilities_) {
      bindings.push_back(active.second.binding_index);
    }
    for (const auto& source : source_streams_) {
      streams.push_back(source.second);
    }
    source_streams_.clear();
    closed_sources_.clear();
    source_state_bytes_ = 0;
    had_spec = spec_revision_ != 0;
    active_capabilities_.clear();
    active_spec_.Clear();
    spec_revision_ = 0;
    spec_digest_.clear();
    capability_statuses_ = DisabledCapabilityStatuses();
  }
  WaitForCallbacks();
  for (auto iterator = bindings.rbegin(); iterator != bindings.rend(); ++iterator) {
    SafeStopCapability(*iterator);
  }
  if (had_spec) {
    SafeClearInstanceSpec();
  }
  const auto error = MakeError(
      xgc::adapter::v1::ERROR_CLASS_CANCELLED, "source-stream-closed",
      internal::IsCanonicalText(reason) ? reason : std::string("source stream closed"));
  for (const auto& stream : streams) {
    NotifySourceClosed(stream, error);
  }
}

void Client::Impl::SafeStopCapability(std::size_t binding_index) const {
  const auto& callback = config_.capabilities()[binding_index].callbacks.stop;
  if (!callback) {
    return;
  }
  try {
    callback();
  } catch (const std::exception& exception) {
    Log(LogLevel::kError, "capability stop threw: " + std::string(exception.what()));
  } catch (...) {
    Log(LogLevel::kError, "capability stop threw an unknown exception");
  }
}

void Client::Impl::SafeClearInstanceSpec() const {
  if (!callbacks_.clear_instance_spec) {
    return;
  }
  try {
    callbacks_.clear_instance_spec();
  } catch (const std::exception& exception) {
    Log(LogLevel::kError,
        "clear_instance_spec threw: " + std::string(exception.what()));
  } catch (...) {
    Log(LogLevel::kError, "clear_instance_spec threw an unknown exception");
  }
}

void Client::Impl::SafeSourceClosed(const CapabilityCallbacks& callbacks,
                                    const xgc::adapter::v1::SourceOpenRequest& request,
                                    const xgc::adapter::v1::AdapterError& error) const {
  try {
    callbacks.source_closed(request, error);
  } catch (const std::exception& exception) {
    Log(LogLevel::kError, "source_closed threw: " + std::string(exception.what()));
  } catch (...) {
    Log(LogLevel::kError, "source_closed threw an unknown exception");
  }
}

void Client::Impl::WaitForCallbacks() {
  std::unique_lock<std::mutex> lock(mutex_);
  callbacks_condition_.wait(
      lock, [this] { return in_flight_work_ == 0 && active_dispatch_jobs_ == 0; });
}

void Client::Impl::CancelAllWorkLocked() {
  for (const auto& token : work_tokens_) {
    token.second.cancellation->requested.store(true);
  }
  for (const auto& source : source_streams_) {
    source.second.cancellation->requested.store(true);
  }
  condition_.notify_all();
}

}  // namespace adapter_runtime
}  // namespace xgc2
