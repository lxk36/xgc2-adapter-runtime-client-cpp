#include <set>

#include "client_impl.hpp"
#include "xgc2/adapter_runtime/version.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::ContractKey;

namespace {

bool IsLowerAlpha(char value) { return value >= 'a' && value <= 'z'; }

bool IsCanonicalText(const std::string& value) {
  return internal::IsCanonicalText(value);
}

bool IsCanonicalIdentifier(const std::string& value) {
  return internal::IsCanonicalIdentifier(value);
}

bool IsCanonicalCapabilityId(const std::string& value) {
  if (value.find('.') == std::string::npos || !IsCanonicalIdentifier(value)) {
    return false;
  }
  for (std::size_t index = 0; index < value.size(); ++index) {
    if ((index == 0 || value[index - 1] == '.') && !IsLowerAlpha(value[index])) {
      return false;
    }
  }
  return true;
}

bool IsCanonicalDigest(const std::string& value) {
  return internal::IsCanonicalSha256(value);
}

bool IsCanonicalInstanceId(const std::string& value) {
  if (value.size() != 36 || value.compare(0, 4, "ari-") != 0) {
    return false;
  }
  for (std::size_t index = 4; index < value.size(); ++index) {
    if (!((value[index] >= '0' && value[index] <= '9') ||
          (value[index] >= 'a' && value[index] <= 'f'))) {
      return false;
    }
  }
  return true;
}

bool ValidSchema(const xgc::v1::SchemaReference& schema) {
  return schema.message_id() > 0 && schema.schema_version() > 0 &&
         schema.schema_fingerprint() > 0 && IsCanonicalText(schema.type_name());
}

bool ShapeFail(std::string* error, const std::string& message) {
  internal::SetError(error, message);
  return false;
}

bool ValidateEndpointShape(const xgc::adapter::v1::CapabilityEndpointContract& endpoint,
                           std::string* error) {
  const auto mode = endpoint.interaction_mode();
  const bool unary = mode == xgc::adapter::v1::INTERACTION_MODE_UNARY;
  const bool operation = mode == xgc::adapter::v1::INTERACTION_MODE_OPERATION;
  const bool source = mode == xgc::adapter::v1::INTERACTION_MODE_STREAM_SOURCE;
  if (!IsCanonicalIdentifier(endpoint.endpoint_id()) ||
      (!unary && !operation && !source)) {
    return ShapeFail(error, "capability endpoint identity or mode is invalid");
  }
  if ((endpoint.has_input_schema() && !ValidSchema(endpoint.input_schema())) ||
      (endpoint.has_output_schema() && !ValidSchema(endpoint.output_schema())) ||
      (endpoint.has_event_schema() && !ValidSchema(endpoint.event_schema()))) {
    return ShapeFail(error, "capability endpoint schema identity is invalid");
  }
  if ((unary || operation) && !endpoint.has_input_schema()) {
    return ShapeFail(error, "endpoint interaction requires input schema");
  }
  if ((unary || operation || source) && !endpoint.has_output_schema()) {
    return ShapeFail(error, "endpoint interaction requires output schema");
  }
  if (endpoint.side_effect_class() < xgc::adapter::v1::SIDE_EFFECT_CLASS_READ_ONLY ||
      endpoint.side_effect_class() >
          xgc::adapter::v1::SIDE_EFFECT_CLASS_NON_IDEMPOTENT ||
      endpoint.idempotency_mode() < xgc::adapter::v1::IDEMPOTENCY_MODE_NOT_SUPPORTED ||
      endpoint.idempotency_mode() > xgc::adapter::v1::IDEMPOTENCY_MODE_REQUIRED) {
    return ShapeFail(error, "endpoint side-effect or idempotency mode is invalid");
  }
  if (endpoint.default_timeout_ms() == 0 || endpoint.maximum_timeout_ms() == 0 ||
      endpoint.default_timeout_ms() > endpoint.maximum_timeout_ms()) {
    return ShapeFail(error, "endpoint timeouts must be positive and ordered");
  }
  const auto& limits = endpoint.limits();
  if (limits.maximum_request_bytes() == 0 || limits.maximum_response_bytes() == 0 ||
      limits.maximum_concurrency() == 0) {
    return ShapeFail(error,
                     "endpoint request/response/concurrency limits must be positive");
  }
  const bool any_stream_limit = limits.maximum_streams() != 0 ||
                                limits.maximum_stream_chunk_bytes() != 0 ||
                                limits.maximum_stream_chunk_messages() != 0;
  if (source &&
      (limits.maximum_streams() == 0 || limits.maximum_stream_chunk_bytes() == 0 ||
       limits.maximum_stream_chunk_messages() == 0)) {
    return ShapeFail(error, "source endpoint stream limits must be positive");
  }
  if (!source && any_stream_limit) {
    return ShapeFail(error, "non-stream endpoint declares stream limits");
  }
  return true;
}

}  // namespace

bool Client::Impl::ValidateConfig(std::string* error) const {
  if (!internal::ValidateRuntimeTarget(config_.runtime_target(), error)) {
    return false;
  }

  const auto& registration = config_.registration();
  if (!IsCanonicalInstanceId(registration.instance_id()) ||
      registration.process_generation() == 0) {
    return Fail(error, "instance_id must be canonical and process_generation positive");
  }
  if (!IsCanonicalIdentifier(registration.definition_id()) ||
      !IsCanonicalDigest(registration.definition_digest()) ||
      !IsCanonicalText(registration.software_version()) ||
      !IsCanonicalDigest(registration.build_digest()) ||
      !IsCanonicalDigest(registration.manifest_digest()) ||
      !IsCanonicalText(registration.bootstrap_token()) ||
      registration.sdk_version() != kClientVersion) {
    return Fail(
        error,
        "trusted definition/build/manifest/software/token/SDK identity is required");
  }
  if (registration.runtime_link_protocol_version() != kRuntimeLinkProtocolVersion) {
    return Fail(error, "Runtime SDK 0.5 requires protocol version 2 exclusively");
  }
  if (config_.initial_spec().instance_id() != registration.instance_id() ||
      config_.initial_spec().process_generation() !=
          registration.process_generation() ||
      config_.initial_spec().revision() == 0 ||
      !IsCanonicalDigest(config_.initial_spec().spec_digest())) {
    return Fail(error, "initial_spec must match registration identity and generation");
  }
  if (config_.rpc_timeout_ms == 0 || config_.initial_connect_timeout_ms == 0 ||
      config_.reconnect_initial_delay_ms == 0 ||
      config_.reconnect_max_delay_ms < config_.reconnect_initial_delay_ms ||
      config_.maximum_pair_reconnect_attempts == 0 ||
      config_.maximum_control_queue == 0 || config_.maximum_work_queue == 0 ||
      config_.maximum_dispatch_queue == 0 || config_.maximum_terminal_replay == 0 ||
      config_.maximum_source_streams == 0 ||
      config_.maximum_source_tombstones < config_.maximum_source_streams ||
      config_.maximum_control_queue_bytes == 0 ||
      config_.maximum_work_queue_bytes == 0 ||
      config_.maximum_dispatch_queue_bytes == 0 ||
      config_.maximum_terminal_replay_bytes == 0 ||
      config_.maximum_source_state_bytes == 0 || config_.dispatch_workers == 0) {
    return Fail(error, "timeouts, queue bounds, and worker counts must be positive");
  }

  std::set<std::string> contracts;
  for (const auto& binding : config_.capabilities()) {
    const auto& contract = binding.contract;
    if (!IsCanonicalCapabilityId(contract.capability_id()) ||
        contract.contract_version() == 0 ||
        !IsCanonicalDigest(contract.contract_digest()) ||
        contract.endpoints_size() == 0 ||
        !contracts.insert(ContractKey(contract)).second) {
      return Fail(error,
                  "capability contracts must have unique identity and endpoints");
    }
    std::set<std::string> endpoints;
    for (const auto& endpoint : contract.endpoints()) {
      if (!ValidateEndpointShape(endpoint, error) ||
          !endpoints.insert(endpoint.endpoint_id()).second) {
        if (error != nullptr && error->empty()) {
          *error = "capability contract contains an invalid or duplicate endpoint";
        }
        return false;
      }
    }
    bool trusted = false;
    for (const auto& advertised : registration.supported_capabilities()) {
      if (ContractKey(advertised) == ContractKey(contract)) {
        trusted = true;
        break;
      }
    }
    if (!trusted) {
      return Fail(error,
                  "application capability is not present in bootstrap registration");
    }
  }
  if (config_.capabilities().size() !=
      static_cast<std::size_t>(registration.supported_capabilities_size())) {
    return Fail(error, "bootstrap capability contracts were dropped or added");
  }

  std::vector<std::size_t> initial_bindings;
  return ValidateInstanceSpec(config_.initial_spec(), &initial_bindings, error);
}

}  // namespace adapter_runtime
}  // namespace xgc2
