#pragma once

#include <google/protobuf/message.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#include "xgc/adapter/v1/adapter.pb.h"
#include "xgc/v1/message.pb.h"

namespace xgc2 {
namespace adapter_runtime {
namespace internal {

constexpr std::uint32_t kMinimumHeartbeatIntervalMs = 100;
constexpr std::uint32_t kDefaultHeartbeatIntervalMs = 1000;
constexpr std::size_t kCanonicalSha256TextBytes = 71;

// Returns sha256:<lowercase-hex> over the protobuf type name, one NUL byte,
// and the deterministic wire encoding. The type-name prefix is part of the
// Runtime Link v2 terminal acknowledgement contract.
bool TerminalDigest(const google::protobuf::Message& message, std::string* digest,
                    std::string* error = nullptr);

inline std::chrono::system_clock::time_point DeadlineAfter(std::uint32_t milliseconds) {
  return std::chrono::system_clock::now() + std::chrono::milliseconds(milliseconds);
}

inline std::int64_t UnixNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline xgc::adapter::v1::AdapterError MakeError(
    xgc::adapter::v1::ErrorClass error_class, std::string code, std::string message,
    std::uint32_t retry_after_ms = 0) {
  xgc::adapter::v1::AdapterError error;
  error.set_class_(error_class);
  error.set_code(std::move(code));
  error.set_message(std::move(message));
  error.set_retry_after_ms(retry_after_ms);
  return error;
}

inline bool IsTerminal(xgc::adapter::v1::OperationPhase phase) {
  return phase == xgc::adapter::v1::OPERATION_PHASE_SUCCEEDED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_REJECTED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_FAILED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_EXPIRED ||
         phase == xgc::adapter::v1::OPERATION_PHASE_CANCELLED;
}

inline std::string ContractKey(const std::string& capability_id,
                               std::uint32_t contract_version,
                               const std::string& contract_digest) {
  return capability_id + "\n" + std::to_string(contract_version) + "\n" +
         contract_digest;
}

inline std::string ContractKey(const xgc::adapter::v1::CapabilityContract& contract) {
  return ContractKey(contract.capability_id(), contract.contract_version(),
                     contract.contract_digest());
}

inline std::string ContractKey(const xgc::adapter::v1::EnabledCapability& capability) {
  return ContractKey(capability.capability_id(), capability.contract_version(),
                     capability.contract_digest());
}

inline bool SchemaMatches(const xgc::v1::SchemaReference& expected,
                          const xgc::v1::SchemaReference& actual) {
  return expected.message_id() == actual.message_id() &&
         expected.type_name() == actual.type_name() &&
         expected.schema_version() == actual.schema_version() &&
         expected.schema_fingerprint() == actual.schema_fingerprint();
}

inline bool IsCanonicalText(const std::string& value) {
  const auto is_ascii_whitespace = [](char character) {
    return character == ' ' || character == '\t' || character == '\v' ||
           character == '\f' || character == '\r' || character == '\n';
  };
  if (value.empty() || is_ascii_whitespace(value.front()) ||
      is_ascii_whitespace(value.back())) {
    return false;
  }
  return value.find('\0') == std::string::npos &&
         value.find('\r') == std::string::npos && value.find('\n') == std::string::npos;
}

inline bool IsCanonicalIdentifier(const std::string& value) {
  if (value.empty() || value.front() < 'a' || value.front() > 'z') {
    return false;
  }
  bool separator = false;
  for (const char character : value) {
    const bool alphanumeric = (character >= 'a' && character <= 'z') ||
                              (character >= '0' && character <= '9');
    if (alphanumeric) {
      separator = false;
    } else if ((character == '.' || character == '_' || character == '-') &&
               !separator) {
      separator = true;
    } else {
      return false;
    }
  }
  return !separator;
}

inline bool IsCanonicalSha256(const std::string& value) {
  if (value.size() != 71 || value.compare(0, 7, "sha256:") != 0) {
    return false;
  }
  for (std::size_t index = 7; index < value.size(); ++index) {
    if (!((value[index] >= '0' && value[index] <= '9') ||
          (value[index] >= 'a' && value[index] <= 'f'))) {
      return false;
    }
  }
  return true;
}

inline bool IsValidPayloadEncoding(xgc::v1::PayloadEncoding encoding) {
  return encoding == xgc::v1::PAYLOAD_ENCODING_PROTOBUF ||
         encoding == xgc::v1::PAYLOAD_ENCODING_JSON ||
         encoding == xgc::v1::PAYLOAD_ENCODING_BYTES;
}

inline bool IsValidPayload(const xgc::v1::Payload& payload) {
  if (!payload.has_schema() || payload.schema().message_id() == 0 ||
      !IsCanonicalText(payload.schema().type_name()) ||
      payload.schema().schema_version() == 0 ||
      payload.schema().schema_fingerprint() == 0 ||
      !IsValidPayloadEncoding(payload.encoding())) {
    return false;
  }
  if (payload.encoding() == xgc::v1::PAYLOAD_ENCODING_JSON) {
    google::protobuf::Value json;
    return google::protobuf::util::JsonStringToMessage(payload.value(), &json).ok();
  }
  return true;
}

inline bool IsTypedError(const xgc::adapter::v1::AdapterError& error) {
  const auto error_class = error.class_();
  return error_class >= xgc::adapter::v1::ERROR_CLASS_PERMANENT &&
         error_class <= xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED &&
         IsCanonicalIdentifier(error.code()) && IsCanonicalText(error.message()) &&
         (!error.has_details() || IsValidPayload(error.details()));
}

inline bool ErrorMatchesOperationPhase(xgc::adapter::v1::OperationPhase phase,
                                       xgc::adapter::v1::ErrorClass error_class) {
  switch (phase) {
    case xgc::adapter::v1::OPERATION_PHASE_REJECTED:
      return error_class == xgc::adapter::v1::ERROR_CLASS_REJECTED;
    case xgc::adapter::v1::OPERATION_PHASE_FAILED:
      return error_class == xgc::adapter::v1::ERROR_CLASS_PERMANENT ||
             error_class == xgc::adapter::v1::ERROR_CLASS_TRANSIENT ||
             error_class == xgc::adapter::v1::ERROR_CLASS_RESOURCE_EXHAUSTED;
    case xgc::adapter::v1::OPERATION_PHASE_EXPIRED:
      return error_class == xgc::adapter::v1::ERROR_CLASS_DEADLINE;
    case xgc::adapter::v1::OPERATION_PHASE_CANCELLED:
      return error_class == xgc::adapter::v1::ERROR_CLASS_CANCELLED;
    default:
      return false;
  }
}

inline std::uint64_t SaturatingAdd(std::uint64_t left, std::uint64_t right) {
  if (right > std::numeric_limits<std::uint64_t>::max() - left) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return left + right;
}

inline void NormalizeDeadline(xgc::adapter::v1::Deadline* deadline,
                              std::int64_t received_unix_nanos) {
  if (deadline->ttl_ms() == 0) {
    return;
  }
  const std::int64_t ttl_nanos =
      static_cast<std::int64_t>(deadline->ttl_ms()) * 1000000LL;
  const std::int64_t relative_deadline =
      received_unix_nanos > std::numeric_limits<std::int64_t>::max() - ttl_nanos
          ? std::numeric_limits<std::int64_t>::max()
          : received_unix_nanos + ttl_nanos;
  if (deadline->deadline_unix_nanos() <= 0 ||
      relative_deadline < deadline->deadline_unix_nanos()) {
    deadline->set_deadline_unix_nanos(relative_deadline);
  }
  deadline->set_ttl_ms(0);
}

inline void SetError(std::string* error, const std::string& message) {
  if (error != nullptr) {
    *error = message;
  }
}

inline bool ValidateRuntimeTarget(const std::string& runtime_target,
                                  std::string* error) {
  std::string socket_path;
  if (runtime_target.compare(0, 7, "unix://") == 0) {
    socket_path = runtime_target.substr(7);
  } else if (runtime_target.compare(0, 5, "unix:") == 0) {
    socket_path = runtime_target.substr(5);
  } else {
    SetError(error, "runtime_target must use the unix scheme");
    return false;
  }
  if (socket_path.size() <= 1 || socket_path.front() != '/' ||
      socket_path.back() == '/') {
    SetError(error, "unix target must contain a canonical absolute socket path");
    return false;
  }
  std::size_t component_start = 1;
  while (component_start < socket_path.size()) {
    const std::size_t separator = socket_path.find('/', component_start);
    const std::size_t component_size =
        (separator == std::string::npos ? socket_path.size() : separator) -
        component_start;
    const std::string component = socket_path.substr(component_start, component_size);
    if (component.empty() || component == "." || component == "..") {
      SetError(error, "unix target must contain a canonical absolute socket path");
      return false;
    }
    if (separator == std::string::npos) {
      break;
    }
    component_start = separator + 1;
  }
  return true;
}

}  // namespace internal
}  // namespace adapter_runtime
}  // namespace xgc2
