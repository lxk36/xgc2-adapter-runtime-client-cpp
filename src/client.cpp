#include "xgc2/adapter_runtime/client.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <utility>
#include <vector>

#include "client_impl.hpp"
#include "internal.hpp"
#include "xgc2/adapter_runtime/version.hpp"

namespace xgc2 {
namespace adapter_runtime {

using internal::ContractKey;
using internal::MakeError;
using internal::SetError;

namespace {

constexpr std::size_t kMaximumBootstrapBytes = 4U * 1024U * 1024U;

class FileDescriptor final {
 public:
  explicit FileDescriptor(int value) : value_(value) {}
  ~FileDescriptor() {
    if (value_ >= 0) {
      ::close(value_);
    }
  }

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

  int get() const { return value_; }

 private:
  int value_ = -1;
};

}  // namespace

CancellationToken::CancellationToken(std::shared_ptr<CancellationState> state)
    : state_(std::move(state)) {}

bool CancellationToken::IsCancellationRequested() const noexcept {
  return state_ && state_->requested.load();
}

UnaryResult UnaryResult::Success(xgc::v1::Payload output) {
  UnaryResult result;
  result.succeeded = true;
  result.output = std::move(output);
  return result;
}

UnaryResult UnaryResult::Failure(xgc::adapter::v1::ErrorClass error_class,
                                 std::string code, std::string message,
                                 std::uint32_t retry_after_ms) {
  UnaryResult result;
  result.error =
      MakeError(error_class, std::move(code), std::move(message), retry_after_ms);
  return result;
}

OperationResult OperationResult::Success(xgc::v1::Payload output, bool has_output) {
  OperationResult result;
  result.has_output = has_output;
  result.output = std::move(output);
  return result;
}

OperationResult OperationResult::Failure(xgc::adapter::v1::ErrorClass error_class,
                                         std::string code, std::string message,
                                         std::int32_t native_code) {
  OperationResult result;
  switch (error_class) {
    case xgc::adapter::v1::ERROR_CLASS_UNCERTAIN:
      result.phase = xgc::adapter::v1::OPERATION_PHASE_UNCERTAIN;
      break;
    case xgc::adapter::v1::ERROR_CLASS_CANCELLED:
      result.phase = xgc::adapter::v1::OPERATION_PHASE_CANCELLED;
      break;
    case xgc::adapter::v1::ERROR_CLASS_REJECTED:
      result.phase = xgc::adapter::v1::OPERATION_PHASE_REJECTED;
      break;
    case xgc::adapter::v1::ERROR_CLASS_DEADLINE:
      result.phase = xgc::adapter::v1::OPERATION_PHASE_EXPIRED;
      break;
    default:
      result.phase = xgc::adapter::v1::OPERATION_PHASE_FAILED;
      break;
  }
  result.error = MakeError(error_class, std::move(code), std::move(message));
  result.native_code = native_code;
  return result;
}

OperationResult OperationResult::Rejected(std::string code, std::string message) {
  OperationResult result;
  result.phase = xgc::adapter::v1::OPERATION_PHASE_REJECTED;
  result.error = MakeError(xgc::adapter::v1::ERROR_CLASS_REJECTED, std::move(code),
                           std::move(message));
  return result;
}

OperationResult OperationResult::Cancelled(std::string message) {
  OperationResult result;
  result.phase = xgc::adapter::v1::OPERATION_PHASE_CANCELLED;
  result.error = MakeError(xgc::adapter::v1::ERROR_CLASS_CANCELLED, "cancelled",
                           std::move(message));
  return result;
}

SourceOpenDecision SourceOpenDecision::Accept() {
  SourceOpenDecision decision;
  decision.accepted = true;
  return decision;
}

SourceOpenDecision SourceOpenDecision::Accept(xgc::v1::Payload initial_payload) {
  SourceOpenDecision decision;
  decision.accepted = true;
  decision.has_initial_payload = true;
  decision.initial_payload = std::move(initial_payload);
  return decision;
}

SourceOpenDecision SourceOpenDecision::Reject(xgc::adapter::v1::ErrorClass error_class,
                                              std::string code, std::string message,
                                              std::uint32_t retry_after_ms) {
  SourceOpenDecision decision;
  decision.error =
      MakeError(error_class, std::move(code), std::move(message), retry_after_ms);
  return decision;
}

ClientConfig ClientConfig::FromBootstrapFile(const std::string& path) {
  if (path.empty() || path.front() != '/') {
    throw std::invalid_argument("adapter bootstrap path must be absolute");
  }
  const FileDescriptor input(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
  if (input.get() < 0) {
    throw std::runtime_error("adapter bootstrap could not be opened securely");
  }
  struct stat status {};
  if (::fstat(input.get(), &status) != 0 || !S_ISREG(status.st_mode)) {
    throw std::runtime_error("adapter bootstrap must be a regular file");
  }
  if (status.st_uid != ::geteuid() || (status.st_mode & 0777) != 0600) {
    throw std::runtime_error(
        "adapter bootstrap must be owned by the process user and mode 0600");
  }
  if (status.st_size <= 0 ||
      static_cast<std::uint64_t>(status.st_size) > kMaximumBootstrapBytes) {
    throw std::runtime_error("adapter bootstrap size must be between 1 byte and 4 MiB");
  }

  std::vector<std::uint8_t> bytes(static_cast<std::size_t>(status.st_size));
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t count =
        ::read(input.get(), bytes.data() + offset, bytes.size() - offset);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      throw std::runtime_error("adapter bootstrap changed or ended during read");
    }
    offset += static_cast<std::size_t>(count);
  }
  std::uint8_t extra = 0;
  ssize_t extra_count = 0;
  do {
    extra_count = ::read(input.get(), &extra, 1);
  } while (extra_count < 0 && errno == EINTR);
  if (extra_count != 0) {
    throw std::runtime_error("adapter bootstrap changed or grew during read");
  }

  xgc::adapter::v1::AdapterProcessBootstrap bootstrap;
  if (!bootstrap.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
    throw std::runtime_error("failed to parse binary AdapterProcessBootstrap");
  }
  if (bootstrap.format_version() != kAdapterBootstrapFormatVersion ||
      bootstrap.runtime_target().empty() || !bootstrap.has_registration() ||
      !bootstrap.has_initial_spec()) {
    throw std::invalid_argument(
        "AdapterProcessBootstrap format, target, registration, or spec is invalid");
  }
  std::string target_error;
  if (!internal::ValidateRuntimeTarget(bootstrap.runtime_target(), &target_error)) {
    throw std::invalid_argument(target_error);
  }
  if (!bootstrap.registration().sdk_version().empty()) {
    throw std::invalid_argument(
        "bootstrap must not pre-assert the Runtime SDK version");
  }

  ClientConfig config;
  config.runtime_target_ = bootstrap.runtime_target();
  config.registration_ = bootstrap.registration();
  config.registration_.set_sdk_version(kClientVersion);
  config.initial_spec_ = bootstrap.initial_spec();
  for (const auto& contract : config.registration_.supported_capabilities()) {
    CapabilityBinding binding;
    binding.contract = contract;
    config.capabilities_.push_back(std::move(binding));
  }
  return config;
}

bool ClientConfig::BindCapability(std::string capability_id,
                                  std::uint32_t contract_version,
                                  std::string contract_digest,
                                  CapabilityCallbacks callbacks, std::string* error) {
  const std::string key = ContractKey(capability_id, contract_version, contract_digest);
  for (auto& binding : capabilities_) {
    if (ContractKey(binding.contract) != key) {
      continue;
    }
    const bool already_bound =
        binding.callbacks.start || binding.callbacks.stop || binding.callbacks.ready ||
        binding.callbacks.unary || binding.callbacks.operation ||
        binding.callbacks.source_open || binding.callbacks.source_closed;
    if (already_bound) {
      SetError(error, "capability handlers are already bound");
      return false;
    }
    binding.callbacks = std::move(callbacks);
    return true;
  }
  SetError(error, "capability handlers are not a trusted bootstrap contract");
  return false;
}

const char* ToString(ClientState state) noexcept {
  switch (state) {
    case ClientState::kStopped:
      return "stopped";
    case ClientState::kConnecting:
      return "connecting";
    case ClientState::kRegistered:
      return "registered";
    case ClientState::kApplyingSpec:
      return "applying-spec";
    case ClientState::kReady:
      return "ready";
    case ClientState::kDraining:
      return "draining";
    case ClientState::kReconnecting:
      return "reconnecting";
    case ClientState::kSessionLost:
      return "session-lost";
    case ClientState::kStopping:
      return "stopping";
  }
  return "unknown";
}

const char* ToString(SourceWriteResult result) noexcept {
  switch (result) {
    case SourceWriteResult::kAccepted:
      return "accepted";
    case SourceWriteResult::kNotReady:
      return "not-ready";
    case SourceWriteResult::kUnknownStream:
      return "unknown-stream";
    case SourceWriteResult::kNoCredit:
      return "no-credit";
    case SourceWriteResult::kQueueFull:
      return "queue-full";
    case SourceWriteResult::kTooLarge:
      return "too-large";
  }
  return "unknown";
}

Client::Client(ClientConfig config, ClientCallbacks callbacks)
    : impl_(new Impl(std::move(config), std::move(callbacks))) {}

Client::~Client() = default;

bool Client::Start(std::string* error) { return impl_->Start(error); }

void Client::Stop() { impl_->Stop(); }

SourceWriteResult Client::PublishSource(const std::string& stream_id,
                                        std::vector<std::string> items) {
  return impl_->PublishSource(stream_id, std::move(items));
}

bool Client::CloseSource(const std::string& stream_id,
                         const xgc::adapter::v1::AdapterError* error) {
  return impl_->CloseSource(stream_id, error);
}

SessionSnapshot Client::session() const { return impl_->session(); }

}  // namespace adapter_runtime
}  // namespace xgc2
