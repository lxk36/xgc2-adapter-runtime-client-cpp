#include <algorithm>
#include <chrono>

#include "client_impl.hpp"

namespace xgc2 {
namespace adapter_runtime {

void Client::Impl::EnterSessionLost(const std::string& reason) {
  // A transport pair failure preserves native state, but a terminal session
  // loss is the application fence: stop capabilities and clear the spec before
  // reporting the loss to the owner.
  DeactivateApplication(reason);
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_requested_) {
      return;
    }
    terminal_session_failure_ = true;
    terminal_session_error_ = reason;
    session_failed_ = true;
    accepting_work_ = false;
    state_ = ClientState::kSessionLost;
    last_error_ = reason;
    if (!session_lost_reported_) {
      session_lost_reported_ = true;
      notify = true;
    }
  }
  condition_.notify_all();
  Log(LogLevel::kError, "Runtime Link session lost: " + reason);
  if (notify && callbacks_.session_lost) {
    try {
      callbacks_.session_lost(reason);
    } catch (const std::exception& exception) {
      Log(LogLevel::kError,
          "session_lost callback threw: " + std::string(exception.what()));
    } catch (...) {
      Log(LogLevel::kError, "session_lost callback threw an unknown exception");
    }
  }
}

bool Client::Impl::TerminalSessionFailureRequested() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return terminal_session_failure_;
}

bool Client::Impl::StopRequested() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stop_requested_;
}

bool Client::Impl::CurrentSessionIs(const std::string& session_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return session_id_ == session_id && !session_failed_;
}

void Client::Impl::SetState(ClientState state) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!stop_requested_) {
    state_ = state;
  }
  condition_.notify_all();
}

void Client::Impl::RecordError(const std::string& error) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    last_error_ = error;
  }
  condition_.notify_all();
  Log(LogLevel::kWarning, error);
}

void Client::Impl::WaitFor(std::uint32_t milliseconds) {
  std::unique_lock<std::mutex> lock(mutex_);
  condition_.wait_for(lock, std::chrono::milliseconds(milliseconds),
                      [this] { return stop_requested_; });
}

std::uint32_t Client::Impl::NextBackoff(std::uint32_t current) const {
  if (current >= config_.reconnect_max_delay_ms) {
    return config_.reconnect_max_delay_ms;
  }
  return std::min(config_.reconnect_max_delay_ms,
                  static_cast<std::uint32_t>(current * 2));
}

void Client::Impl::Log(LogLevel level, const std::string& message) const noexcept {
  if (!callbacks_.log) {
    return;
  }
  try {
    callbacks_.log(level, message);
  } catch (...) {
  }
}

}  // namespace adapter_runtime
}  // namespace xgc2
