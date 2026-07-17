#pragma once

#include <cstdint>

#define XGC2_ADAPTER_RUNTIME_CLIENT_VERSION_MAJOR 0
#define XGC2_ADAPTER_RUNTIME_CLIENT_VERSION_MINOR 5
#define XGC2_ADAPTER_RUNTIME_CLIENT_VERSION_PATCH 0
#define XGC2_ADAPTER_RUNTIME_CLIENT_ABI_VERSION 1

namespace xgc2 {
namespace adapter_runtime {

constexpr const char* kClientVersion = "0.5.0";
constexpr int kClientAbiVersion = XGC2_ADAPTER_RUNTIME_CLIENT_ABI_VERSION;
constexpr std::uint32_t kAdapterBootstrapFormatVersion = 2;
constexpr std::uint32_t kRuntimeLinkProtocolVersion = 2;

}  // namespace adapter_runtime
}  // namespace xgc2
