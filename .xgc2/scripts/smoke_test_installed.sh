#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
package_name="libxgc2-adapter-link-client-dev"
multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"
dpkg -s "${package_name}" >/dev/null
dpkg -s xgc2-protobuf-dev >/dev/null

test -f /usr/include/xgc2/adapter_link/client.hpp
test -f /usr/include/xgc2/adapter_link/version.hpp
test -f /usr/include/xgc/adapter/v1/adapter.pb.h
test -f /usr/include/xgc/adapter/v1/adapter.grpc.pb.h
test -f /usr/include/xgc/semantic/aerial/v1/control.pb.h
test -f /usr/include/xgc/semantic/aerial/v1/diagnostic.pb.h
test -f /usr/include/xgc/semantic/aerial/v1/setpoint.pb.h
test -f "/usr/lib/${multiarch}/libxgc2_adapter_link_client.so"
test -f "/usr/lib/${multiarch}/libxgc2_adapter_link_protocol.so"

for library in \
    "/usr/lib/${multiarch}/libxgc2_adapter_link_client.so" \
    "/usr/lib/${multiarch}/libxgc2_adapter_link_protocol.so"; do
  if ldd "${library}" | tee /tmp/xgc2-adapter-link-ldd.txt | grep -q 'not found'; then
    exit 1
  fi
done

product_version="$(
  sed -n 's/^version:[[:space:]]*//p' "${repo_root}/.xgc2/product.yml" | head -n 1
)"
expected_library_version="${product_version%-*}"
installed_library_version="$(pkg-config --modversion xgc2-adapter-link-client)"
if [[ "${installed_library_version}" != "${expected_library_version}" ]]; then
  echo "installed library version ${installed_library_version} does not match ${expected_library_version}" >&2
  exit 1
fi

probe_dir="${XGC2_ADAPTER_CLIENT_SMOKE_DIR:-$(mktemp -d -t xgc2-adapter-client-smoke-XXXXXX)}"
mkdir -p "${probe_dir}"
cat > "${probe_dir}/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.10)
project(xgc2_adapter_client_probe LANGUAGES CXX)

find_package(xgc2_adapter_link_client REQUIRED CONFIG)
if(NOT TARGET xgc2::adapter_link_client OR
   NOT TARGET xgc2::adapter_link_protocol)
  message(FATAL_ERROR "installed AdapterLink targets are missing")
endif()
if(NOT EXISTS "${XGC2_PROTOBUF_REGISTRY_JSON}" OR
   NOT EXISTS "${XGC2_PROTOBUF_PROFILES_DIR}")
  message(FATAL_ERROR "xgc2-protobuf discovery variables were not forwarded")
endif()
add_executable(probe probe.cpp)
target_compile_features(probe PRIVATE cxx_std_14)
target_link_libraries(probe PRIVATE xgc2::adapter_link_client)
CMAKE
cat > "${probe_dir}/probe.cpp" <<'CPP'
#include <xgc2/adapter_link/client.hpp>
#include <xgc2/adapter_link/version.hpp>
#include <xgc/semantic/aerial/v1/control.pb.h>

int main()
{
  xgc2::adapter_link::ClientConfig config;
  xgc::semantic::aerial::v1::ArmRequest request;
  return config.socket_path.empty() || request.ByteSize() != 0 ||
                 xgc2::adapter_link::kClientAbiVersion != 0
             ? 1
             : 0;
}
CPP
cmake -S "${probe_dir}" -B "${probe_dir}/build"
cmake --build "${probe_dir}/build" -- -j2
"${probe_dir}/build/probe"

echo "libxgc2-adapter-link-client-dev installed smoke test passed."
