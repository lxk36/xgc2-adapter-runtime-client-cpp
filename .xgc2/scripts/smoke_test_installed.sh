#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
package_name="libxgc2-adapter-runtime-client-dev"
multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"
dpkg -s "${package_name}" >/dev/null
dpkg -s xgc2-protobuf-dev >/dev/null
# shellcheck source=../dependencies/xgc2-protobuf.env
source "${repo_root}/.xgc2/dependencies/xgc2-protobuf.env"
# shellcheck disable=SC1091
. /etc/os-release
distribution="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
expected_protobuf_version="${XGC2_PROTOBUF_VERSION}~${distribution}"
installed_protobuf_version="$(dpkg-query -W -f='${Version}' xgc2-protobuf-dev)"
if [[ "${installed_protobuf_version}" != "${expected_protobuf_version}" ]]; then
  echo "installed protobuf ${installed_protobuf_version} does not equal ${expected_protobuf_version}" >&2
  exit 1
fi

test -f /usr/include/xgc2/adapter_runtime/client.hpp
test -f /usr/include/xgc2/adapter_runtime/version.hpp
test -f /usr/include/xgc/adapter/v1/adapter.pb.h
test -f /usr/include/xgc/adapter/v1/adapter.grpc.pb.h
test -f "/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so"
test -f "/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so"

for library in \
    "/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so" \
    "/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so"; do
  if ldd "${library}" | tee /tmp/xgc2-adapter-runtime-ldd.txt | grep -q 'not found'; then
    exit 1
  fi
done

product_version="$(
  sed -n 's/^version:[[:space:]]*//p' "${repo_root}/.xgc2/product.yml" | head -n 1
)"
expected_library_version="${product_version%-*}"
installed_library_version="$(pkg-config --modversion xgc2-adapter-runtime-client)"
if [[ "${installed_library_version}" != "${expected_library_version}" ]]; then
  echo "installed library version ${installed_library_version} does not match ${expected_library_version}" >&2
  exit 1
fi

probe_dir="${XGC2_ADAPTER_RUNTIME_SMOKE_DIR:-$(mktemp -d -t xgc2-adapter-runtime-smoke-XXXXXX)}"
mkdir -p "${probe_dir}"
cat > "${probe_dir}/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.10)
project(xgc2_adapter_runtime_probe LANGUAGES CXX)

find_package(xgc2_adapter_runtime_client REQUIRED CONFIG)
if(NOT TARGET xgc2::adapter_runtime_client OR
   NOT TARGET xgc2::adapter_runtime_protocol)
  message(FATAL_ERROR "installed Adapter Runtime targets are missing")
endif()
if(NOT EXISTS "${XGC2_PROTOBUF_REGISTRY_JSON}")
  message(FATAL_ERROR "xgc2-protobuf discovery variables were not forwarded")
endif()
add_executable(probe probe.cpp)
target_compile_features(probe PRIVATE cxx_std_14)
target_link_libraries(probe PRIVATE xgc2::adapter_runtime_client)
CMAKE
cat > "${probe_dir}/probe.cpp" <<'CPP'
#include <xgc2/adapter_runtime/client.hpp>
#include <xgc2/adapter_runtime/version.hpp>

int main()
{
  xgc::adapter::v1::AdapterProcessBootstrap bootstrap;
  xgc::adapter::v1::WorkAttach attach;
  return bootstrap.format_version() != 0 || attach.applied_spec_revision() != 0 ||
                 xgc2::adapter_runtime::kClientAbiVersion != 1
             ? 1
             : 0;
}
CPP
cmake -S "${probe_dir}" -B "${probe_dir}/build"
cmake --build "${probe_dir}/build" -- -j2
"${probe_dir}/build/probe"

echo "libxgc2-adapter-runtime-client-dev installed smoke test passed."
