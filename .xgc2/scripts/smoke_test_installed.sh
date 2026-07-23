#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dev_package="libxgc2-adapter-runtime-client-dev"
runtime_package="libxgc2-adapter-runtime-client2"
multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"
dpkg -s "${dev_package}" >/dev/null
dpkg -s "${runtime_package}" >/dev/null
dpkg -s xgc2-protobuf-dev >/dev/null
# shellcheck source=../dependencies/xgc2-protobuf.env
source "${repo_root}/.xgc2/dependencies/xgc2-protobuf.env"
# shellcheck disable=SC1091
. /etc/os-release
distribution="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
installed_protobuf_version="$(dpkg-query -W -f='${Version}' xgc2-protobuf-dev)"
protobuf_protocol_pattern="${XGC2_PROTOBUF_PROTOCOL_VERSION//./\\.}"
if [[ ! "${installed_protobuf_version}" =~ ^${protobuf_protocol_pattern}-[0-9]+~${distribution}$ ]]; then
  echo "installed protobuf ${installed_protobuf_version} is outside the ${XGC2_PROTOBUF_PROTOCOL_VERSION} protocol line for ${distribution}" >&2
  exit 1
fi

test -f /usr/include/xgc2/adapter_runtime/client.hpp
test -f /usr/include/xgc2/adapter_runtime/version.hpp
test -f /usr/include/xgc/adapter/v1/adapter.pb.h
test -f /usr/include/xgc/adapter/v1/adapter.grpc.pb.h
test -f "/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so"
test -f "/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so"
test -L "/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so.2"
test -L "/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so.2"

dev_version="$(dpkg-query -W -f='${Version}' "${dev_package}")"
runtime_version="$(dpkg-query -W -f='${Version}' "${runtime_package}")"
if [[ "${runtime_version}" != "${dev_version}" ]]; then
  echo "runtime ${runtime_version} does not match development package ${dev_version}" >&2
  exit 1
fi
dev_depends="$(dpkg-query -W -f='${Depends}' "${dev_package}")"
runtime_depends="$(dpkg-query -W -f='${Depends}' "${runtime_package}")"
grep -Fq "${runtime_package} (= ${dev_version})" <<<"${dev_depends}"
if grep -Eq '(^|, )(xgc2-protobuf-dev|libxgc2-adapter-runtime-client-dev)([ (]|,|$)' \
    <<<"${runtime_depends}"; then
  echo "runtime package leaked a schema or development dependency" >&2
  exit 1
fi

package_files() {
  local package="$1"
  local path
  while IFS= read -r path; do
    if [[ -f "${path}" || -L "${path}" ]]; then
      printf '%s\n' "${path}"
    fi
  done < <(dpkg-query -L "${package}")
}
mapfile -t overlaps < <(
  comm -12 \
    <(package_files "${runtime_package}" | sort) \
    <(package_files "${dev_package}" | sort)
)
if (( ${#overlaps[@]} != 0 )); then
  echo "runtime and development packages overlap files:" >&2
  printf '  %s\n' "${overlaps[@]}" >&2
  exit 1
fi

for library in \
    "/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so.2" \
    "/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so.2"; do
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
                 xgc2::adapter_runtime::kClientAbiVersion != 2
             ? 1
             : 0;
}
CPP
cmake -S "${probe_dir}" -B "${probe_dir}/build"
cmake --build "${probe_dir}/build" -- -j2
"${probe_dir}/build/probe"

mkdir -p "${probe_dir}/debian"
cat > "${probe_dir}/debian/control" <<'EOF'
Source: xgc2-adapter-runtime-probe
Section: misc
Priority: optional
Maintainer: XGC2 <apt@example.com>

Package: xgc2-adapter-runtime-probe
Architecture: any
EOF
probe_shlibs="$(
  cd "${probe_dir}"
  dpkg-shlibdeps -O "-e${probe_dir}/build/probe"
)"
grep -Eq '(^|=|, )libxgc2-adapter-runtime-client2( |[(])' \
  <<<"${probe_shlibs}"
if grep -Eq '(libxgc2-adapter-runtime-client-dev|xgc2-protobuf-dev)' \
    <<<"${probe_shlibs}"; then
  echo "consumer shlibs leaked development-only dependencies" >&2
  exit 1
fi

echo "Adapter Runtime ABI and development package smoke test passed."
