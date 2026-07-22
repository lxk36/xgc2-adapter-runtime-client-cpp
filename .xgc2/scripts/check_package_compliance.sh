#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

bash -n .xgc2/scripts/*.sh

required=(
  LICENSE README.md CMakeLists.txt
  cmake/xgc2_adapter_runtime_clientConfig.cmake.in
  pkgconfig/xgc2-adapter-runtime-client.pc.in
  include/xgc2/adapter_runtime/client.hpp
  include/xgc2/adapter_runtime/version.hpp
  src/bootstrap.cpp src/client.cpp src/client_impl.hpp src/control.cpp
  src/digest.cpp src/dispatch.cpp src/internal.hpp src/queue.cpp src/session.cpp
  src/session_state.cpp src/source_dispatch.cpp src/spec.cpp src/stream.cpp src/work.cpp
  test/adapter_runtime_client_cancellation_test.cpp
  test/adapter_runtime_client_handler_test.cpp
  test/adapter_runtime_client_session_test.cpp
  test/adapter_runtime_client_source_test.cpp
  test/adapter_runtime_client_test_support.hpp docs/design.md
  .github/workflows/ci.yml .github/workflows/release.yml
  .xgc2/dependencies/xgc2-protobuf.env
  .xgc2/product.yml .xgc2/scripts/build_deb.sh
  .xgc2/scripts/configure_xgc2_apt.sh
  .xgc2/scripts/install_protobuf_dependency.sh
  .xgc2/scripts/check_cpp_quality.sh
  .xgc2/scripts/smoke_test_installed.sh
)
for file in "${required[@]}"; do
  test -f "${file}" || { echo "missing required file: ${file}" >&2; exit 1; }
done

(
  unset XGC2_PROTOBUF_VERSION XGC2_PROTOBUF_SOURCE_REF
  # shellcheck source=../dependencies/xgc2-protobuf.env
  source .xgc2/dependencies/xgc2-protobuf.env
  if [[ ! "${XGC2_PROTOBUF_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]]; then
    echo "invalid locked protobuf product version: ${XGC2_PROTOBUF_VERSION}" >&2
    exit 1
  fi
  if [[ ! "${XGC2_PROTOBUF_SOURCE_REF}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "protobuf dependency must be a full source SHA" >&2
    exit 1
  fi
)

if git ls-files | grep -E '(^|/)(build|devel|install|\.ci|Testing)(/|$)' >/dev/null; then
  echo "generated build artifacts are tracked" >&2
  exit 1
fi
if grep -R -E '#include[[:space:]]*[<\"]ros/|find_package\(catkin|catkin_package' \
    CMakeLists.txt cmake include src test 2>/dev/null; then
  echo "ROS or catkin dependency leaked into the common client" >&2
  exit 1
fi
if grep -R -E -i \
    'adapter_link|AdapterPlan|ProfileAdvertisement|robot([-_](group|resource|id))?|telemetry|OpenSourceStream|CloseSourceStream|STREAM_(SINK|DUPLEX)|protocol[[:space:]]+0\.4|bounded[[:space:]]+FIFO' \
    CMakeLists.txt cmake pkgconfig include src test README.md docs 2>/dev/null; then
  echo "legacy or domain-specific API leaked into the generic Runtime SDK" >&2
  exit 1
fi
while IFS= read -r source; do
  lines="$(wc -l < "${source}")"
  if (( lines > 700 )); then
    echo "C++ source exceeds 700-line responsibility gate: ${source} (${lines})" >&2
    exit 1
  fi
done < <(find include src -type f \( -name '*.hpp' -o -name '*.cpp' \) | sort)
while IFS= read -r source; do
  lines="$(wc -l < "${source}")"
  if (( lines > 1600 )); then
    echo "C++ test fixture exceeds 1600-line test-harness gate: ${source} (${lines})" >&2
    exit 1
  fi
done < <(find test -type f \( -name '*.hpp' -o -name '*.cpp' \) | sort)
metadata=.xgc2/product.yml
product_version="$(sed -n 's/^version:[[:space:]]*//p' "${metadata}")"
apt_distributions="$(
  sed -n '/^apt:$/,/^[^[:space:]]/s/^  distribution:[[:space:]]*//p' "${metadata}"
)"

grep -q '^id: libxgc2-adapter-runtime-client-dev$' "${metadata}"
grep -q '^kind: toolchain-apt$' "${metadata}"
grep -q '^    - libxgc2-adapter-runtime-client1$' "${metadata}"
grep -q '^    xgc2-protobuf: rebuild$' "${metadata}"
for workflow in .github/workflows/ci.yml .github/workflows/release.yml; do
  docker_builds="$(grep -c 'docker run --rm' "${workflow}")"
  protobuf_ref_forwards="$(grep -c -- '-e XGC2_PROTOBUF_SOURCE_REF' "${workflow}")"
  if [[ "${protobuf_ref_forwards}" -ne "${docker_builds}" ]]; then
    echo "${workflow} must forward XGC2_PROTOBUF_SOURCE_REF into every build container" >&2
    exit 1
  fi
done
if [[ ! "${product_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]]; then
  echo "product metadata version is missing or invalid: ${product_version:-<empty>}" >&2
  exit 1
fi
if [[ -z "${apt_distributions}" ]]; then
  echo "product metadata apt.distribution is missing" >&2
  exit 1
fi

IFS=',' read -r -a distributions <<< "${apt_distributions}"
for distribution in "${distributions[@]}"; do
  distribution="${distribution//[[:space:]]/}"
  expected_apt_version="${product_version}~${distribution}"
  if ! grep -Fqx "    ${distribution}: ${expected_apt_version}" "${metadata}"; then
    echo "release.apt_versions.${distribution} must be ${expected_apt_version}" >&2
    exit 1
  fi
done
grep -q '^#define XGC2_ADAPTER_RUNTIME_CLIENT_ABI_VERSION 1$' \
  include/xgc2/adapter_runtime/version.hpp
if [[ "$(grep -c '^  SOVERSION 1$' CMakeLists.txt)" -ne 2 ]]; then
  echo "both public shared libraries must preserve ABI SONAME 1" >&2
  exit 1
fi
grep -q '^runtime_package="libxgc2-adapter-runtime-client1"$' \
  .xgc2/scripts/build_deb.sh
grep -Fq "Depends: \${runtime_package} (= \${version})" \
  .xgc2/scripts/build_deb.sh
grep -Fq "Breaks: \${dev_package} (<< \${version})" \
  .xgc2/scripts/build_deb.sh
grep -Fq "Replaces: \${dev_package} (<< \${version})" \
  .xgc2/scripts/build_deb.sh
grep -Fq "libxgc2_adapter_runtime_client 1 \${runtime_package} (>= \${version})" \
  .xgc2/scripts/build_deb.sh

echo "Adapter Runtime split package compliance checks passed."
