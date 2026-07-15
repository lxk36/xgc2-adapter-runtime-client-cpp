#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

bash -n .xgc2/scripts/*.sh

required=(
  LICENSE README.md CMakeLists.txt
  cmake/xgc2_adapter_link_clientConfig.cmake.in
  pkgconfig/xgc2-adapter-link-client.pc.in
  include/xgc2/adapter_link/client.hpp
  include/xgc2/adapter_link/version.hpp
  src/client.cpp test/adapter_link_client_test.cpp docs/design.md
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
    echo "protobuf dependency must be locked to a full source SHA" >&2
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
metadata=.xgc2/product.yml
product_version="$(sed -n 's/^version:[[:space:]]*//p' "${metadata}")"
apt_distributions="$(
  sed -n '/^apt:$/,/^[^[:space:]]/s/^  distribution:[[:space:]]*//p' "${metadata}"
)"

grep -q '^id: libxgc2-adapter-link-client-dev$' "${metadata}"
grep -q '^kind: toolchain-apt$' "${metadata}"
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
grep -q '^#define XGC2_ADAPTER_LINK_CLIENT_ABI_VERSION 0$' \
  include/xgc2/adapter_link/version.hpp

echo "libxgc2-adapter-link-client-dev package compliance checks passed."
