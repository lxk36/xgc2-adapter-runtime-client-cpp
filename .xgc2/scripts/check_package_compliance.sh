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
  .xgc2/product.yml .xgc2/scripts/build_deb.sh
  .xgc2/scripts/configure_xgc2_apt.sh
  .xgc2/scripts/install_protobuf_dependency.sh
  .xgc2/scripts/check_cpp_quality.sh
  .xgc2/scripts/smoke_test_installed.sh
)
for file in "${required[@]}"; do
  test -f "${file}" || { echo "missing required file: ${file}" >&2; exit 1; }
done

if git ls-files | grep -E '(^|/)(build|devel|install|\.ci|Testing)(/|$)' >/dev/null; then
  echo "generated build artifacts are tracked" >&2
  exit 1
fi
if grep -R -E '#include[[:space:]]*[<\"]ros/|find_package\(catkin|catkin_package' \
    CMakeLists.txt cmake include src test 2>/dev/null; then
  echo "ROS or catkin dependency leaked into the common client" >&2
  exit 1
fi
grep -q '^id: libxgc2-adapter-link-client-dev$' .xgc2/product.yml
grep -q '^version: 0.1.0-1$' .xgc2/product.yml
grep -q '^kind: toolchain-apt$' .xgc2/product.yml
grep -q '^#define XGC2_ADAPTER_LINK_CLIENT_ABI_VERSION 0$' \
  include/xgc2/adapter_link/version.hpp

echo "libxgc2-adapter-link-client-dev package compliance checks passed."
