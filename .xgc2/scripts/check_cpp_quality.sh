#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="${XGC2_ADAPTER_CLIENT_QUALITY_BUILD_DIR:-${repo_root}/.ci/cpp-quality}"
cd "${repo_root}"

sources=(
  include/xgc2/adapter_link/client.hpp
  include/xgc2/adapter_link/version.hpp
  src/client.cpp
  test/adapter_link_client_test.cpp
)
for tool in cmake ctest clang-format cppcheck; do
  command -v "${tool}" >/dev/null || {
    echo "missing C++ quality tool: ${tool}" >&2
    exit 1
  }
done

clang-format --dry-run --Werror "${sources[@]}"
rm -rf "${build_dir}"
cmake -S "${repo_root}" -B "${build_dir}" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -Wpedantic -Werror" \
  -DXGC2_ADAPTER_LINK_CLIENT_BUILD_TESTING=ON
cmake --build "${build_dir}" -- -j"$(nproc)"
test_listing="$(cd "${build_dir}" && ctest -N)"
printf '%s\n' "${test_listing}"
if ! grep -Eq 'Total Tests: [1-9][0-9]*' <<<"${test_listing}"; then
  echo "CTest discovered no AdapterLink client tests" >&2
  exit 1
fi
(cd "${build_dir}" && ctest --output-on-failure)
cppcheck --enable=warning,performance,portability \
  --error-exitcode=1 --std=c++14 --inline-suppr -I include src

echo "C++ quality checks passed."
