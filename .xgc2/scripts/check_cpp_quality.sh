#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="${XGC2_ADAPTER_RUNTIME_QUALITY_BUILD_DIR:-${repo_root}/.ci/cpp-quality}"
cd "${repo_root}"

sources=(
  include/xgc2/adapter_runtime/client.hpp
  include/xgc2/adapter_runtime/version.hpp
  src/bootstrap.cpp
  src/client.cpp
  src/client_impl.hpp
  src/control.cpp
  src/digest.cpp
  src/dispatch.cpp
  src/internal.hpp
  src/queue.cpp
  src/session.cpp
  src/session_state.cpp
  src/source_dispatch.cpp
  src/spec.cpp
  src/stream.cpp
  src/work.cpp
  test/adapter_runtime_client_cancellation_test.cpp
  test/adapter_runtime_client_handler_test.cpp
  test/adapter_runtime_client_session_test.cpp
  test/adapter_runtime_client_source_test.cpp
  test/adapter_runtime_client_test_support.hpp
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
  -DXGC2_ADAPTER_RUNTIME_CLIENT_BUILD_TESTING=ON
cmake --build "${build_dir}" -- -j"$(nproc)"
test_listing="$(cd "${build_dir}" && ctest -N)"
printf '%s\n' "${test_listing}"
if ! grep -Eq 'Total Tests: [1-9][0-9]*' <<<"${test_listing}"; then
  echo "CTest discovered no Adapter Runtime client tests" >&2
  exit 1
fi
(cd "${build_dir}" && ctest --output-on-failure)
cppcheck --enable=warning,performance,portability \
  --error-exitcode=1 --std=c++14 --inline-suppr -I include -I src src

echo "C++ quality checks passed."
