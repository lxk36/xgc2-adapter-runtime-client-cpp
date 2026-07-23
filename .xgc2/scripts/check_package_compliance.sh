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
  unset XGC2_PROTOBUF_PROTOCOL_VERSION XGC2_PROTOBUF_STANDALONE_SOURCE_REF
  # shellcheck source=../dependencies/xgc2-protobuf.env
  source .xgc2/dependencies/xgc2-protobuf.env
  if [[ ! "${XGC2_PROTOBUF_PROTOCOL_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "invalid protobuf protocol version: ${XGC2_PROTOBUF_PROTOCOL_VERSION}" >&2
    exit 1
  fi
  if [[ ! "${XGC2_PROTOBUF_STANDALONE_SOURCE_REF}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "standalone protobuf dependency must be a full source SHA" >&2
    exit 1
  fi
  if [[ "${XGC2_PROTOBUF_PROTOCOL_VERSION}" != "0.5.0" ||
        "${XGC2_PROTOBUF_STANDALONE_SOURCE_REF}" != "6fd0781937613368bc4a3e4cb1a6fd6d03ead826" ]]; then
    echo "protobuf standalone source is not the supported RuntimeLink protocol contract" >&2
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
if grep -R -E \
    'volatile_(supported|work)|volatile (Work|endpoint)|context\(\)\.volatile_|\.volatile_\(' \
    cmake pkgconfig include src test README.md docs 2>/dev/null; then
  echo "retired non-durable Work semantics leaked into the Runtime SDK" >&2
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
metadata_values="$(python3 - "${metadata}" <<'PY'
import sys

try:
    import yaml
except ImportError as exc:
    raise SystemExit(f"PyYAML is required for product metadata validation: {exc}")

with open(sys.argv[1], encoding="utf-8") as stream:
    product = yaml.safe_load(stream)

if not isinstance(product, dict):
    raise SystemExit("product metadata must be a mapping")
if product.get("schema") != "xgc2.product.v1":
    raise SystemExit("product metadata schema mismatch")
if product.get("id") != "libxgc2-adapter-runtime-client-dev":
    raise SystemExit("product metadata identity mismatch")
if product.get("kind") != "toolchain-apt":
    raise SystemExit("product metadata kind mismatch")

version = product.get("version")
if not isinstance(version, str) or not version:
    raise SystemExit("product metadata version is missing")

apt = product.get("apt")
if not isinstance(apt, dict):
    raise SystemExit("product metadata apt section is missing")
distribution_text = apt.get("distribution")
if not isinstance(distribution_text, str) or not distribution_text:
    raise SystemExit("product metadata apt.distribution is missing")
distributions = [item.strip() for item in distribution_text.split(",") if item.strip()]
if not distributions:
    raise SystemExit("product metadata apt.distribution is empty")
packages = apt.get("packages")
if not isinstance(packages, list) or "libxgc2-adapter-runtime-client2" not in packages:
    raise SystemExit("apt.packages must contain libxgc2-adapter-runtime-client2")

release = product.get("release")
if not isinstance(release, dict):
    raise SystemExit("product metadata release section is missing")
policy = release.get("dependency_policy")
if not isinstance(policy, dict) or policy.get("xgc2-protobuf") != "rebuild":
    raise SystemExit("release.dependency_policy must rebuild xgc2-protobuf")
apt_versions = release.get("apt_versions")
if not isinstance(apt_versions, dict):
    raise SystemExit("release.apt_versions is missing")
for distribution in distributions:
    expected = f"{version}~{distribution}"
    if apt_versions.get(distribution) != expected:
        raise SystemExit(
            f"release.apt_versions.{distribution} must be {expected}"
        )

print(version)
PY
)"
product_version="${metadata_values}"
for workflow in .github/workflows/ci.yml .github/workflows/release.yml; do
  docker_builds="$(grep -c 'docker run --rm' "${workflow}")"
  protobuf_resolver_invocations="$(grep -c './.xgc2/scripts/install_protobuf_dependency.sh' "${workflow}")"
  if [[ "${protobuf_resolver_invocations}" -ne "${docker_builds}" ]]; then
    echo "${workflow} must resolve protobuf in every build container" >&2
    exit 1
  fi
  if grep -Eq -- 'vars\.XGC2_PROTOBUF|-[[:space:]]*e[[:space:]]+XGC2_PROTOBUF' "${workflow}"; then
    echo "${workflow} must not override protobuf dependency selection" >&2
    exit 1
  fi
done
grep -Fq 'XGC2_PROTOBUF_STANDALONE_SOURCE_REF' .github/workflows/release.yml
if grep -Fq 'XGC2_PROTOBUF_SOURCE_REF' .github/workflows/release.yml; then
  echo "release workflow retains the retired protobuf source-ref variable" >&2
  exit 1
fi
grep -Fq 'apt-cache policy xgc2-protobuf-dev' .xgc2/scripts/install_protobuf_dependency.sh
grep -Fq 'XGC2_APT_OVERLAY_URL' .xgc2/scripts/install_protobuf_dependency.sh
grep -Fq 'XGC2_PROTOBUF_PROTOCOL_VERSION' .xgc2/scripts/build_deb.sh
grep -Fq 'XGC2_PROTOBUF_PROTOCOL_VERSION' .xgc2/scripts/smoke_test_installed.sh
if grep -Fq 'XGC2_PROTOBUF_DEB_VERSION' .xgc2/scripts/build_deb.sh; then
  echo "package build must derive the protobuf Debian revision from the installed dependency" >&2
  exit 1
fi
if [[ ! "${product_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]]; then
  echo "product metadata version is missing or invalid: ${product_version:-<empty>}" >&2
  exit 1
fi
semantic_version="${product_version%-*}"
IFS='.' read -r version_major version_minor version_patch <<< "${semantic_version}"

grep -Fqx "project(xgc2_adapter_runtime_client VERSION ${semantic_version} LANGUAGES CXX)" \
  CMakeLists.txt
grep -Fqx "#define XGC2_ADAPTER_RUNTIME_CLIENT_VERSION_MAJOR ${version_major}" \
  include/xgc2/adapter_runtime/version.hpp
grep -Fqx "#define XGC2_ADAPTER_RUNTIME_CLIENT_VERSION_MINOR ${version_minor}" \
  include/xgc2/adapter_runtime/version.hpp
grep -Fqx "#define XGC2_ADAPTER_RUNTIME_CLIENT_VERSION_PATCH ${version_patch}" \
  include/xgc2/adapter_runtime/version.hpp
grep -q '^#define XGC2_ADAPTER_RUNTIME_CLIENT_ABI_VERSION 2$' \
  include/xgc2/adapter_runtime/version.hpp
grep -Fqx "constexpr const char* kClientVersion = \"${semantic_version}\";" \
  include/xgc2/adapter_runtime/version.hpp
if [[ "$(grep -c '^  SOVERSION 2$' CMakeLists.txt)" -ne 2 ]]; then
  echo "both public shared libraries must expose ABI SONAME 2" >&2
  exit 1
fi
grep -q '^  COMPATIBILITY ExactVersion$' CMakeLists.txt
grep -q '^runtime_package="libxgc2-adapter-runtime-client2"$' \
  .xgc2/scripts/build_deb.sh
grep -Fq "Depends: \${runtime_package} (= \${version})" \
  .xgc2/scripts/build_deb.sh
grep -Fq "Breaks: \${dev_package} (<< \${version})" \
  .xgc2/scripts/build_deb.sh
grep -Fq "Replaces: \${dev_package} (<< \${version})" \
  .xgc2/scripts/build_deb.sh
grep -Fq "libxgc2_adapter_runtime_client 2 \${runtime_package} (>= \${version})" \
  .xgc2/scripts/build_deb.sh

echo "Adapter Runtime split package compliance checks passed."
