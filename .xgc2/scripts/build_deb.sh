#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
package_name="libxgc2-adapter-runtime-client-dev"
distribution="${PACKAGE_DISTRIBUTION:-}"
build_dir="${XGC2_ADAPTER_RUNTIME_BUILD_DIR:-${repo_root}/.ci/build}"
stage_dir="${XGC2_ADAPTER_RUNTIME_STAGE_DIR:-${repo_root}/.ci/stage}"
output_dir="${XGC2_ADAPTER_RUNTIME_DEB_OUTPUT_DIR:-${repo_root}/debs}"
package_root="${repo_root}/.ci/pkg/${package_name}"
architecture="$(dpkg --print-architecture)"
multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"
# shellcheck source=../dependencies/xgc2-protobuf.env
source "${repo_root}/.xgc2/dependencies/xgc2-protobuf.env"

product_version() {
  sed -n 's/^version:[[:space:]]*//p' "${repo_root}/.xgc2/product.yml" | head -n 1
}

base_version="${PACKAGE_BASE_VERSION:-$(product_version)}"
if [[ -z "${distribution}" && -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  distribution="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
fi
case "${distribution}" in
  focal|jammy|noble) ;;
  *)
    echo "unsupported PACKAGE_DISTRIBUTION: ${distribution:-<empty>}" >&2
    exit 1
    ;;
esac
version="${PACKAGE_VERSION:-${base_version}~${distribution}}"
protobuf_deb_version="${XGC2_PROTOBUF_DEB_VERSION:-$(
  dpkg-query -W -f='${Version}' xgc2-protobuf-dev 2>/dev/null
)}"
if [[ -z "${protobuf_deb_version}" ]]; then
  echo "xgc2-protobuf-dev must be installed before packaging" >&2
  exit 1
fi
expected_protobuf_deb_version="${XGC2_PROTOBUF_VERSION}~${distribution}"
if [[ "${protobuf_deb_version}" != "${expected_protobuf_deb_version}" ]]; then
  echo "xgc2-protobuf-dev must be exactly ${expected_protobuf_deb_version}; installed/selected ${protobuf_deb_version}" >&2
  exit 1
fi
if [[ "${ALLOW_UNSCOPED_BINARY_DEB_VERSION:-0}" != "1" ]]; then
  case "${version}" in
    *"~${distribution}"*|*"+${distribution}"*) ;;
    *)
      echo "binary package version must include ${distribution}: ${version}" >&2
      exit 1
      ;;
  esac
fi

rm -rf "${build_dir}" "${stage_dir}" "${package_root}" "${output_dir}"
mkdir -p "${build_dir}" "${output_dir}"
cmake -S "${repo_root}" -B "${build_dir}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr \
  -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
  -DXGC2_ADAPTER_RUNTIME_CLIENT_BUILD_TESTING=OFF
cmake --build "${build_dir}" -- -j"$(nproc)"
DESTDIR="${stage_dir}" cmake --build "${build_dir}" --target install

mkdir -p \
  "${package_root}/DEBIAN" \
  "${package_root}/usr/share/doc/${package_name}"
cp -a "${stage_dir}/usr" "${package_root}/"
cp -a "${repo_root}/README.md" "${repo_root}/LICENSE" \
  "${package_root}/usr/share/doc/${package_name}/"

cat > "${package_root}/DEBIAN/control" <<EOF
Package: ${package_name}
Version: ${version}
Section: libdevel
Priority: optional
Architecture: ${architecture}
Maintainer: XGC2 <apt@example.com>
Depends: xgc2-protobuf-dev (= ${protobuf_deb_version}), libgrpc++-dev, libprotobuf-dev
Description: Generic XGC2 Adapter Runtime C++ SDK
 Capability-first AdapterRuntimeLink client and generated protocol libraries,
 public headers, CMake exports, and pkg-config metadata. The SDK owns trusted
 bootstrap, registration, paired Control/Work streams, spec application,
 bounded dispatch, source-stream credit, replay, reconnect, and shutdown.
EOF

cat > "${package_root}/DEBIAN/postinst" <<'SH'
#!/bin/sh
set -e
command -v ldconfig >/dev/null 2>&1 && ldconfig
SH
cat > "${package_root}/DEBIAN/postrm" <<'SH'
#!/bin/sh
set -e
command -v ldconfig >/dev/null 2>&1 && ldconfig
SH

test -f "${package_root}/usr/include/xgc2/adapter_runtime/client.hpp"
test -f "${package_root}/usr/include/xgc2/adapter_runtime/version.hpp"
test -f "${package_root}/usr/include/xgc/adapter/v1/adapter.pb.h"
test -f "${package_root}/usr/include/xgc/adapter/v1/adapter.grpc.pb.h"
test -f "${package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so"
test -f "${package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so"
test -f "${package_root}/usr/lib/${multiarch}/cmake/xgc2_adapter_runtime_client/xgc2_adapter_runtime_clientConfig.cmake"
test -f "${package_root}/usr/lib/${multiarch}/pkgconfig/xgc2-adapter-runtime-client.pc"

find "${package_root}" -type d -exec chmod 0755 {} +
find "${package_root}" -type f -exec chmod 0644 {} +
chmod 0755 "${package_root}/DEBIAN" \
  "${package_root}/DEBIAN/postinst" "${package_root}/DEBIAN/postrm"
find "${package_root}/usr/lib/${multiarch}" -maxdepth 1 -type f \
  -name 'libxgc2_adapter_runtime_*.so*' -exec chmod 0755 {} +
find "${package_root}/usr/lib/${multiarch}" -maxdepth 1 -type f \
  -name 'libxgc2_adapter_runtime_*.so*' -exec strip --strip-unneeded {} + \
  2>/dev/null || true

deb_path="${output_dir}/${package_name}_${version}_${architecture}.deb"
fakeroot dpkg-deb --build "${package_root}" "${deb_path}" >/dev/null
dpkg-deb -I "${deb_path}"
echo "Debian artifact written to ${deb_path}"
