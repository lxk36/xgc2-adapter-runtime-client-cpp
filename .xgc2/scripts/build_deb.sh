#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
package_name="libxgc2-adapter-link-client-dev"
distribution="${PACKAGE_DISTRIBUTION:-}"
build_dir="${XGC2_ADAPTER_CLIENT_BUILD_DIR:-${repo_root}/.ci/build}"
stage_dir="${XGC2_ADAPTER_CLIENT_STAGE_DIR:-${repo_root}/.ci/stage}"
output_dir="${XGC2_ADAPTER_CLIENT_DEB_OUTPUT_DIR:-${repo_root}/debs}"
package_root="${repo_root}/.ci/pkg/${package_name}"
architecture="$(dpkg --print-architecture)"
multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"

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
  -DXGC2_ADAPTER_LINK_CLIENT_BUILD_TESTING=OFF
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
Depends: xgc2-protobuf-dev (>= 0.2.0-1~${distribution}), libgrpc++-dev, libprotobuf-dev
Description: ROS-independent XGC2 AdapterLink C++ client
 Shared AdapterLink client and generated protocol libraries, public headers,
 CMake exports, and pkg-config metadata for native robot adapters. The client
 owns registration, plan activation, heartbeat, telemetry batching, operation
 streaming, reconnect, and deterministic shutdown without depending on ROS.
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

test -f "${package_root}/usr/include/xgc2/adapter_link/client.hpp"
test -f "${package_root}/usr/include/xgc2/adapter_link/version.hpp"
test -f "${package_root}/usr/include/xgc/adapter/v1/adapter.pb.h"
test -f "${package_root}/usr/include/xgc/adapter/v1/adapter.grpc.pb.h"
test -f "${package_root}/usr/include/xgc/semantic/aerial/v1/control.pb.h"
test -f "${package_root}/usr/lib/${multiarch}/libxgc2_adapter_link_client.so"
test -f "${package_root}/usr/lib/${multiarch}/libxgc2_adapter_link_protocol.so"
test -f "${package_root}/usr/lib/${multiarch}/cmake/xgc2_adapter_link_client/xgc2_adapter_link_clientConfig.cmake"
test -f "${package_root}/usr/lib/${multiarch}/pkgconfig/xgc2-adapter-link-client.pc"

find "${package_root}" -type d -exec chmod 0755 {} +
find "${package_root}" -type f -exec chmod 0644 {} +
chmod 0755 "${package_root}/DEBIAN" \
  "${package_root}/DEBIAN/postinst" "${package_root}/DEBIAN/postrm"
find "${package_root}/usr/lib/${multiarch}" -maxdepth 1 -type f \
  -name 'libxgc2_adapter_link_*.so*' -exec chmod 0755 {} +
find "${package_root}/usr/lib/${multiarch}" -maxdepth 1 -type f \
  -name 'libxgc2_adapter_link_*.so*' -exec strip --strip-unneeded {} + \
  2>/dev/null || true

deb_path="${output_dir}/${package_name}_${version}_${architecture}.deb"
fakeroot dpkg-deb --build "${package_root}" "${deb_path}" >/dev/null
dpkg-deb -I "${deb_path}"
echo "Debian artifact written to ${deb_path}"
