#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dev_package="libxgc2-adapter-runtime-client-dev"
runtime_package="libxgc2-adapter-runtime-client2"
distribution="${PACKAGE_DISTRIBUTION:-}"
build_dir="${XGC2_ADAPTER_RUNTIME_BUILD_DIR:-${repo_root}/.ci/build}"
stage_dir="${XGC2_ADAPTER_RUNTIME_STAGE_DIR:-${repo_root}/.ci/stage}"
output_dir="${XGC2_ADAPTER_RUNTIME_DEB_OUTPUT_DIR:-${repo_root}/debs}"
dev_package_root="${repo_root}/.ci/pkg/${dev_package}"
runtime_package_root="${repo_root}/.ci/pkg/${runtime_package}"
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
library_version="${base_version%%-*}"
if [[ ! "${library_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid shared-library version derived from ${base_version}" >&2
  exit 1
fi
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

rm -rf \
  "${build_dir}" \
  "${stage_dir}" \
  "${dev_package_root}" \
  "${runtime_package_root}" \
  "${output_dir}"
mkdir -p "${build_dir}" "${output_dir}"
cmake -S "${repo_root}" -B "${build_dir}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr \
  -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
  -DXGC2_ADAPTER_RUNTIME_CLIENT_BUILD_TESTING=OFF
cmake --build "${build_dir}" -- -j"$(nproc)"
DESTDIR="${stage_dir}" cmake --build "${build_dir}" --target install

stage_lib_dir="${stage_dir}/usr/lib/${multiarch}"
dev_lib_dir="${dev_package_root}/usr/lib/${multiarch}"
runtime_lib_dir="${runtime_package_root}/usr/lib/${multiarch}"

mkdir -p \
  "${dev_package_root}/DEBIAN" \
  "${dev_package_root}/usr/share/doc/${dev_package}" \
  "${runtime_package_root}/DEBIAN" \
  "${runtime_package_root}/usr/share/doc/${runtime_package}" \
  "${runtime_lib_dir}"
cp -a "${stage_dir}/usr" "${dev_package_root}/"

for library in xgc2_adapter_runtime_client xgc2_adapter_runtime_protocol; do
  versioned="${stage_lib_dir}/lib${library}.so.${library_version}"
  soname_link="${stage_lib_dir}/lib${library}.so.2"
  test -f "${versioned}"
  test -L "${soname_link}"
  readelf -d "${versioned}" | grep -Fq "Library soname: [lib${library}.so.2]"
  cp -a "${versioned}" "${soname_link}" "${runtime_lib_dir}/"
done
find "${dev_lib_dir}" -maxdepth 1 \
  \( -type f -o -type l \) \
  -name 'libxgc2_adapter_runtime_*.so.*' -delete

cp -a "${repo_root}/README.md" "${repo_root}/LICENSE" \
  "${dev_package_root}/usr/share/doc/${dev_package}/"
cp -a "${repo_root}/README.md" "${repo_root}/LICENSE" \
  "${runtime_package_root}/usr/share/doc/${runtime_package}/"

shlibdeps_dir="${repo_root}/.ci/shlibdeps"
mkdir -p "${shlibdeps_dir}/debian"
cat > "${shlibdeps_dir}/debian/control" <<EOF
Source: xgc2-adapter-runtime-client-cpp
Section: libs
Priority: optional
Maintainer: XGC2 <apt@example.com>

Package: ${runtime_package}
Architecture: any

Package: ${dev_package}
Architecture: any
EOF
cat > "${shlibdeps_dir}/debian/shlibs.local" <<EOF
libxgc2_adapter_runtime_client 2 ${runtime_package} (>= ${version})
libxgc2_adapter_runtime_protocol 2 ${runtime_package} (>= ${version})
EOF
shlibdeps_output="$(
  cd "${shlibdeps_dir}"
  dpkg-shlibdeps -O \
    "-x${runtime_package}" \
    "-l${stage_lib_dir}" \
    "-e${stage_lib_dir}/libxgc2_adapter_runtime_client.so.${library_version}" \
    "-e${stage_lib_dir}/libxgc2_adapter_runtime_protocol.so.${library_version}"
)"
runtime_depends="${shlibdeps_output#shlibs:Depends=}"
if [[ "${runtime_depends}" == "${shlibdeps_output}" || -z "${runtime_depends}" ]]; then
  echo "dpkg-shlibdeps did not produce runtime dependencies" >&2
  exit 1
fi

cat > "${runtime_package_root}/DEBIAN/control" <<EOF
Package: ${runtime_package}
Version: ${version}
Section: libs
Priority: optional
Architecture: ${architecture}
Multi-Arch: same
Maintainer: XGC2 <apt@example.com>
Depends: ${runtime_depends}
Breaks: ${dev_package} (<< ${version})
Replaces: ${dev_package} (<< ${version})
Description: XGC2 Adapter Runtime C++ shared libraries
 ABI 2 shared libraries for AdapterRuntimeLink clients and generated protocol
 bindings. Headers, schemas, CMake exports, and pkg-config metadata are kept in
 the separate development package.
EOF
cat > "${runtime_package_root}/DEBIAN/shlibs" <<EOF
libxgc2_adapter_runtime_client 2 ${runtime_package} (>= ${version})
libxgc2_adapter_runtime_protocol 2 ${runtime_package} (>= ${version})
EOF

cat > "${dev_package_root}/DEBIAN/control" <<EOF
Package: ${dev_package}
Version: ${version}
Section: libdevel
Priority: optional
Architecture: ${architecture}
Maintainer: XGC2 <apt@example.com>
Depends: ${runtime_package} (= ${version}), xgc2-protobuf-dev (= ${protobuf_deb_version}), libgrpc++-dev, libprotobuf-dev
Description: Generic XGC2 Adapter Runtime C++ SDK
 Capability-first AdapterRuntimeLink client and generated protocol libraries,
 public headers, CMake exports, and pkg-config metadata. The SDK owns trusted
 bootstrap, registration, paired Control/Work streams, spec application,
 bounded dispatch, source-stream credit, replay, reconnect, and shutdown.
EOF

cat > "${runtime_package_root}/DEBIAN/postinst" <<'SH'
#!/bin/sh
set -e
command -v ldconfig >/dev/null 2>&1 && ldconfig
SH
cat > "${runtime_package_root}/DEBIAN/postrm" <<'SH'
#!/bin/sh
set -e
command -v ldconfig >/dev/null 2>&1 && ldconfig
SH

test -f "${dev_package_root}/usr/include/xgc2/adapter_runtime/client.hpp"
test -f "${dev_package_root}/usr/include/xgc2/adapter_runtime/version.hpp"
test -f "${dev_package_root}/usr/include/xgc/adapter/v1/adapter.pb.h"
test -f "${dev_package_root}/usr/include/xgc/adapter/v1/adapter.grpc.pb.h"
test -L "${dev_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so"
test -L "${dev_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so"
test ! -e "${dev_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so.2"
test ! -e "${dev_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so.2"
test -f "${dev_package_root}/usr/lib/${multiarch}/cmake/xgc2_adapter_runtime_client/xgc2_adapter_runtime_clientConfig.cmake"
test -f "${dev_package_root}/usr/lib/${multiarch}/pkgconfig/xgc2-adapter-runtime-client.pc"
test ! -e "${runtime_package_root}/usr/include"
test ! -e "${runtime_package_root}/usr/lib/${multiarch}/cmake"
test ! -e "${runtime_package_root}/usr/lib/${multiarch}/pkgconfig"
test -L "${runtime_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so.2"
test -L "${runtime_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so.2"
test -f "${runtime_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so.${library_version}"
test -f "${runtime_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so.${library_version}"
test ! -e "${runtime_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_client.so"
test ! -e "${runtime_package_root}/usr/lib/${multiarch}/libxgc2_adapter_runtime_protocol.so"

for package_root in "${dev_package_root}" "${runtime_package_root}"; do
  find "${package_root}" -type d -exec chmod 0755 {} +
  find "${package_root}" -type f -exec chmod 0644 {} +
  chmod 0755 "${package_root}/DEBIAN"
done
chmod 0755 \
  "${runtime_package_root}/DEBIAN/postinst" \
  "${runtime_package_root}/DEBIAN/postrm"
find "${runtime_package_root}/usr/lib/${multiarch}" -maxdepth 1 -type f \
  -name 'libxgc2_adapter_runtime_*.so*' -exec chmod 0755 {} +
find "${runtime_package_root}/usr/lib/${multiarch}" -maxdepth 1 -type f \
  -name 'libxgc2_adapter_runtime_*.so*' -exec strip --strip-unneeded {} + \
  2>/dev/null || true

runtime_deb="${output_dir}/${runtime_package}_${version}_${architecture}.deb"
dev_deb="${output_dir}/${dev_package}_${version}_${architecture}.deb"
fakeroot dpkg-deb --build "${runtime_package_root}" "${runtime_deb}" >/dev/null
fakeroot dpkg-deb --build "${dev_package_root}" "${dev_deb}" >/dev/null
dpkg-deb -I "${runtime_deb}"
dpkg-deb -I "${dev_deb}"
echo "Debian artifacts written to ${runtime_deb} and ${dev_deb}"
