#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dependency_lock="${script_dir}/../dependencies/xgc2-protobuf.env"
if [[ ! -f "${dependency_lock}" ]]; then
  echo "missing protobuf dependency lock: ${dependency_lock}" >&2
  exit 1
fi
# shellcheck source=../dependencies/xgc2-protobuf.env
source "${dependency_lock}"

if [[ ! "${XGC2_PROTOBUF_PROTOCOL_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid protobuf protocol version: ${XGC2_PROTOBUF_PROTOCOL_VERSION}" >&2
  exit 1
fi
if [[ ! "${XGC2_PROTOBUF_STANDALONE_SOURCE_REF}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "XGC2_PROTOBUF_STANDALONE_SOURCE_REF must name an immutable protocol commit; current value: ${XGC2_PROTOBUF_STANDALONE_SOURCE_REF}" >&2
  exit 1
fi

distribution="${1:-${PACKAGE_DISTRIBUTION:-}}"
if [[ -z "${distribution}" && -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  distribution="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
fi
case "${distribution}" in
  focal|jammy|noble) ;;
  *)
    echo "unsupported protobuf dependency distribution: ${distribution:-<empty>}" >&2
    exit 1
    ;;
esac
protocol_version_pattern="${XGC2_PROTOBUF_PROTOCOL_VERSION//./\\.}"
selected_version=""

if [[ -n "${XGC2_APT_OVERLAY_URL:-}" ]]; then
  "${script_dir}/configure_xgc2_apt.sh" "${distribution}"
  selected_version="$(apt-cache policy xgc2-protobuf-dev | awk '/Candidate:/ {print $2; exit}')"
  if [[ -z "${selected_version}" || "${selected_version}" == "(none)" ]]; then
    echo "release overlay has no xgc2-protobuf-dev candidate for ${distribution}" >&2
    exit 1
  fi
  if [[ ! "${selected_version}" =~ ^${protocol_version_pattern}-[0-9]+~${distribution}$ ]]; then
    echo "release overlay selected incompatible xgc2-protobuf-dev ${selected_version}; expected ${XGC2_PROTOBUF_PROTOCOL_VERSION} protocol line for ${distribution}" >&2
    exit 1
  fi
  apt-get install -y --no-install-recommends \
    "xgc2-protobuf-dev=${selected_version}"
else
  source_url="https://github.com/lxk36/xgc2-protobuf.git"
  work_dir="$(mktemp -d -t xgc2-protobuf-source-XXXXXX)"
  trap 'rm -rf "${work_dir}"' EXIT
  apt-get update
  apt-get install -y --no-install-recommends \
    ca-certificates fakeroot git protobuf-compiler python3 \
    python3-protobuf python3-yaml
  git init -q "${work_dir}/source"
  git -C "${work_dir}/source" remote add origin "${source_url}"
  git -C "${work_dir}/source" fetch --depth 1 origin "${XGC2_PROTOBUF_STANDALONE_SOURCE_REF}"
  git -C "${work_dir}/source" checkout -q --detach FETCH_HEAD
  source_version="$(
    sed -n 's/^version:[[:space:]]*//p' \
      "${work_dir}/source/.xgc2/product.yml" | head -n 1
  )"
  if [[ ! "${source_version}" =~ ^${protocol_version_pattern}-[0-9]+$ ]]; then
    echo "standalone protobuf source ${source_version:-<empty>} is outside the ${XGC2_PROTOBUF_PROTOCOL_VERSION} protocol line" >&2
    exit 1
  fi
  selected_version="${source_version}~${distribution}"
  PACKAGE_DISTRIBUTION="${distribution}" \
    XGC2_PROTOBUF_WORK_DIR="${work_dir}/work" \
    XGC2_PROTOBUF_DEB_OUTPUT_DIR="${work_dir}/debs" \
    "${work_dir}/source/.xgc2/scripts/build_deb.sh"
  apt-get install -y "${work_dir}"/debs/xgc2-protobuf-dev_*.deb
fi

installed_version="$(dpkg-query -W -f='${Version}' xgc2-protobuf-dev)"
if [[ "${installed_version}" != "${selected_version}" ]]; then
  echo "xgc2-protobuf-dev ${installed_version} does not equal selected ${selected_version}" >&2
  exit 1
fi
if [[ ! "${installed_version}" =~ ^${protocol_version_pattern}-[0-9]+~${distribution}$ ]]; then
  echo "installed xgc2-protobuf-dev ${installed_version} is outside the ${XGC2_PROTOBUF_PROTOCOL_VERSION} protocol line for ${distribution}" >&2
  exit 1
fi
