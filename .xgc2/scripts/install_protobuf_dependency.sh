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

if [[ -n "${XGC2_APT_OVERLAY_URL:-}" ]]; then
  "${script_dir}/configure_xgc2_apt.sh" "${distribution}"
  apt-get install -y --no-install-recommends xgc2-protobuf-dev
else
  source_url="${XGC2_PROTOBUF_SOURCE_URL:-https://github.com/lxk36/xgc2-protobuf.git}"
  work_dir="$(mktemp -d -t xgc2-protobuf-source-XXXXXX)"
  trap 'rm -rf "${work_dir}"' EXIT
  apt-get update
  apt-get install -y --no-install-recommends \
    ca-certificates fakeroot git protobuf-compiler python3 \
    python3-jsonschema python3-protobuf python3-yaml
  git init -q "${work_dir}/source"
  git -C "${work_dir}/source" remote add origin "${source_url}"
  git -C "${work_dir}/source" fetch --depth 1 origin "${XGC2_PROTOBUF_SOURCE_REF}"
  git -C "${work_dir}/source" checkout -q --detach FETCH_HEAD
  source_version="$(
    sed -n 's/^version:[[:space:]]*//p' \
      "${work_dir}/source/.xgc2/product.yml" | head -n 1
  )"
  if [[ "${source_version}" != "${XGC2_PROTOBUF_VERSION}" ]]; then
    echo "locked protobuf source version ${source_version:-<empty>} does not match ${XGC2_PROTOBUF_VERSION}" >&2
    exit 1
  fi
  PACKAGE_DISTRIBUTION="${distribution}" \
    XGC2_PROTOBUF_WORK_DIR="${work_dir}/work" \
    XGC2_PROTOBUF_DEB_OUTPUT_DIR="${work_dir}/debs" \
    "${work_dir}/source/.xgc2/scripts/build_deb.sh"
  apt-get install -y "${work_dir}"/debs/xgc2-protobuf-dev_*.deb
fi

installed_version="$(dpkg-query -W -f='${Version}' xgc2-protobuf-dev)"
minimum_version="${XGC2_PROTOBUF_VERSION}~${distribution}"
if ! dpkg --compare-versions "${installed_version}" ge "${minimum_version}"; then
  echo "xgc2-protobuf-dev ${installed_version} is older than ${minimum_version}" >&2
  exit 1
fi
