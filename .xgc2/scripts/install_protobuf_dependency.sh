#!/usr/bin/env bash

set -euo pipefail

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
  "$(dirname "${BASH_SOURCE[0]}")/configure_xgc2_apt.sh" "${distribution}"
  apt-get install -y --no-install-recommends xgc2-protobuf-dev
else
  source_ref="${XGC2_PROTOBUF_SOURCE_REF:-v0.2.0-1}"
  source_url="${XGC2_PROTOBUF_SOURCE_URL:-https://github.com/lxk36/xgc2-protobuf.git}"
  work_dir="$(mktemp -d -t xgc2-protobuf-source-XXXXXX)"
  apt-get update
  apt-get install -y --no-install-recommends \
    ca-certificates fakeroot git protobuf-compiler python3 \
    python3-jsonschema python3-protobuf python3-yaml
  git clone --depth 1 --branch "${source_ref}" "${source_url}" \
    "${work_dir}/source"
  PACKAGE_DISTRIBUTION="${distribution}" \
    XGC2_PROTOBUF_WORK_DIR="${work_dir}/work" \
    XGC2_PROTOBUF_DEB_OUTPUT_DIR="${work_dir}/debs" \
    "${work_dir}/source/.xgc2/scripts/build_deb.sh"
  apt-get install -y "${work_dir}"/debs/xgc2-protobuf-dev_*.deb
fi

installed_version="$(dpkg-query -W -f='${Version}' xgc2-protobuf-dev)"
minimum_version="0.2.0-1~${distribution}"
if ! dpkg --compare-versions "${installed_version}" ge "${minimum_version}"; then
  echo "xgc2-protobuf-dev ${installed_version} is older than ${minimum_version}" >&2
  exit 1
fi
