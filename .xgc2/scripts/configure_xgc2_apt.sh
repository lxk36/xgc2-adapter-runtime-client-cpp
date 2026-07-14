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
    echo "unsupported XGC2 APT distribution: ${distribution:-<empty>}" >&2
    exit 1
    ;;
esac

if [[ "${EUID}" -eq 0 ]]; then
  sudo_cmd=()
else
  sudo_cmd=(sudo)
fi

base_url="${XGC2_APT_OVERLAY_URL:-https://xgc2.apt.xiaokang.ink}"
base_url="${base_url%/}"
key_url="${XGC2_APT_KEY_URL:-https://xgc2.apt.xiaokang.ink/xgc2-archive-keyring.gpg}"

"${sudo_cmd[@]}" apt-get update
"${sudo_cmd[@]}" apt-get install -y --no-install-recommends ca-certificates curl gnupg
curl -fsSL "${key_url}" -o /tmp/xgc2-archive-keyring.gpg
gpg --show-keys --with-fingerprint --with-colons \
  /tmp/xgc2-archive-keyring.gpg 2>&1 \
  | grep -q '^fpr:.*:2A8E11B36F56D307ADF626D85E5FDC30979EA43F:$'
"${sudo_cmd[@]}" install -d -m 0755 /etc/apt/keyrings
"${sudo_cmd[@]}" install -m 0644 /tmp/xgc2-archive-keyring.gpg \
  /etc/apt/keyrings/xgc2-archive-keyring.gpg
echo "deb [signed-by=/etc/apt/keyrings/xgc2-archive-keyring.gpg] ${base_url} ${distribution} main" \
  | "${sudo_cmd[@]}" tee /etc/apt/sources.list.d/xgc2.list >/dev/null
"${sudo_cmd[@]}" apt-get update
