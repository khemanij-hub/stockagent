#!/usr/bin/env bash
set -euo pipefail

SERVICE_USER="${SERVICE_USER:-agentbot}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/agentbot}"
CODE_DIR="${CODE_DIR:-$INSTALL_ROOT/India}"
VENV_PATH="${VENV_PATH:-$INSTALL_ROOT/venv}"
ETC_DIR="${ETC_DIR:-/etc/agentbot}"
DATA_ROOT="${DATA_ROOT:-/var/lib/agentbot}"
LOG_ROOT="${LOG_ROOT:-/var/log/agentbot}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

require_file() {
  local file_path="$1"
  if [[ ! -f "${file_path}" ]]; then
    echo "Missing required file: ${file_path}" >&2
    exit 1
  fi
}

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root (example: sudo bash deploy/gce/setup_from_clone.sh)" >&2
  exit 1
fi

require_file "${REPO_ROOT}/deploy/gce/kite.env"
require_file "${REPO_ROOT}/deploy/gce/ib.env"
require_file "${REPO_ROOT}/deploy/gce/kite-bot.service"
require_file "${REPO_ROOT}/deploy/gce/ib-bot.service"
require_file "${REPO_ROOT}/kite_options_brain_csv.py"
require_file "${REPO_ROOT}/ib_options_brain_csv.py"

apt update
apt install -y python3 python3-venv python3-pip git

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "${SERVICE_USER}"
fi

mkdir -p "${INSTALL_ROOT}" "${ETC_DIR}"
mkdir -p "${DATA_ROOT}/kite" "${DATA_ROOT}/ib"
mkdir -p "${LOG_ROOT}/kite" "${LOG_ROOT}/ib"
chmod 750 "${ETC_DIR}"
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${INSTALL_ROOT}" "${DATA_ROOT}" "${LOG_ROOT}"

mkdir -p "${CODE_DIR}"
REPO_ROOT_REAL="$(cd "${REPO_ROOT}" && pwd -P)"
CODE_DIR_REAL="$(cd "${CODE_DIR}" && pwd -P)"
if [[ "${REPO_ROOT_REAL}" != "${CODE_DIR_REAL}" ]]; then
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --exclude ".git/" "${REPO_ROOT}/" "${CODE_DIR}/"
  else
    cp -a "${REPO_ROOT}/." "${CODE_DIR}/"
    rm -rf "${CODE_DIR}/.git"
  fi
fi
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${CODE_DIR}"

if [[ ! -d "${VENV_PATH}" ]]; then
  sudo -u "${SERVICE_USER}" python3 -m venv "${VENV_PATH}"
fi
sudo -u "${SERVICE_USER}" "${VENV_PATH}/bin/pip" install --upgrade pip

if [[ -f "${CODE_DIR}/requirements_vm.txt" ]]; then
  sudo -u "${SERVICE_USER}" "${VENV_PATH}/bin/pip" install -r "${CODE_DIR}/requirements_vm.txt"
elif [[ -f "${CODE_DIR}/requirements.txt" ]]; then
  sudo -u "${SERVICE_USER}" "${VENV_PATH}/bin/pip" install -r "${CODE_DIR}/requirements.txt"
else
  echo "No requirements file found. Skipping pip dependency install." >&2
fi

cp "${CODE_DIR}/deploy/gce/kite.env" "${ETC_DIR}/kite.env"
cp "${CODE_DIR}/deploy/gce/ib.env" "${ETC_DIR}/ib.env"
chown root:root "${ETC_DIR}/kite.env" "${ETC_DIR}/ib.env"
chmod 600 "${ETC_DIR}/kite.env" "${ETC_DIR}/ib.env"

cp "${CODE_DIR}/deploy/gce/kite-bot.service" /etc/systemd/system/kite-bot.service
cp "${CODE_DIR}/deploy/gce/ib-bot.service" /etc/systemd/system/ib-bot.service

systemctl daemon-reload

echo "Setup complete."
echo "Next commands:"
echo "  sudo systemctl enable --now kite-bot"
echo "  sudo systemctl enable --now ib-bot"
echo "  sudo systemctl status kite-bot --no-pager"
echo "  sudo systemctl status ib-bot --no-pager"
