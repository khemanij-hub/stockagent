# GCE VM Deployment Guide (Kite + IB)

This guide uses:
- Code path: `/opt/agentbot/India`
- Python venv: `/opt/agentbot/venv`
- Service user: `agentbot`
- Env files: `/etc/agentbot/kite.env`, `/etc/agentbot/ib.env`

## Quick path (recommended)

After cloning your repo on the VM, run one setup command:

```bash
git clone <your-repo-url> /opt/agentbot/India
cd /opt/agentbot/India
sudo bash deploy/gce/setup_from_clone.sh
```

This script creates runtime directories, installs Python deps, copies your current `deploy/gce/kite.env` and `deploy/gce/ib.env` into `/etc/agentbot`, installs systemd services, and reloads systemd.

Then start services:

```bash
sudo systemctl enable --now kite-bot
sudo systemctl enable --now ib-bot
```

Use the detailed steps below if you prefer manual install.

## 1) Prepare VM

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git

sudo useradd -m -s /bin/bash agentbot || true
sudo mkdir -p /opt/agentbot /etc/agentbot
sudo mkdir -p /var/lib/agentbot/kite /var/lib/agentbot/ib
sudo mkdir -p /var/log/agentbot/kite /var/log/agentbot/ib
sudo chown -R agentbot:agentbot /opt/agentbot /var/lib/agentbot /var/log/agentbot
sudo chmod 750 /etc/agentbot
```

## 2) Copy code to VM

Put your project under:
- `/opt/agentbot/India`

Required CSV files:
- `/opt/agentbot/India/India Stocks.csv`
- `/opt/agentbot/India/US Stocks.csv`

## 3) Create venv and install dependencies

```bash
sudo -u agentbot python3 -m venv /opt/agentbot/venv
sudo -u agentbot /opt/agentbot/venv/bin/pip install --upgrade pip

# Install your exact dependencies file here:
sudo -u agentbot /opt/agentbot/venv/bin/pip install -r /opt/agentbot/India/requirements_vm.txt
```

## 4) Create env files

Copy templates from this folder:
- `deploy/gce/kite.env.example` -> `/etc/agentbot/kite.env`
- `deploy/gce/ib.env.example` -> `/etc/agentbot/ib.env`

Pre-filled defaults are also provided:
- `deploy/gce/kite.env`
- `deploy/gce/ib.env`

```bash
sudo cp /opt/agentbot/India/deploy/gce/kite.env /etc/agentbot/kite.env
sudo cp /opt/agentbot/India/deploy/gce/ib.env /etc/agentbot/ib.env
sudo chown root:root /etc/agentbot/kite.env /etc/agentbot/ib.env
sudo chmod 600 /etc/agentbot/kite.env /etc/agentbot/ib.env
```

Edit both files and set real values:
```bash
sudo nano /etc/agentbot/kite.env
sudo nano /etc/agentbot/ib.env
```

## 5) Install systemd services

Copy service units:
- `deploy/gce/kite-bot.service` -> `/etc/systemd/system/kite-bot.service`
- `deploy/gce/ib-bot.service` -> `/etc/systemd/system/ib-bot.service`

```bash
sudo cp /opt/agentbot/India/deploy/gce/kite-bot.service /etc/systemd/system/kite-bot.service
sudo cp /opt/agentbot/India/deploy/gce/ib-bot.service /etc/systemd/system/ib-bot.service
sudo systemctl daemon-reload
sudo systemctl enable --now kite-bot
sudo systemctl enable --now ib-bot
```

## 6) Verify

```bash
sudo systemctl status kite-bot --no-pager
sudo systemctl status ib-bot --no-pager

sudo journalctl -u kite-bot -f
sudo journalctl -u ib-bot -f
```

## 7) Daily operations

Kite access token rotates daily.

Update token in `/etc/agentbot/kite.env`:
```bash
KITE_ACCESS_TOKEN=NEW_DAILY_TOKEN
```

Then restart:
```bash
sudo systemctl restart kite-bot
```

## Required variables summary

Kite (`/etc/agentbot/kite.env`):
- `KITE_API_KEY`
- `KITE_API_SECRET`
- `KITE_AUTH_MODE=non_interactive`
- One of:
  - `KITE_ACCESS_TOKEN` (recommended on VM)
  - `KITE_REQUEST_TOKEN` (optional one-time exchange path)

IB (`/etc/agentbot/ib.env`):
- `IB_HOST`
- `IB_PORT`
- `IB_CLIENT_ID`

Recommended for both:
- `BOT_BASE_DIR`
- `BOT_DATA_DIR`
- `BOT_LOG_DIR`
- `LOG_FILE_PATH`
