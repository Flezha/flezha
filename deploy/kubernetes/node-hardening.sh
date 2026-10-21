#!/usr/bin/env bash
# Run once per cluster node (sudo). Idempotent. Makes a node freeze
# diagnosable + cheaply self-recovering without adding any daemon/log volume.
#   1. journald: persist across reboot, hard-capped at 200M (the 2026-06-11
#      rpi4 wedge was unexplainable because logs lived only in volatile /run).
#   2. hung_task_panic: a >120s uninterruptible-sleep hang reboots (panic=10)
#      and leaves its stack in the now-persistent journal.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }

install -d -m 2755 /var/log/journal
install -d /etc/systemd/journald.conf.d /etc/sysctl.d
cat > /etc/systemd/journald.conf.d/persist.conf <<'EOF'
[Journal]
Storage=persistent
SystemMaxUse=200M
MaxRetentionSec=14day
EOF
systemctl restart systemd-journald
journalctl --flush   # migrate /run -> /var/log/journal; persistent only takes effect after this

cat > /etc/sysctl.d/90-hung-task.conf <<'EOF'
kernel.hung_task_panic = 1
kernel.panic = 10
EOF
sysctl -p /etc/sysctl.d/90-hung-task.conf

echo "node-hardening applied on $(hostname)"
