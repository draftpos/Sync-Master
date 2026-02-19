#!/bin/bash

# --- CONFIG ---
SERVICE_NAME="local_ws"
BENCH_PATH="/home/frappe/frappe-bench"
PYTHON_BIN="$BENCH_PATH/env/bin/python"
WEBSOCKET_SCRIPT="$BENCH_PATH/apps/sync_master/sync_master/sync_master/websockets/local.py"



SYSTEMD_FILE="/etc/systemd/system/$SERVICE_NAME.service"

# --- CHECK IF SERVICE EXISTS ---
if [ -f "$SYSTEMD_FILE" ]; then
    echo "Service file $SYSTEMD_FILE exists. Stopping and removing existing service ..."
    sudo systemctl stop $SERVICE_NAME.service
    sudo systemctl disable $SERVICE_NAME.service
    sudo rm -f $SYSTEMD_FILE
fi

# --- CREATE SYSTEMD SERVICE ---
echo "Creating systemd service file at $SYSTEMD_FILE ..."
sudo tee $SYSTEMD_FILE > /dev/null <<EOL
[Unit]
Description=Cloud WebSocket Sync Service
After=network.target

[Service]
Type=simple
User=frappe
WorkingDirectory=$BENCH_PATH
ExecStart=$PYTHON_BIN $WEBSOCKET_SCRIPT
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOL

# --- RELOAD SYSTEMD, ENABLE & START ---
echo "Reloading systemd ..."
sudo systemctl daemon-reload

echo "Enabling $SERVICE_NAME.service ..."
sudo systemctl enable $SERVICE_NAME.service

echo "Starting $SERVICE_NAME.service ..."
sudo systemctl start $SERVICE_NAME.service

echo "Status of $SERVICE_NAME.service:"
sudo systemctl status $SERVICE_NAME.service --no-pager
