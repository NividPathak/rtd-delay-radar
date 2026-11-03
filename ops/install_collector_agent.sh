#!/bin/sh
# Install (or reinstall) the launchd agent that keeps the laptop collector running.
# It starts at login and restarts within 30 seconds if the process exits.
#   Install:   sh ops/install_collector_agent.sh [profile]   (default profile: DEFAULT)
#   Status:    launchctl print gui/$(id -u)/com.rtd-delay-radar.collector | head
#   Uninstall: launchctl bootout gui/$(id -u)/com.rtd-delay-radar.collector
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.rtd-delay-radar.collector"
PROFILE="${1:-DEFAULT}"
TARGET="$HOME/Library/LaunchAgents/$LABEL.plist"

mkdir -p "$REPO/data/logs" "$HOME/Library/LaunchAgents"
sed -e "s|__REPO__|$REPO|g" -e "s|__PROFILE__|$PROFILE|g" "$REPO/ops/rtd-collector.plist.template" > "$TARGET"
plutil -lint "$TARGET"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$TARGET"
echo "installed $TARGET"
