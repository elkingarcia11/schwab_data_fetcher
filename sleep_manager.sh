#!/bin/bash
# Sleep Manager for Trading Hours
# Prevents sleep during market hours (9:30 AM - 4:30 PM ET) on weekdays
# Allows sleep outside trading hours to save energy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/sleep_manager.log"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to disable sleep (for trading hours)
disable_sleep() {
    log_message "🔓 DISABLING sleep mode for trading hours"
    sudo pmset -c sleep 0
    sudo pmset -c disksleep 0
    sudo pmset -c displaysleep 0
    log_message "✅ Sleep disabled - system will stay awake for trading"
}

# Function to enable sleep (for after hours)
enable_sleep() {
    log_message "💤 ENABLING sleep mode for after hours"
    sudo pmset -c sleep 60        # Sleep after 60 minutes
    sudo pmset -c disksleep 10    # Disk sleep after 10 minutes
    sudo pmset -c displaysleep 10 # Display sleep after 10 minutes
    log_message "✅ Sleep enabled - system will sleep when idle"
}

# Check if it's a trading day (Monday-Friday)
is_trading_day() {
    local day_of_week=$(date +%u)  # 1=Monday, 7=Sunday
    [[ $day_of_week -le 5 ]]
}

# Check current time
current_hour=$(date +%-H)  # Remove leading zero
current_minute=$(date +%-M)  # Remove leading zero
current_time_minutes=$((current_hour * 60 + current_minute))

# Trading hours: 9:30 AM (570 minutes) to 4:30 PM (1050 minutes)
trading_start_minutes=570   # 9:30 AM
trading_end_minutes=1050    # 4:30 PM

log_message "🕒 Current time: $(date '+%H:%M') (${current_time_minutes} minutes since midnight)"

if is_trading_day; then
    log_message "📅 Today is a trading day (weekday)"
    
    if [[ $current_time_minutes -ge $trading_start_minutes && $current_time_minutes -le $trading_end_minutes ]]; then
        log_message "📈 Currently in trading hours (9:30 AM - 4:30 PM)"
        disable_sleep
    else
        log_message "🌙 Currently outside trading hours"
        enable_sleep
    fi
else
    log_message "📅 Today is not a trading day (weekend)"
    enable_sleep
fi

# Show current power settings
log_message "📊 Current power settings:"
pmset -g | grep -E "(sleep|disksleep|displaysleep)" | while read line; do
    log_message "   $line"
done 