#!/bin/bash
# Install Cron Jobs for Schwab Trading System + Sleep Management
# Run this script to set up complete automation

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRON_FILE="/tmp/schwab_cron_jobs.txt"

echo "🚀 Installing Schwab Trading System Cron Jobs..."
echo "📍 Project Directory: $SCRIPT_DIR"

# Create temporary cron file
cat > "$CRON_FILE" << 'EOF'
# ================================================================================================
# SCHWAB TRADING SYSTEM - AUTOMATED SCHEDULING
# Runs during market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
# Project: /Users/elkingarcia/Documents/python/schwab_market_api
# ================================================================================================

# ====== SLEEP MANAGEMENT ======
# Automatically manage sleep settings based on trading hours
0 9 * * 1-5 /Users/elkingarcia/Documents/python/schwab_market_api/sleep_manager.sh >> /var/log/sleep_manager.log 2>&1
30 16 * * 1-5 /Users/elkingarcia/Documents/python/schwab_market_api/sleep_manager.sh >> /var/log/sleep_manager.log 2>&1
0 0 * * 6,0 /Users/elkingarcia/Documents/python/schwab_market_api/sleep_manager.sh >> /var/log/sleep_manager.log 2>&1

# ====== 5-MINUTE JOBS ======
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 5m >> /var/log/schwab_SPY_5m.log 2>&1
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 5m >> /var/log/schwab_META_5m.log 2>&1
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 5m >> /var/log/schwab_AMZN_5m.log 2>&1

# ====== 10-MINUTE JOBS ======
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 10m >> /var/log/schwab_SPY_10m.log 2>&1
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 10m >> /var/log/schwab_META_10m.log 2>&1
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 10m >> /var/log/schwab_AMZN_10m.log 2>&1

# ====== 15-MINUTE JOBS ======
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 15m >> /var/log/schwab_SPY_15m.log 2>&1
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 15m >> /var/log/schwab_META_15m.log 2>&1
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 15m >> /var/log/schwab_AMZN_15m.log 2>&1

# ====== 30-MINUTE JOBS ======
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 30m >> /var/log/schwab_SPY_30m.log 2>&1
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 30m >> /var/log/schwab_META_30m.log 2>&1
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 30m >> /var/log/schwab_AMZN_30m.log 2>&1

# ====== BOOTSTRAP JOBS (Market Open) ======
31 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
32 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
33 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
34 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
35 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
36 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
37 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
38 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
39 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
40 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
41 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
42 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1

# ====== SYSTEM HEALTH CHECK ======
*/30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python status_check.py >> /var/log/schwab_health.log 2>&1

EOF

echo "📋 Installing cron jobs..."
crontab "$CRON_FILE"

echo ""
echo "✅ INSTALLATION COMPLETE!"
echo ""
echo "📊 Installed Jobs:"
echo "   • 12 Data Collection Jobs (SPY, META, AMZN × 4 timeframes)"
echo "   • 12 Bootstrap Jobs (Fill gaps at market open)"
echo "   • 3 Sleep Management Jobs (9am, 4:30pm, weekends)"
echo "   • 1 Health Check Job (Every 30 minutes)"
echo "   • Total: 28 scheduled jobs"
echo ""
echo "🕒 Schedule:"
echo "   • Sleep disabled: 9:00 AM - 4:30 PM (weekdays)"
echo "   • Data collection: 9:30 AM - 4:00 PM (weekdays)"
echo "   • Health checks: Every 30 minutes during market hours"
echo ""
echo "📁 Log Files:"
echo "   • Trading: /var/log/schwab_*.log"
echo "   • Sleep: /var/log/sleep_manager.log"
echo "   • Health: /var/log/schwab_health.log"
echo ""
echo "🔍 To monitor:"
echo "   • View all cron jobs: crontab -l"
echo "   • Check logs: tail -f /var/log/schwab_*.log"
echo "   • Test sleep manager: ./sleep_manager.sh"
echo ""

# Clean up
rm "$CRON_FILE"

echo "🚀 Your Mac mini is now fully automated for trading!"
echo "   System will stay awake during market hours and sleep after 4:30 PM." 