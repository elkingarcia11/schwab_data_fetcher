#!/bin/bash

# Continuous Trading System Startup Script
# Run with default symbols: SPY, META, AMZN, NVDA, TSLA

echo "🚀 Starting Continuous Trading System..."
echo "📊 Symbols: SPY, META, AMZN, NVDA, TSLA"
echo "⏰ Frequencies: 5m, 10m, 15m, 30m"
echo "🏥 Health checks every 4 minutes"
echo ""
echo "Press Ctrl+C to stop gracefully"
echo "=" * 50

# Change to script directory
cd "$(dirname "$0")"

# Ensure logs directory exists
mkdir -p logs

# Start the continuous trading system
python3 continuous_trader.py "SPY,META,AMZN,NVDA,TSLA" 