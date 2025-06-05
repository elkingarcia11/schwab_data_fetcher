# Schwab Market API - Continuous Trading System

A comprehensive Python-based automated trading system that monitors multiple stock symbols across different timeframes using technical indicators and executes LONG/SHORT position signals via email notifications.

## 🚀 **System Overview**

This system provides fully automated trading signal generation with:
- **Real-time monitoring** of 5 symbols: SPY, META, AMZN, NVDA, TSLA
- **Multi-timeframe analysis**: 5m, 10m, 15m, 30m intervals  
- **Dual position tracking**: LONG (regular) and SHORT (inverse) positions
- **Technical indicators**: EMA, VWMA, MACD, ROC
- **Automated email alerts** for position changes
- **Comprehensive bootstrap** from historical data
- **Market hours awareness** with automatic weekend handling

## 📊 **Architecture**

### **Core Components**

1. **`continuous_trader.py`** - Main orchestrator with multi-threaded execution
2. **`scheduled_coordinator.py`** - Trading logic coordinator with bootstrap functionality  
3. **`data_fetcher.py`** - Schwab API integration with historical data management
4. **`indicator_calculator.py`** - Technical indicator calculations (EMA, VWMA, MACD, ROC)
5. **`position_tracker.py`** - Position state management and signal generation
6. **`email_notifier.py`** - Email notification system for trading signals
7. **`schwab_auth.py`** - Schwab API authentication and token management

### **Trading Logic**

**Position Opening Conditions** (ALL must be met):
1. **EMA Condition**: EMA(7) > VWMA(17) 
2. **MACD Condition**: MACD Line > MACD Signal
3. **ROC Condition**: ROC(8) > 0

**Position Closing Conditions** (≤1 condition remaining):
- Positions close when 2 or more conditions fail

**Position Constraints**:
- Maximum 1 LONG + 1 SHORT position per timeframe per symbol
- 24 total position trackers (5 symbols × 4 timeframes × 2 directions)

## 🔧 **Installation & Setup**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Configure Schwab API**
Create `schwab_credentials.env`:
```env
SCHWAB_CLIENT_ID=your_client_id
SCHWAB_CLIENT_SECRET=your_client_secret  
SCHWAB_REDIRECT_URI=your_redirect_uri
```

### **3. Configure Email Notifications**
Create `email_credentials.env`:
```env
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
EMAIL_ADDRESS=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
RECIPIENT_EMAIL=recipient@gmail.com
```

### **4. Authenticate with Schwab**
```bash
python3 schwab_auth.py
```
Follow the OAuth flow to generate access tokens.

## 🎯 **Usage**

### **Quick Start**
```bash
# Start with default symbols (SPY, META, AMZN, NVDA, TSLA)
./start_trading.sh
```

### **Custom Symbols**
```bash
# Run with custom symbols
python3 continuous_trader.py "AAPL,GOOGL,MSFT"
```

### **Advanced Options**
```bash
# Custom health check interval (default: 4 minutes)
python3 continuous_trader.py "SPY,META,AMZN" --health-interval 300
```

### **Reliable All-Day Operation**
```bash
# Run with nohup and caffeinate for maximum reliability
nohup caffeinate -s python3 continuous_trader.py "SPY,META,AMZN,NVDA,TSLA" > logs/nohup.log 2>&1 &
```

## ⏰ **Execution Schedule**

The system runs continuously during market hours (9:30 AM - 4:00 PM ET) with:

- **5-minute jobs**: 9:30:05, 9:35:05, 9:40:05...
- **10-minute jobs**: 9:30:05, 9:40:05, 9:50:05...  
- **15-minute jobs**: 9:30:05, 9:45:05, 10:00:05...
- **30-minute jobs**: 9:30:05, 10:00:05, 10:30:05...

**Note**: 5-second offset ensures complete candle data availability.

## 🔄 **Bootstrap Process**

On startup, the system performs comprehensive bootstrap:

1. **Historical Data Fetch**: Previous trading day + today's complete candles
2. **Indicator Calculation**: Technical indicators for all historical data  
3. **Position Analysis**: Replay all historical signals (emails suppressed)
4. **State Restoration**: Set current position states based on historical analysis

This ensures the system continues exactly where it left off with accurate position tracking.

## 🏥 **Health Monitoring**

**Automatic Health Checks** (every 4 minutes):
- Market hours validation
- Authentication status
- Worker thread monitoring  
- Position status reporting
- Auto-refresh of expired tokens

## 📁 **File Structure**

```
schwab_market_api/
├── continuous_trader.py          # Main continuous trading system
├── scheduled_coordinator.py      # Trading logic coordinator
├── data_fetcher.py               # Schwab API data fetching
├── indicator_calculator.py       # Technical indicator calculations
├── position_tracker.py           # Position management & signals
├── email_notifier.py             # Email notification system
├── schwab_auth.py                # API authentication
├── start_trading.sh              # Quick start script
├── data/                         # CSV data storage
│   ├── SPY_5m.csv               # Regular price data
│   ├── SPY_5m_INVERSE.csv       # Inverse price data
│   └── ...                      # Other symbols/timeframes
├── logs/                         # System logs
│   ├── continuous_trader.log    # Main system log
│   └── nohup.log               # Background execution log
├── position_states.json         # Current position states
├── schwab_credentials.env       # API credentials
├── email_credentials.env        # Email configuration
└── requirements.txt             # Python dependencies
```

## 📧 **Email Notifications**

Automated emails are sent for:
- **Position openings**: When all 3 conditions are met
- **Position closings**: When ≤1 condition remains  
- **System alerts**: Authentication issues, errors

**Email includes**:
- Symbol and timeframe
- Position type (LONG/SHORT)
- Action (OPEN/CLOSE)
- Current price
- P&L information (for closings)
- Technical condition status

## 🔧 **System Management**

### **Monitor System**
```bash
# Check if running
ps aux | grep continuous_trader

# View live logs  
tail -f logs/continuous_trader.log

# View background logs
tail -f logs/nohup.log
```

### **Stop System**
```bash
# Graceful shutdown
pkill -f continuous_trader.py

# Force stop if needed
pkill -9 -f continuous_trader.py
```

### **Restart System**
```bash
# Quick restart with default symbols
./start_trading.sh

# Or reliable all-day restart
nohup caffeinate -s python3 continuous_trader.py "SPY,META,AMZN,NVDA,TSLA" > logs/nohup.log 2>&1 &
```

## 💡 **Key Features**

✅ **Comprehensive Coverage**: 5 symbols × 4 timeframes × 2 directions = 40 trading scenarios  
✅ **Real-time Execution**: Multi-threaded design with precise timing  
✅ **Historical Continuity**: Bootstrap ensures accurate position states  
✅ **Market Aware**: Automatic weekend/holiday handling  
✅ **Robust Architecture**: Health monitoring with auto-recovery  
✅ **Production Ready**: Designed for reliable all-day operation  
✅ **Email Integration**: Instant notifications for all trading signals  
✅ **Data Integrity**: Duplicate prevention and incomplete candle filtering

## ⚠️ **Important Notes**

- **Paper Trading**: This system generates signals only - no actual trades are executed
- **Market Hours**: Operates only during regular trading hours (9:30 AM - 4:00 PM ET)
- **Data Dependencies**: Requires active Schwab API access
- **Email Setup**: Ensure email credentials are configured for notifications
- **System Resources**: Multi-threaded design - monitor CPU/memory usage

## 🛡️ **Reliability Features**

- **Sleep Prevention**: `caffeinate` keeps system awake during trading hours
- **Background Execution**: `nohup` ensures operation continues if terminal closes  
- **Graceful Shutdown**: Handles interruption signals properly
- **Auto-Recovery**: Automatic token refresh and error handling
- **Comprehensive Logging**: Full audit trail of all operations

## 📈 **Trading Performance**

The system tracks and reports:
- Total signals generated per symbol/timeframe
- Position opening/closing counts
- P&L calculations for closed positions
- Technical condition success rates
- System uptime and reliability metrics

---

**Built for automated trading signal generation with institutional-grade reliability and comprehensive market coverage.**
