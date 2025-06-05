# Schwab Market Data Collection & Trading Signal System

## 🚀 **Automated Multi-Symbol, Multi-Timeframe Trading System**

A comprehensive, scheduled market data collection and trading signal analysis system that monitors **SPY, META, and AMZN** across **4 timeframes** (5m, 10m, 15m, 30m) for **LONG and SHORT position opportunities**.

### **🎯 Key Features**

- **📊 Multi-Symbol Monitoring**: SPY, META, AMZN (easily expandable)
- **⏰ Multi-Timeframe Analysis**: 5-minute, 10-minute, 15-minute, 30-minute
- **📈 LONG/SHORT Position Tracking**: Independent position states for each symbol/timeframe
- **🔄 Real-Time Signal Detection**: 3-condition trading logic (EMA, MACD, ROC)
- **📧 Instant Email Notifications**: Immediate alerts when positions open/close
- **🕘 Scheduled Execution**: Cron-based automation for market hours
- **🎯 Precision Trading**: Full numerical precision (no rounding errors)
- **🔐 Secure Authentication**: Schwab API integration with token management

---

## 📈 **Trading Strategy**

### **Signal Conditions (Applied to Both LONG & SHORT)**

1. **EMA Condition**: 7-period EMA > 17-period VWMA
2. **MACD Condition**: MACD Line > MACD Signal
3. **Momentum Condition**: 8-period ROC > 0%

### **Position Logic**

- **🟢 OPEN Position**: When ALL 3 conditions are met
- **🔴 CLOSE Position**: When ≤1 condition remains (2+ conditions fail)

### **LONG vs SHORT Positions**

- **LONG Positions**: Based on regular price data
- **SHORT Positions**: Based on inverse price data (1/price calculations)
- Each symbol/timeframe maintains independent LONG and SHORT position states

---

## 🏗️ **System Architecture**

### **Core Components**

```
📁 schwab_market_api/
├── 🎯 scheduled_coordinator.py    # Main execution engine (cron jobs run this)
├── 📡 data_fetcher.py            # Direct frequency data collection
├── 📊 indicator_calculator.py    # Technical indicator calculations
├── 🎯 position_tracker.py        # LONG/SHORT position logic
├── 📧 email_notifier.py          # Instant email notifications
├── 🔐 schwab_auth.py             # API authentication management
├── 🔍 status_check.py            # System health monitoring
├── 📋 cron_jobs.md               # Complete cron job configuration
└── 📄 README.md                  # This documentation
```

### **Data Files (Auto-Generated)**

```
📁 data/
├── SPY_5m.csv, SPY_5m_INVERSE.csv       # SPY 5-minute data
├── SPY_10m.csv, SPY_10m_INVERSE.csv     # SPY 10-minute data
├── SPY_15m.csv, SPY_15m_INVERSE.csv     # SPY 15-minute data
├── SPY_30m.csv, SPY_30m_INVERSE.csv     # SPY 30-minute data
├── META_5m.csv, META_5m_INVERSE.csv     # META data (all timeframes)
├── META_10m.csv, META_10m_INVERSE.csv
├── META_15m.csv, META_15m_INVERSE.csv
├── META_30m.csv, META_30m_INVERSE.csv
├── AMZN_5m.csv, AMZN_5m_INVERSE.csv     # AMZN data (all timeframes)
├── AMZN_10m.csv, AMZN_10m_INVERSE.csv
├── AMZN_15m.csv, AMZN_15m_INVERSE.csv
└── AMZN_30m.csv, AMZN_30m_INVERSE.csv
```

---

## ⚡ **Scheduled Execution Workflow**

Each cron job executes this complete workflow:

1. **🔐 Authentication Check** → Skip if invalid
2. **🕘 Market Hours Validation** → Skip if closed
3. **📡 Fetch New Data** → Only incremental updates
4. **➕ Append to CSV** → Add to existing files
5. **📊 Calculate Indicators** → Process new candles only
6. **🎯 Analyze Signals** → Check latest data point for position changes
7. **📧 Send Email Alerts** → Instant notifications if positions open/close

---

## 🛠️ **Setup Instructions**

### **1. Install Dependencies**

```bash
pip install pandas requests pytz python-dotenv
```

### **2. Configure Schwab API Credentials**

```bash
# Create credentials file
cp schwab_credentials.env.example schwab_credentials.env

# Edit with your Schwab API credentials
nano schwab_credentials.env
```

### **3. Configure Email Notifications**

```bash
# Create email configuration
cp email_credentials.env.example email_credentials.env

# Edit with your email settings
nano email_credentials.env
```

### **4. Test System Status**

```bash
# Check if everything is configured correctly
python status_check.py
```

### **5. Bootstrap Initial Data**

```bash
# Load historical data for all symbols/timeframes
python scheduled_coordinator.py SPY 5m --mode bootstrap
python scheduled_coordinator.py META 15m --mode bootstrap
python scheduled_coordinator.py AMZN 30m --mode bootstrap
```

### **6. Deploy Cron Jobs**

```bash
# Edit crontab
crontab -e

# Copy the complete configuration from cron_jobs.md
# This sets up 24 scheduled jobs (12 data streams + 12 bootstrap jobs)
```

---

## 🕘 **Cron Job Schedule**

### **Data Collection Jobs**

- **5-minute**: Every 5 minutes during market hours
- **10-minute**: Every 10 minutes during market hours
- **15-minute**: Every 15 minutes during market hours
- **30-minute**: Every 30 minutes during market hours

### **Market Hours**: 9:30 AM - 4:00 PM ET, Monday-Friday

### **Example Cron Jobs**

```bash
# SPY 5-minute data collection
*/5 9-16 * * 1-5 cd /path/to/schwab_market_api && python scheduled_coordinator.py SPY 5m

# META 15-minute data collection
0,15,30,45 9-16 * * 1-5 cd /path/to/schwab_market_api && python scheduled_coordinator.py META 15m

# AMZN 30-minute data collection
0,30 9-16 * * 1-5 cd /path/to/schwab_market_api && python scheduled_coordinator.py AMZN 30m
```

**📋 Complete configuration available in `cron_jobs.md`**

---

## 📊 **Position Tracking**

### **24 Independent Position Trackers**

- **SPY**: 4 timeframes × 2 position types (LONG/SHORT) = 8 trackers
- **META**: 4 timeframes × 2 position types = 8 trackers
- **AMZN**: 4 timeframes × 2 position types = 8 trackers

### **Position States**

- `CLOSED` → All position states start closed
- `OPENED` → Position opened when all 3 conditions met
- `CLOSED` → Position closed when ≤1 condition remains

### **Email Notifications Include**

- **Position Opens**: Entry price, conditions met, market context
- **Position Closes**: Exit price, P&L calculation, profit/loss analysis
- **Real-time delivery**: Sent immediately when signals occur

---

## 🔍 **Monitoring & Troubleshooting**

### **Check System Status**

```bash
python status_check.py
```

### **Monitor Execution Logs**

```bash
# View specific symbol/timeframe logs
tail -f /var/log/schwab_SPY_5m.log
tail -f /var/log/schwab_META_15m.log

# Monitor all execution logs
tail -f /var/log/schwab_*.log
```

### **Manual Testing**

```bash
# Test complete workflow
python scheduled_coordinator.py SPY 5m

# Test bootstrap mode
python scheduled_coordinator.py META 10m --mode bootstrap

# Test analysis only
python scheduled_coordinator.py AMZN 15m --mode analysis
```

### **Verify Cron Jobs**

```bash
# List active cron jobs
crontab -l

# Check cron service status
sudo systemctl status cron
```

---

## 📈 **Trading Performance**

### **Signal Detection**

- **Real-time analysis**: Checks latest data point only (not historical re-analysis)
- **Precision calculations**: Full numerical precision maintained throughout
- **Multi-timeframe**: Independent signals across all timeframes
- **Dual direction**: Simultaneous LONG and SHORT opportunity detection

### **Data Efficiency**

- **Incremental updates**: Only fetches new data since last execution
- **Direct frequency collection**: No aggregation needed (5m, 10m, 15m, 30m from API)
- **Smart filtering**: Excludes incomplete/forming candles
- **Authentication optimization**: Proactive token refresh

---

## 🎯 **Usage Examples**

### **Single Symbol/Timeframe Execution**

```bash
# Run SPY 5-minute complete workflow
python scheduled_coordinator.py SPY 5m

# Bootstrap META 15-minute historical data
python scheduled_coordinator.py META 15m --mode bootstrap

# Analyze AMZN 30-minute signals only
python scheduled_coordinator.py AMZN 30m --mode analysis
```

### **System Health Check**

```bash
# Complete system status
python status_check.py

# Expected output:
# ✅ Authentication: Valid
# ✅ Email Notifications: Configured
# ✅ Data Streams: 12/12 active
# ✅ Overall Status: 🚀 READY
```

---

## 🚀 **Key Advantages**

- **🎯 Precision**: Full numerical precision prevents signal accuracy issues
- **⚡ Efficiency**: Incremental data updates, no redundant processing
- **🔄 Scalability**: Easy to add new symbols or timeframes
- **📧 Real-time**: Instant email notifications when positions change
- **🛡️ Reliability**: Market hours validation, authentication checks
- **📊 Comprehensive**: 24 concurrent position trackers
- **🕘 Automated**: Complete hands-off operation via cron jobs
- **🏗️ Modern Architecture**: Clean, modular design with direct frequency fetching

### **🔄 Recent Architectural Improvements**

- ✅ **Eliminated aggregation complexity**: Direct API fetching for each timeframe
- ✅ **Removed legacy components**: Streamlined codebase with `scheduled_coordinator.py`
- ✅ **Enhanced modularity**: Independent, reusable components
- ✅ **Improved efficiency**: No redundant data processing or computation
- ✅ **Simplified deployment**: Single command execution via cron jobs

---

## 📋 **File Structure Summary**

```
schwab_market_api/
├── 🎯 Core System
│   ├── scheduled_coordinator.py     # Main execution engine
│   ├── data_fetcher.py             # Market data collection
│   ├── indicator_calculator.py     # Technical analysis
│   ├── position_tracker.py         # Trading signal logic
│   ├── email_notifier.py           # Alert system
│   └── schwab_auth.py              # API authentication
├── 🔧 Utilities
│   ├── status_check.py             # System health monitoring
│   └── cron_jobs.md                # Deployment configuration
├── 📄 Documentation
│   ├── README.md                   # This file
│   ├── schwab_credentials.env.example
│   └── email_credentials.env.example
└── 📊 Data (Auto-generated)
    └── data/                       # CSV files for all symbols/timeframes
```

---

## 🎉 **Ready for Production**

Your automated trading system is ready to monitor **3 major stocks** across **4 timeframes** for **LONG and SHORT opportunities** using proven technical indicators, with instant email notifications and comprehensive logging.

**Deploy with confidence!** 🚀📈📉
