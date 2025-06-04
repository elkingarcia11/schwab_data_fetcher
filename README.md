# 🚀 Schwab Continuous Market Data Collector with Position Tracking

A production-ready Python application that continuously streams real-time market data from Charles Schwab's API during trading hours. Features automatic token management, multi-timeframe aggregation, technical indicators, **position tracking with P&L analysis**, and **email notifications**.

## ✨ Key Features

- **🔄 Continuous Operation**: Runs automatically during market hours (9:30 AM - 4:00 PM ET)
- **🎯 Bootstrap + Incremental**: Initial complete update, then real-time incremental updates
- **⏰ Precise Timing**: API calls at XX:XX:05 (5 seconds after each minute boundary)
- **🔐 Smart Token Management**: Proactive refresh every 20 minutes + reactive refresh
- **📊 Multi-Timeframe**: 1m → 5m → 15m aggregation with perfect boundaries
- **📈 Technical Indicators**: 7-period EMA, 17-period VWMA, MACD, Signal Line, ROC-8
- **🎯 Position Tracking**: Automated open/close signals based on technical analysis
- **💰 P&L Analysis**: Real-time profit/loss calculations for all trades
- **📧 Email Notifications**: Instant alerts for position changes with detailed P&L
- **🛡️ Data Integrity**: Only complete periods aggregated, no partial candles
- **🔧 Production Ready**: Graceful shutdown, error recovery, weekend detection

## 🎯 Position Tracking System

### **Trading Logic**

- **Opens Position**: When ALL 3 conditions are met:

  1. 7 EMA > 17 VWMA
  2. MACD Line > MACD Signal
  3. ROC-8 > 0

- **Closes Position**: When ANY 2 of the 3 conditions fail (≤1 condition remaining)

### **Independent Timeframes**

- **1m, 5m, and 15m** tracked separately
- Each timeframe maintains its own position state
- Different entry/exit timing based on aggregation periods

### **P&L Tracking**

- **Opening Price**: Recorded when position opens
- **Closing Price**: Recorded when position closes
- **Profit/Loss**: Dollar amount and percentage calculated
- **Real-time**: Updates with every position change

## 📧 Email Notifications

### **Notification Types**

- **Position Opens**: Technical conditions, opening price, market context
- **Position Closes**: P&L analysis, closing price, profit/loss details

### **Email Content**

```
🚨 SPY 1m - CLOSE POSITION at $597.18 📈$0.3700

Position Change Details:
- Symbol: SPY
- Timeframe: 1m
- Action: CLOSE POSITION
- Time: 2025-06-04 10:38:00
- Price: $597.18
- Conditions Met: 1/3

P&L Analysis:
- Opening Price: $596.8100
- Closing Price: $597.1800
- Profit/Loss: 📈 $0.3700 (+0.06%)

Technical Indicators:
- 7 EMA: 597.0123
- 17 VWMA: 596.8456
- MACD Line: -0.002341
- MACD Signal: 0.001234
- ROC-8: -0.25%

Current Positions Status:
- 1m: CLOSED
- 5m: OPENED
- 15m: CLOSED
```

## 📁 Data Structure

```
data/
├── SPY_1m.csv   - Real-time 1-minute OHLC bars with indicators
├── SPY_5m.csv   - Aggregated 5-minute bars (:00, :05, :10, :15...)
└── SPY_15m.csv  - Aggregated 15-minute bars (:00, :15, :30, :45...)
```

Each CSV contains:

```csv
timestamp,datetime,open,high,low,close,volume,ema_7,vwma_17,ema_12,ema_26,macd_line,macd_signal,roc_8
1749043800000,2025-06-04 09:30:00,595.50,595.80,595.45,595.72,45120,595.68,595.61,595.65,595.58,0.070000,-0.002341,1.25
```

## 🚀 Quick Start

### **Installation**

```bash
# Clone and setup
git clone <repository-url>
cd schwab_market_api
pip install -r requirements.txt
```

### **Configuration**

1. **API Credentials** - Create `schwab_credentials.env`:

```env
SCHWAB_APP_KEY=your_app_key_here
SCHWAB_APP_SECRET=your_app_secret_here
```

2. **Email Notifications** - Create `email_credentials.env`:

```env
EMAIL_ALERTS_ENABLED=true
EMAIL_SENDER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_TO=recipient1@gmail.com, recipient2@gmail.com
```

3. **Setup Tokens**: Place your refresh token in `schwab_refresh_token.txt`

### **Usage Options**

#### **Continuous Mode with Position Tracking (Recommended)**

```bash
# Run continuously for SPY with position tracking
python market_data_updater.py

# Run continuously for custom symbol
python market_data_updater.py AAPL

# Monitor with logs
python market_data_updater.py | tee market_data.log
```

#### **Historical Position Analysis**

```bash
# Analyze all historical data for trading signals
python market_data_updater.py --analyze SPY

# Analyze custom symbol
python market_data_updater.py --analyze TSLA
```

#### **Single Update Mode**

```bash
# One-time update + position analysis for SPY
python market_data_updater.py --single

# One-time update + position analysis for custom symbol
python market_data_updater.py --single AAPL
```

#### **Status Check**

```bash
# Check system health and data freshness
python status_check.py
```

## 🔄 How Position Tracking Works

### **Historical Analysis (Bootstrap)**

- Analyzes ALL historical data chronologically
- Tracks position changes from first data point
- Sends retrospective email notifications for all signals
- Establishes current position state before real-time mode

### **Real-time Tracking (Continuous)**

- Checks position signals after each data update
- Sends immediate email notifications for new signals
- Maintains position state across all timeframes
- Calculates P&L for completed trades

### **Example P&L Performance**

From real SPY data analysis:

```
1-Minute Timeframe Results:
Trade 1: 📈 +$0.37 (+0.06%) - 10:14 to 10:38
Trade 2: 📈 +$0.14 (+0.02%) - 10:58 to 11:14
Trade 3: 📉 -$0.43 (-0.07%) - 11:42 to 11:48
Trade 4: 📉 -$0.43 (-0.07%) - 11:52 to 11:58
Trade 5: 📉 -$0.36 (-0.06%) - 12:20 to 12:29
Trade 6: 📈 +$0.86 (+0.14%) - 12:40 to 13:18
Trade 7: 📉 -$0.22 (-0.04%) - 13:36 to 13:40
Trade 8: 📈 +$0.09 (+0.01%) - 13:48 to 14:08

Net P&L: -$0.01 (breakeven)
Win Rate: 4/8 (50%)
```

## 📧 Email Setup Guide

### **Gmail Configuration**

1. **Enable 2-Factor Authentication** on your Google Account
2. **Generate App Password**:

   - Go to Google Account > Security > 2-Step Verification
   - Select "App passwords"
   - Choose "Mail" and generate password
   - Use this password in `EMAIL_PASSWORD`

3. **Configure Recipients**:
   - Single recipient: `EMAIL_TO=trader@gmail.com`
   - Multiple recipients: `EMAIL_TO=trader1@gmail.com, trader2@gmail.com`

### **Other Email Providers**

Update `smtp_server` and `smtp_port` in the code for:

- **Outlook**: `smtp-mail.outlook.com:587`
- **Yahoo**: `smtp.mail.yahoo.com:587`
- **Custom SMTP**: Modify `load_email_config()` method

## 🔄 How Continuous Mode Works

### **Phase 1: Bootstrap**

- Complete market data update to fill missing data
- **Historical position analysis** across all timeframes
- Retrospective email notifications for all discovered signals
- Establishes current position states

### **Phase 2: Incremental Real-time Updates**

- API calls every minute at XX:XX:05
- Updates indicators and aggregates timeframes
- **Live position signal checking** after each update
- Immediate email notifications for new signals
- P&L calculations for closed positions

### **Console Output Example**

```
🔄 Incremental Update Cycle #45
🕒 2025-06-04 14:23:05 ET
--------------------------------------------------
📊 Step 1: Updating 1-minute data...
✅ Retrieved 1 new completed candles
📊 Step 2: Aggregating to 5-minute data...
⏳ Skipping incomplete 5m period: 2025-06-04 14:20:00
📊 Step 3: Aggregating to 15-minute data...
⏳ Skipping incomplete 15m period: 2025-06-04 14:15:00
✅ Incremental cycle #45 completed successfully

🎯 Checking live position signals...

🚨 POSITION CHANGE: SPY_1m
   Action: CLOSE POSITION
   Time: 2025-06-04 14:23:00
   Price: $598.45
   Conditions: 1/3
   EMA>VWMA: True
   MACD>Sig: False
   ROC>0: False
   - Profit/Loss: 📈 $0.94 (+0.16%)

🚨 New position signals detected and processed!
📊 Current Positions: 1m:CLOSED | 5m:OPENED | 15m:CLOSED
```

## 📈 Technical Indicators

### **Core Trend Indicators**

**7 EMA (Exponential Moving Average)**

- **Purpose**: Fast-reacting short-term trend indicator
- **Formula**: `EMA = (Price × 0.25) + (Previous EMA × 0.75)`
- **Use**: Quick trend identification and entry signals

**12 EMA & 26 EMA**

- **Purpose**: MACD components for medium/long-term trend
- **Use**: Momentum analysis and trend confirmation

### **Volume-Based Indicators**

**17 VWMA (Volume Weighted Moving Average)**

- **Purpose**: Volume-adjusted price average
- **Formula**: `VWMA = Sum(Price × Volume) / Sum(Volume)`
- **Use**: More accurate price average considering volume impact

### **Momentum Indicators**

**MACD Line & Signal Line**

- **MACD**: `12 EMA - 26 EMA`
- **Signal**: `9 EMA of MACD Line`
- **Use**: Position entry/exit signals when MACD crosses Signal line

**ROC-8 (Rate of Change - 8 Period)**

- **Formula**: `((Current Price - Price 8 periods ago) / Price 8 periods ago) × 100`
- **Use**: Momentum measurement and trend strength

## 🚨 Live Performance Monitoring

### **Real-time Metrics**

- **Active Positions**: Track open positions across timeframes
- **P&L Tracking**: Running profit/loss for each timeframe
- **Signal Frequency**: Monitor entry/exit signal rate
- **Win Rate**: Track profitable vs losing trades

### **Email Alerts Include**

- Position entry/exit with precise timing
- Detailed technical analysis at signal time
- P&L calculations with dollar and percentage gains
- Current status across all timeframes
- Multiple recipient support for team notifications

## 🔧 Production Deployment

### **Systemd Service (Recommended)**

```ini
[Unit]
Description=Schwab Market Data Collector with Position Tracking
After=network.target

[Service]
Type=simple
User=trader
WorkingDirectory=/path/to/schwab_market_api
ExecStart=/path/to/venv/bin/python market_data_updater.py
Restart=always
RestartSec=30
Environment=TZ=America/New_York

[Install]
WantedBy=multi-user.target
```

### **Monitoring Commands**

```bash
# View real-time logs with position signals
tail -f market_data.log | grep -E "(🚨|📈|📉|🎯)"

# Check position changes only
tail -f market_data.log | grep "POSITION CHANGE"

# Monitor P&L results
tail -f market_data.log | grep "Profit/Loss"

# Check email notifications
tail -f market_data.log | grep "📧 Email sent"
```

## 🚨 Troubleshooting

### **Position Tracking Issues**

**No position signals generated:**

- ✅ Verify sufficient data for all indicators (26+ periods for MACD)
- ✅ Check indicator calculations in CSV files
- ✅ Ensure market hours and valid data timestamps

**Email notifications not sending:**

- ✅ Verify `email_credentials.env` configuration
- ✅ Test Gmail app password authentication
- ✅ Check network connectivity and SMTP settings
- ✅ Review console for email error messages

**P&L calculations incorrect:**

- ✅ Ensure opening prices are properly recorded
- ✅ Check for position state synchronization issues
- ✅ Verify price data accuracy in CSV files

### **Performance Metrics**

- **API Calls**: 1 per minute during market hours (390 calls/day)
- **Email Volume**: Variable based on signal frequency (typically 5-20/day)
- **Memory Usage**: ~60-120MB (includes position state tracking)
- **Network**: ~10KB per API response + email SMTP traffic

## 🔮 Future Enhancements

- **📊 Position Dashboard**: Web interface for real-time position monitoring
- **📈 Performance Analytics**: Historical P&L analysis and strategy optimization
- **🎯 Multi-Symbol Support**: Track positions across multiple stocks simultaneously
- **🔔 Advanced Alerts**: SMS, Slack, Discord notifications
- **📋 Trade Journal**: Automated trade logging with entry/exit reasoning
- **🤖 Strategy Backtesting**: Historical performance simulation tools

## 📄 License

MIT License - See LICENSE file for details

## 🆘 Support

1. **Check System Status**: Run `python status_check.py` for health check
2. **Test Position Analysis**: Run `python market_data_updater.py --analyze SPY`
3. **Verify Email Setup**: Check `email_credentials.env` configuration
4. **Monitor Logs**: Use grep commands above to filter relevant output
5. **Check Schwab API**: Visit Schwab developer documentation for API status

---

**⚠️ Disclaimer**: This tool is for educational and informational purposes only. Not financial advice. P&L calculations are for analysis only and do not represent actual trading results. Always verify data accuracy before making trading decisions.
