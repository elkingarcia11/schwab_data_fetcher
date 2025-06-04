# 🚀 Schwab Continuous Market Data Collector

A production-ready Python application that continuously streams real-time market data from Charles Schwab's API during trading hours. Features automatic token management, multi-timeframe aggregation, technical indicators, and robust error handling.

## ✨ Key Features

- **🔄 Continuous Operation**: Runs automatically during market hours (9:30 AM - 4:00 PM ET)
- **🎯 Bootstrap + Incremental**: Initial complete update, then real-time incremental updates
- **⏰ Precise Timing**: API calls at XX:XX:05 (5 seconds after each minute boundary)
- **🔐 Smart Token Management**: Proactive refresh every 20 minutes + reactive refresh
- **📊 Multi-Timeframe**: 1m → 5m → 15m aggregation with perfect boundaries
- **📈 Technical Indicators**: 7-period EMA and 17-period VWMA for all timeframes
- **🛡️ Data Integrity**: Only complete periods aggregated, no partial candles
- **🔧 Production Ready**: Graceful shutdown, error recovery, weekend detection

## 📁 Data Structure

```
data/
├── SPY_1m.csv   - Real-time 1-minute OHLC bars with indicators
├── SPY_5m.csv   - Aggregated 5-minute bars (:00, :05, :10, :15...)
└── SPY_15m.csv  - Aggregated 15-minute bars (:00, :15, :30, :45...)
```

Each CSV contains:

```csv
timestamp,datetime,open,high,low,close,volume,ema_7,vwma_17
1749043800000,2025-06-04 09:30:00,595.50,595.80,595.45,595.72,45120,595.68,595.61
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

Create `schwab_credentials.env`:

```env
SCHWAB_APP_KEY=your_app_key_here
SCHWAB_APP_SECRET=your_app_secret_here
```

Setup initial tokens:

- Place your refresh token in `schwab_refresh_token.txt`
- Run initial authentication if needed

### **Usage Options**

#### **Continuous Mode (Recommended)**

```bash
# Run continuously for SPY during market hours
python market_data_updater.py

# Run continuously for custom symbol
python market_data_updater.py AAPL

# Monitor with logs
python market_data_updater.py | tee market_data.log
```

#### **Single Update Mode**

```bash
# One-time update for SPY
python market_data_updater.py --single

# One-time update for custom symbol
python market_data_updater.py --single TSLA
```

#### **Status Check**

```bash
# Check system health and data freshness
python status_check.py
```

## 🔄 How Continuous Mode Works

### **Two-Phase Operation**

#### **Phase 1: Bootstrap (Startup)**

- Runs complete market data update to fill any missing data
- Ensures all timeframes are current before starting incremental updates
- One-time initialization when program starts

#### **Phase 2: Incremental (Real-time)**

- Triggers every minute at XX:XX:05 (5 seconds after minute boundary)
- Fetches only new completed minute candles
- Updates indicators and aggregates to higher timeframes
- Runs continuously until market close

### **Aggregation Logic**

```
📊 Every Minute (at XX:05):
├── Fetch new 1m data from Schwab API
├── Filter out current forming minute (incomplete)
├── Add completed candles to SPY_1m.csv
├── Calculate 7 EMA and 17 VWMA indicators
├── Check for complete 5m periods → aggregate if ready
└── Check for complete 15m periods → aggregate if ready

🕒 5m Aggregation Timing:
├── 13:00-13:04 complete → Creates 13:00 5m candle at 13:05:05
├── 13:05-13:09 complete → Creates 13:05 5m candle at 13:10:05
└── Only aggregates when all 5 minutes are available

🕒 15m Aggregation Timing:
├── 13:00-13:14 complete → Creates 13:00 15m candle at 13:15:05
├── 13:15-13:29 complete → Creates 13:15 15m candle at 13:30:05
└── Only aggregates when all 15 minutes are available
```

### **Token Management**

- **Proactive Refresh**: Every 20 minutes automatically
- **Reactive Refresh**: When token expires or becomes invalid
- **5-Minute Buffer**: Treats tokens as expired 5 minutes before expiration
- **Fallback Logic**: Multiple retry mechanisms for reliability

## 📊 Live Console Output

### **Bootstrap Phase**

```
🔄 BOOTSTRAP PHASE: Filling any missing data...
------------------------------------------------------------
📊 Step 1: Updating 1-minute data...
✅ Retrieved 4 new completed candles
📊 Step 2: Aggregating to 5-minute data...
✅ Aggregated 5m period: 2025-06-04 13:10:00 (5 1m candles)
📊 Step 3: Aggregating to 15-minute data...
⏳ Skipping incomplete 15m period: 2025-06-04 13:00:00
✅ Bootstrap phase completed successfully
```

### **Incremental Phase**

```
🔄 INCREMENTAL PHASE: Starting real-time updates...
------------------------------------------------------------
⏰ Waiting 42.3 seconds until 13:35:05 ET...

🔄 Incremental Update Cycle #1
🕒 2025-06-04 13:35:05 ET
--------------------------------------------------
📊 Step 1: Updating 1-minute data...
✅ Retrieved 1 new completed candles
   New data range: 2025-06-04 13:34:00 to 2025-06-04 13:34:00
📊 Step 2: Aggregating to 5-minute data...
✅ Aggregated 5m period: 2025-06-04 13:30:00 (5 1m candles)
📊 Step 3: Aggregating to 15-minute data...
⏳ Skipping incomplete 15m period: 2025-06-04 13:15:00
✅ Incremental cycle #1 completed successfully
```

### **Status Indicators**

- ✅ Success operations
- ⚠️ Warning conditions
- ❌ Error situations
- 🕒 Token refresh events
- ⏰ Timing information
- 📊 Data statistics
- ⏳ Waiting for complete periods

## 🛡️ Data Integrity Features

### **Complete Candles Only**

- **1m Data**: Filters out current forming minute (only saves completed minutes)
- **5m Data**: Only aggregates when all 5 constituent minutes are available
- **15m Data**: Only aggregates when all 15 constituent minutes are available
- **No Partial Data**: Ensures historical data accuracy

### **Perfect Boundaries**

- **5m Periods**: :00, :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55
- **15m Periods**: :00, :15, :30, :45 (market standard)
- **Timezone Aware**: All calculations in ET (Eastern Time)

### **Duplicate Prevention**

- Timestamp-based filtering prevents duplicate entries
- Incremental aggregation logic avoids reprocessing existing periods
- Robust CSV validation and error handling

## 📈 Technical Indicators

### **7 EMA (Exponential Moving Average)**

- **Purpose**: Fast-reacting trend indicator
- **Formula**: `EMA = (Price × 0.25) + (Previous EMA × 0.75)`
- **Calculation**: Uses 2/(period+1) smoothing factor
- **Updates**: Recalculated for entire dataset after new data

### **17 VWMA (Volume Weighted Moving Average)**

- **Purpose**: Volume-adjusted price average
- **Formula**: `VWMA = Sum(Price × Volume) / Sum(Volume)`
- **Benefit**: More responsive to high-volume movements
- **Updates**: Recalculated for entire dataset after new data

## 🔧 Production Deployment

### **Systemd Service (Recommended)**

```ini
[Unit]
Description=Schwab Market Data Collector
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

Start the service:

```bash
sudo systemctl enable schwab-data-collector
sudo systemctl start schwab-data-collector
sudo systemctl status schwab-data-collector
```

### **Docker Container**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "market_data_updater.py"]
```

Build and run:

```bash
docker build -t schwab-collector .
docker run -d --name schwab-data --restart unless-stopped schwab-collector
```

### **Monitoring**

```bash
# View real-time logs
tail -f market_data.log

# Check system status
python status_check.py

# Monitor CSV data freshness
ls -la data/*.csv | awk '{print $6, $7, $8, $9}'
```

## 🚨 Troubleshooting

### **Common Issues**

**No incremental updates:**

- ✅ Verify market hours (9:30 AM - 4:00 PM ET, weekdays only)
- ✅ Check if Schwab API has new data (sometimes 1-2 minute delays)
- ✅ Look for timing messages: "⏰ Waiting X seconds until XX:XX:05 ET"

**Token refresh failures:**

- ✅ Verify `schwab_credentials.env` has correct APP_KEY and APP_SECRET
- ✅ Check `schwab_refresh_token.txt` isn't expired (90-day lifetime)
- ✅ Ensure network connectivity to Schwab API

**Missing aggregated data:**

- ✅ Check if enough 1m data exists for complete periods
- ✅ Look for "⏳ Skipping incomplete period" messages (normal behavior)
- ✅ 5m periods need 5 minutes, 15m periods need 15 minutes

**CSV file issues:**

- ✅ Check file permissions in `data/` directory
- ✅ Verify CSV headers: `timestamp,datetime,open,high,low,close,volume,ema_7,vwma_17`
- ✅ Look for pandas/CSV read errors in logs

### **Log Analysis**

```bash
# Monitor in real-time
tail -f market_data.log

# Filter for errors
grep "❌" market_data.log

# Check timing and aggregation
grep -E "(⏰|✅ Aggregated)" market_data.log

# View incremental updates
grep "Incremental cycle" market_data.log

# Check token refresh events
grep "🔄.*token" market_data.log
```

## ⚡ Performance Metrics

- **API Calls**: 1 per minute during market hours (390 calls/day)
- **Memory Usage**: ~50-100MB (depends on historical data size)
- **CPU Usage**: Minimal, brief spikes during indicator calculations
- **Network**: ~10KB per API response
- **Storage**: ~1MB per symbol per month (1m data)
- **Latency**: Data available 5-10 seconds after minute completion

## 🔮 Future Enhancements

- **Multi-Symbol Support**: Collect data for multiple symbols simultaneously
- **WebSocket Integration**: Real-time tick data streaming
- **Additional Indicators**: RSI, MACD, Bollinger Bands, Stochastic
- **Database Storage**: PostgreSQL, InfluxDB, TimescaleDB options
- **REST API**: Serve collected data via HTTP endpoints
- **Alert System**: Price breakout and pattern recognition alerts
- **Backtesting Framework**: Historical strategy testing capabilities
- **Cloud Deployment**: AWS/GCP/Azure deployment templates

## 📄 License

MIT License - See LICENSE file for details

## 🆘 Support

1. **Check System Status**: Run `python status_check.py` for health check
2. **Review Logs**: Use grep commands above to filter log output
3. **Verify Setup**: Ensure credentials, tokens, and market hours are correct
4. **Test Single Mode**: Run `python market_data_updater.py --single` to test workflow
5. **Check Schwab API**: Visit Schwab developer documentation for API status

---

**⚠️ Disclaimer**: This tool is for educational and informational purposes only. Not financial advice. Always verify data accuracy before making trading decisions.
