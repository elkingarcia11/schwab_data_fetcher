# Schwab Real-Time Market Data Streamer

A Python client for streaming real-time 1-minute OHLC data from the Charles Schwab Market Data API with local aggregation to 5-minute and 15-minute timeframes. Features automatic token management, current day data fetching, and intelligent CSV persistence.

## 🚀 Key Features

- ✅ **Real-Time Current Day Data** - Uses targeted API calls with `startDate`/`endDate` to get current day minute bars
- ✅ **Smart Timing** - Fetches data 5-15 seconds after minute boundaries to ensure complete bars
- ✅ **Automatic Backfill** - Detects CSV gaps and fills missing data from last entry to current time
- ✅ **Local Aggregation** - Converts 1-minute data to 5-minute and 15-minute bars locally
- ✅ **Perfect Alignment** - 15-minute bars align with market standards (:00, :15, :30, :45)
- ✅ **CSV Persistence** - Saves to `symbol_1m.csv`, `symbol_5m.csv`, `symbol_15m.csv`
- ✅ **Efficient API Usage** - Only 1 API call per minute using targeted fetching
- ✅ **Automatic Token Management** - Handles OAuth refresh and access token caching
- ✅ **Gap Recovery** - Automatically handles interruptions and restarts

## 🎯 Strategy

The system uses a revolutionary approach that solves Schwab API's current day data limitations:

1. **Targeted Fetching**: Uses `startDate`/`endDate` parameters to get current day data
2. **CSV-Driven**: Checks last CSV timestamp and fetches only missing data
3. **Local Aggregation**: Creates 5min/15min bars from 1min data for perfect alignment
4. **Smart Timing**: Waits for minute completion before fetching

## 📊 Data Output

```
SPY_1m.csv   - Individual 1-minute OHLC bars (e.g., 9:30, 9:31, 9:32...)
SPY_5m.csv   - 5-minute aggregated bars (e.g., 9:30, 9:35, 9:40...)
SPY_15m.csv  - 15-minute aggregated bars (e.g., 9:30, 9:45, 10:00...)
```

## 📋 Prerequisites

- Python 3.7+
- Charles Schwab Developer Account
- Schwab API credentials (App Key and App Secret)
- Valid refresh token (obtained through Schwab OAuth flow)

## 🛠️ Installation

1. **Clone or download the project**
2. **Create virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## ⚙️ Setup

### 1. Environment Variables

Create a `.env` file in the project root:

```env
SCHWAB_APP_KEY=your_schwab_app_key_here
SCHWAB_APP_SECRET=your_schwab_app_secret_here
```

### 2. Refresh Token

Place your refresh token in `schwab_refresh_token.txt` in the project root.

**Note**: Obtain the refresh token through Schwab's OAuth authentication flow.

## 🚀 Usage

### Start Real-Time Streaming

```bash
python schwab_data_fetcher.py
```

Enter a symbol (e.g., SPY, AAPL, TSLA) and the system will:

1. **Backfill** any missing data from 9:30 AM today to current time
2. **Stream** new 1-minute data every minute
3. **Aggregate** locally to 5-minute and 15-minute bars
4. **Save** to CSV files continuously

### Example Session

```
🚀 Schwab Real-Time OHLC Data Streamer
==================================================
📊 Features:
   • Fetches 1-minute bars from Schwab API
   • Aggregates locally to 5min & 15min bars
   • Saves to CSV: symbol_1m.csv, symbol_5m.csv, symbol_15m.csv
   • Perfect timeframe alignment (:00, :15, :30, :45 for 15min)
   • Uses current day session data (not delayed)
   • Only 1 API call per minute (efficient)

Enter symbol to stream (e.g., SPY, AAPL, TSLA): SPY

🎯 Starting real-time streaming for SPY
📈 Strategy: 1min API data → Local aggregation to 5min & 15min
💾 Output files:
   📄 SPY_1m.csv  (1-minute bars)
   📄 SPY_5m.csv  (5-minute bars)
   📄 SPY_15m.csv (15-minute bars)

🔄 Program will run continuously...
🛑 Press Ctrl+C to stop
```

## 🔄 How It Works

### Timing System

```
14:30:00-14:30:59  → 1-minute bar forming
14:31:05           → FETCH completed 14:30 bar
14:31:00-14:31:59  → Next 1-minute bar forming
14:32:05           → FETCH completed 14:31 bar
```

### API Strategy

```
Last CSV: 14:25:00
Current:  14:32:05
Action:   Fetch 14:26, 14:27, 14:28, 14:29, 14:30, 14:31 bars
```

### Aggregation Logic

```
1-min bars: 14:30, 14:31, 14:32, 14:33, 14:34
5-min bar:  14:30 (aggregates all 5 bars)

1-min bars: 14:30, 14:31, ..., 14:44
15-min bar: 14:30 (aggregates all 15 bars)
```

## 📁 File Structure

```
schwab_market_api/
├── schwab_data_fetcher.py      # Main application (all functionality)
├── requirements.txt            # Python dependencies
├── .env                        # API credentials (create this)
├── schwab_refresh_token.txt    # Refresh token (create this)
├── schwab_access_token.txt     # Auto-generated token cache
├── README.md                   # This documentation
├── {SYMBOL}_1m.csv            # Generated: 1-minute bars
├── {SYMBOL}_5m.csv            # Generated: 5-minute bars
└── {SYMBOL}_15m.csv           # Generated: 15-minute bars
```

## 🔧 Key Technical Features

### Current Day Data Solution

- **Problem**: Schwab price history API has 1-day delay for current day minute data
- **Solution**: Use `startDate`/`endDate` parameters with targeted time ranges
- **Result**: Get real-time current day minute bars instead of delayed data

### Smart Gap Detection

- **Problem**: Interruptions cause missing data
- **Solution**: Check last CSV timestamp and fetch only missing data
- **Result**: Seamless recovery from any interruption

### Perfect Timeframe Alignment

- **Problem**: API bars may not align with standard intervals
- **Solution**: Local aggregation from 1-minute source data
- **Result**: 15-minute bars perfectly aligned at :00, :15, :30, :45

### Efficient API Usage

- **Before**: 3 API calls per minute (1min, 5min, 15min)
- **After**: 1 API call per minute (1min only, aggregate locally)
- **Benefit**: 66% reduction in API usage

## 📊 CSV Format

All CSV files have the same structure:

```csv
timestamp,datetime,open,high,low,close,volume
1748973180000,2025-06-03 09:30:00,592.34,593.45,592.10,593.12,156789
1748973240000,2025-06-03 09:31:00,593.12,593.67,592.98,593.45,142456
```

## 🛠️ Programmatic Usage

```python
from schwab_data_fetcher import SchwabDataFetcher

# Create instance
fetcher = SchwabDataFetcher()

# Manual backfill (fills gaps from last CSV entry to now)
fetcher.backfill_missing_1min_data('SPY')

# Start streaming (runs continuously)
fetcher.stream_1min_with_aggregation('SPY')

# Get targeted minute data
candles = fetcher.get_minute_data_from_csv_to_now('SPY')
```

## ⚡ Performance & Efficiency

- **API Calls**: 1 per minute (vs 3 in previous approaches)
- **Data Accuracy**: Current day real-time data (not delayed)
- **Recovery**: Automatic gap filling on restart
- **Alignment**: Perfect 15-minute boundaries
- **Storage**: Persistent CSV files for historical analysis

## 🔍 Troubleshooting

### No Current Day Data

- **Cause**: API delay for current day data
- **Solution**: System automatically uses targeted `startDate`/`endDate` approach

### Missing Data After Restart

- **Cause**: Program interruption
- **Solution**: System automatically detects gaps and backfills missing data

### Incorrect 15-minute Alignment

- **Cause**: Using direct API 15-minute data
- **Solution**: System uses local aggregation for perfect alignment

### Token Errors

- Verify `.env` file has correct `SCHWAB_APP_KEY` and `SCHWAB_APP_SECRET`
- Check `schwab_refresh_token.txt` contains valid refresh token
- Token files are auto-generated and managed

## 🔒 Security Notes

- Keep `.env` file secure and never commit to version control
- Protect `schwab_refresh_token.txt` - contains sensitive authentication data
- Access tokens are cached locally in `schwab_access_token.txt`

## 📈 Market Coverage

- **Regular Hours**: 9:30 AM - 4:00 PM ET
- **Pre-Market**: 7:00 AM - 9:30 AM ET (when available)
- **After Hours**: Limited data availability
- **Weekends**: No new data (system handles gracefully)

## 🤝 Contributing

1. Follow existing code structure in `schwab_data_fetcher.py`
2. Test with both market hours and after-hours scenarios
3. Ensure CSV compatibility and proper timestamp handling
4. Update README for any new functionality

## 📝 Dependencies

```txt
requests==2.31.0
python-dotenv==1.0.0
```

## 🎉 Success Metrics

✅ **Real-time current day data** - Solved API delay limitations  
✅ **Perfect 15-minute alignment** - Standard market intervals  
✅ **Efficient API usage** - 66% reduction in calls  
✅ **Automatic recovery** - Handles interruptions seamlessly  
✅ **Production ready** - Clean, robust, and well-documented
