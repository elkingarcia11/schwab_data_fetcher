# Schwab Market Data API Client

A Python client for fetching real-time and historical market data from the Charles Schwab Market Data API with automatic token management and intelligent caching.

## Features

- ✅ **Automatic Token Management** - Handles OAuth refresh tokens and access token caching
- ✅ **Real-time Market Data** - Get live quotes with bid/ask spreads during market hours
- ✅ **Historical Price Data** - Fetch OHLC data with configurable time periods
- ✅ **Smart Fallback** - Automatically falls back to price history when quotes unavailable
- ✅ **Multiple Symbol Support** - Fetch data for multiple stocks in a single request
- ✅ **Token Caching** - Saves and reuses access tokens until expiration
- ✅ **Error Handling** - Robust error handling and retry logic

## Prerequisites

- Python 3.7+
- Charles Schwab Developer Account
- Schwab API credentials (App Key and App Secret)
- Valid refresh token (obtained through Schwab OAuth flow)

## Installation

1. **Clone or download the project files**
2. **Create a virtual environment** (recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Setup

### 1. Environment Variables

Create a `.env` file in the project root with your Schwab API credentials:

```env
SCHWAB_APP_KEY=your_schwab_app_key_here
SCHWAB_APP_SECRET=your_schwab_app_secret_here
```

### 2. Refresh Token

Place your refresh token in a file named `schwab_refresh_token.txt` in the project root.

**Note**: The refresh token should be obtained through Schwab's OAuth authentication flow using another program or script.

## Usage

### Command Line Interface

Run the interactive script:

```bash
python schwab_data_fetcher.py
```

Choose from three options:

1. **SPY OHLC** - Get SPY market data
2. **Custom Symbol OHLC** - Enter any stock symbol
3. **Multiple Symbols OHLC** - Enter comma-separated symbols

### Programmatic Usage

```python
from schwab_data_fetcher import SchwabDataFetcher

# Create client instance
fetcher = SchwabDataFetcher()

# Get single symbol data
spy_data = fetcher.get_symbol_ohlc('SPY')

# Get multiple symbols
symbols_data = fetcher.get_multiple_symbols_ohlc(['AAPL', 'MSFT', 'GOOGL'])

# Get SPY data (convenience method)
spy_data = fetcher.get_spy_ohlc()
```

### Sample Output

```json
{
  "symbol": "SPY",
  "open": 587.76,
  "high": 589.94,
  "low": 585.06,
  "close": 589.31,
  "volume": 25745366,
  "bid": 589.31,
  "ask": 589.32,
  "timestamp": "2025-06-02T11:46:26.518878"
}
```

## API Methods

### Core Methods

- `get_symbol_ohlc(symbol, use_history_fallback=True)` - Get OHLC data for any symbol
- `get_spy_ohlc()` - Convenience method for SPY data
- `get_multiple_symbols_ohlc(symbols)` - Get data for multiple symbols

### Raw API Methods

- `get_quote(symbol, access_token)` - Get real-time quote data
- `get_price_history(symbol, access_token, ...)` - Get historical price data
- `get_multiple_quotes(symbols, access_token)` - Get multiple quotes

### Token Management

- `get_valid_access_token()` - Get cached or refresh access token
- `load_cached_access_token()` - Load token from cache file
- `save_access_token(token)` - Save token to cache file

## File Structure

```
schwab_market_api/
├── schwab_data_fetcher.py      # Main client class
├── requirements.txt            # Python dependencies
├── .env                        # API credentials (create this)
├── schwab_refresh_token.txt    # Refresh token (create this)
├── schwab_access_token.txt     # Auto-generated token cache
├── README.md                   # This file
└── venv/                       # Virtual environment (optional)
```

## Token Management Flow

1. **First Run**: Loads refresh token → exchanges for access token → saves to cache
2. **Subsequent Runs**: Uses cached access token if valid
3. **Token Expiry**: Automatically refreshes using refresh token when needed
4. **API Test**: Validates token with test API call before use

## Error Handling

The client handles various error scenarios:

- Missing or expired tokens
- API rate limits
- Network connectivity issues
- Invalid symbols
- Market closure (falls back to historical data)

## Market Hours

- **Real-time quotes**: Available during market hours (9:30 AM - 4:00 PM ET)
- **Historical data**: Always available as fallback
- **After hours**: Automatically uses price history for recent data

## Dependencies

- `requests` - HTTP client for API calls
- `python-dotenv` - Environment variable management

## Troubleshooting

### "Quote data unavailable"

- **During market hours**: Check token validity and API credentials
- **After hours**: Normal behavior - system falls back to historical data

### "Access token failed"

- Verify `.env` file has correct API credentials
- Check if refresh token is valid and not expired
- Ensure refresh token file exists and is readable

### "No module named 'requests'"

- Make sure you've activated your virtual environment
- Run `pip install -r requirements.txt`

## Security Notes

- Keep your `.env` file secure and never commit it to version control
- The refresh token file contains sensitive data - protect it appropriately
- Access tokens are cached locally - ensure proper file permissions

## API Rate Limits

Schwab API has rate limits. The client includes:

- Automatic token caching to reduce authentication calls
- Intelligent fallbacks to minimize API usage
- Error handling for rate limit responses

## Contributing

1. Follow the existing code style and structure
2. Add error handling for new features
3. Update this README for any new functionality
4. Test with both real-time and historical data scenarios

## License

This project is for educational and personal use. Ensure compliance with Schwab's API terms of service.

## Disclaimer

This software is not affiliated with Charles Schwab. Use at your own risk. Always verify market data from official sources before making trading decisions.
