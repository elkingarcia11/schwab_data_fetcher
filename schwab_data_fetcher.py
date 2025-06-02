import requests
import json
from datetime import datetime
import base64

# Schwab Market Data API URL
MARKET_DATA_URL = 'https://api.schwabapi.com/marketdata/v1'
TOKEN_URL = 'https://api.schwabapi.com/v1/oauth/token'

def load_refresh_token():
    """Load refresh token from file (provided by external authentication program)"""
    try:
        with open('schwab_refresh_token.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        print("Error: schwab_refresh_token.txt file not found")
        return None
    except Exception as e:
        print(f"Error reading refresh token: {e}")
        return None

def get_access_token_from_refresh(refresh_token, client_id, client_secret):
    """Exchange refresh token for access token"""
    # Encode client credentials
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {encoded_credentials}'
    }
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get('access_token')
    else:
        print(f"Error getting access token: {response.status_code} - {response.text}")
        return None

def get_valid_access_token(client_id=None, client_secret=None):
    """Get access token using refresh token from file"""
    refresh_token = load_refresh_token()
    if not refresh_token:
        return None
    
    print("Refresh token loaded successfully from file")
    print(f"Refresh token: {refresh_token[:20]}...")  # Show first 20 chars for verification
    
    # If client credentials are provided, exchange refresh token for access token
    if client_id and client_secret:
        print("Exchanging refresh token for access token...")
        access_token = get_access_token_from_refresh(refresh_token, client_id, client_secret)
        if access_token:
            print("Access token obtained successfully")
            return access_token
        else:
            print("Failed to exchange refresh token for access token")
            return None
    else:
        print("Warning: No client credentials provided. Using refresh token directly (may not work for API calls)")
        print("You need to provide client_id and client_secret to exchange for access token")
        return refresh_token  # Return refresh token as fallback

def get_quote(symbol, access_token):
    """Get current quote for a symbol"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    url = f"{MARKET_DATA_URL}/quotes"
    params = {'symbols': symbol}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error getting quote: {response.status_code} - {response.text}")
        return None

def get_price_history(symbol, access_token, period_type='day', period=1, frequency_type='minute', frequency=1):
    """Get price history for a symbol"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    url = f"{MARKET_DATA_URL}/pricehistory"
    params = {
        'symbol': symbol,
        'periodType': period_type,
        'period': period,
        'frequencyType': frequency_type,
        'frequency': frequency
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error getting price history: {response.status_code} - {response.text}")
        return None

def get_multiple_quotes(symbols, access_token):
    """Get quotes for multiple symbols at once"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    url = f"{MARKET_DATA_URL}/quotes"
    params = {'symbols': ','.join(symbols)}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error getting quotes: {response.status_code} - {response.text}")
        return None

def extract_ohlc_from_quote(quote_data, symbol):
    """Extract OHLC from quote data"""
    if quote_data and symbol in quote_data:
        data = quote_data[symbol]
        return {
            'symbol': symbol,
            'open': data.get('openPrice') or data.get('regularMarketLastPrice'),
            'high': data.get('highPrice') or data.get('regularMarketDayHighPrice'),
            'low': data.get('lowPrice') or data.get('regularMarketDayLowPrice'),
            'close': data.get('lastPrice') or data.get('regularMarketLastPrice'),
            'volume': data.get('totalVolume') or data.get('regularMarketVolume'),
            'bid': data.get('bidPrice'),
            'ask': data.get('askPrice'),
            'timestamp': datetime.now().isoformat()
        }
    return None

def extract_ohlc_from_history(history_data):
    """Extract OHLC from price history data"""
    if history_data and 'candles' in history_data:
        candles = history_data['candles']
        if candles:
            latest = candles[-1]
            return {
                'symbol': history_data.get('symbol'),
                'open': latest.get('open'),
                'high': latest.get('high'),
                'low': latest.get('low'),
                'close': latest.get('close'),
                'volume': latest.get('volume'),
                'timestamp': datetime.fromtimestamp(latest.get('datetime', 0) / 1000).isoformat()
            }
    return None

def format_ohlc_display(ohlc):
    """Format OHLC data for display"""
    if not ohlc:
        return "No data available"
    
    output = f"\n{ohlc['symbol']} Summary:\n"
    output += f"Open: ${ohlc['open'] if ohlc['open'] is not None else 'N/A'}\n"
    output += f"High: ${ohlc['high'] if ohlc['high'] is not None else 'N/A'}\n"
    output += f"Low: ${ohlc['low'] if ohlc['low'] is not None else 'N/A'}\n"
    output += f"Close: ${ohlc['close'] if ohlc['close'] is not None else 'N/A'}\n"
    output += f"Volume: {ohlc['volume']:,}" if ohlc['volume'] is not None else "Volume: N/A"
    
    if ohlc.get('bid') is not None:
        output += f"\nBid: ${ohlc['bid']}"
    if ohlc.get('ask') is not None:
        output += f"\nAsk: ${ohlc['ask']}"
    
    return output

def get_symbol_ohlc(symbol, use_history_fallback=True):
    """Get OHLC data for any symbol with automatic token management"""
    
    # Get valid access token (handles Monday refresh automatically)
    access_token = get_valid_access_token()
    
    if not access_token:
        print("Failed to get access token")
        return None
    
    # Try quote data first
    quote_data = get_quote(symbol, access_token)
    
    if quote_data:
        ohlc = extract_ohlc_from_quote(quote_data, symbol)
        if ohlc and ohlc['close'] is not None:  # Check if we got valid data
            print(f"{symbol} OHLC from Quote:")
            print(json.dumps(ohlc, indent=2))
            print(format_ohlc_display(ohlc))
            return ohlc
    
    # Fallback to price history if quote fails or returns null values
    if use_history_fallback:
        print(f"Quote data unavailable for {symbol}, trying price history...")
        history_data = get_price_history(symbol, access_token, period_type='day', period=5)
        
        if history_data:
            ohlc = extract_ohlc_from_history(history_data)
            if ohlc:
                print(f"{symbol} OHLC from Price History:")
                print(json.dumps(ohlc, indent=2))
                print(format_ohlc_display(ohlc))
                return ohlc
    
    print(f"No {symbol} data available")
    return None

def get_spy_ohlc():
    """Convenience function to get SPY OHLC data"""
    return get_symbol_ohlc('SPY')

def get_multiple_symbols_ohlc(symbols):
    """Get OHLC data for multiple symbols"""
    
    # Get valid access token
    access_token = get_valid_access_token()
    
    if not access_token:
        print("Failed to get access token")
        return None
    
    # Get quotes for all symbols at once
    quote_data = get_multiple_quotes(symbols, access_token)
    
    if not quote_data:
        print("Failed to get quote data")
        return None
    
    results = {}
    for symbol in symbols:
        ohlc = extract_ohlc_from_quote(quote_data, symbol)
        if ohlc:
            results[symbol] = ohlc
            print(format_ohlc_display(ohlc))
        else:
            print(f"No data available for {symbol}")
    
    return results

# MAIN EXECUTION EXAMPLES
if __name__ == "__main__":
    print("Choose data to fetch:")
    print("1. SPY OHLC")
    print("2. Custom symbol OHLC") 
    print("3. Multiple symbols OHLC")
    
    choice = input("Enter 1, 2, or 3: ").strip()
    
    if choice == "1":
        spy_data = get_spy_ohlc()
        
    elif choice == "2":
        symbol = input("Enter symbol (e.g., AAPL, TSLA): ").strip().upper()
        symbol_data = get_symbol_ohlc(symbol)
        
    elif choice == "3":
        symbols_input = input("Enter symbols separated by commas (e.g., SPY,QQQ,AAPL): ").strip()
        symbols = [s.strip().upper() for s in symbols_input.split(',')]
        multiple_data = get_multiple_symbols_ohlc(symbols)
        
    else:
        print("Invalid choice")
        
    print("\nData fetching complete!")