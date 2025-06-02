import base64
import json
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv


class SchwabDataFetcher:
    """Schwab Market Data API client with automatic token management"""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Schwab Market Data API URLs
        self.market_data_url = 'https://api.schwabapi.com/marketdata/v1'
        self.token_url = 'https://api.schwabapi.com/v1/oauth/token'
        self.access_token_file = 'schwab_access_token.txt'
        self.refresh_token_file = 'schwab_refresh_token.txt'

    def load_refresh_token(self):
        """Load refresh token from file (provided by external authentication program)"""
        try:
            with open(self.refresh_token_file, 'r') as file:
                return file.read().strip()
        except FileNotFoundError:
            print(f"Error: {self.refresh_token_file} file not found")
            return None
        except Exception as e:
            print(f"Error reading refresh token: {e}")
            return None

    def save_access_token(self, access_token):
        """Save access token to file with timestamp"""
        try:
            token_data = {
                'access_token': access_token,
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(minutes=30)).isoformat()  # Schwab tokens typically expire in 30 minutes
            }
            with open(self.access_token_file, 'w') as file:
                json.dump(token_data, file)
            print(f"Access token saved to {self.access_token_file}")
        except Exception as e:
            print(f"Error saving access token: {e}")

    def load_cached_access_token(self):
        """Load access token from file if it exists and is still valid"""
        try:
            with open(self.access_token_file, 'r') as file:
                token_data = json.load(file)
            
            # Check if token is still valid (with 2 minute buffer)
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            if datetime.now() < expires_at - timedelta(minutes=2):
                print("Using cached access token")
                return token_data['access_token']
            else:
                print("Cached access token has expired")
                return None
                
        except FileNotFoundError:
            print("No cached access token found")
            return None
        except Exception as e:
            print(f"Error loading cached access token: {e}")
            return None

    def get_access_token_from_refresh(self, refresh_token, client_id, client_secret):
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
        
        response = requests.post(self.token_url, headers=headers, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            return token_data.get('access_token')
        else:
            print(f"Error getting access token: {response.status_code} - {response.text}")
            return None

    def test_access_token(self, access_token):
        """Test if access token is working by making a simple API call"""
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        # Test with a simple quote request
        url = f"{self.market_data_url}/quotes"
        params = {'symbols': 'SPY'}
        
        response = requests.get(url, headers=headers, params=params)
        return response.status_code == 200

    def get_valid_access_token(self):
        """Get access token using cached token or refresh if needed"""
        
        # First try to use cached access token
        cached_token = self.load_cached_access_token()
        if cached_token:
            # Test if the cached token still works
            if self.test_access_token(cached_token):
                print("Cached access token is valid")
                return cached_token
            else:
                print("Cached access token failed API test, refreshing...")
        
        # Need to get new access token using refresh token
        refresh_token = self.load_refresh_token()
        if not refresh_token:
            return None
        
        print("Getting new access token using refresh token")
        print(f"Refresh token: {refresh_token[:20]}...")
        
        # Load client credentials from environment variables
        client_id = os.getenv('SCHWAB_APP_KEY')
        client_secret = os.getenv('SCHWAB_APP_SECRET')
        
        if not client_id or not client_secret:
            print("Error: SCHWAB_APP_KEY and/or SCHWAB_APP_SECRET not found in .env file")
            print("Please ensure your .env file contains:")
            print("SCHWAB_APP_KEY=your_app_key")
            print("SCHWAB_APP_SECRET=your_app_secret")
            return None
        
        print("Client credentials loaded from .env file")
        print("Exchanging refresh token for access token...")
        
        access_token = self.get_access_token_from_refresh(refresh_token, client_id, client_secret)
        if access_token:
            print("New access token obtained successfully")
            self.save_access_token(access_token)  # Save for future use
            return access_token
        else:
            print("Failed to exchange refresh token for access token")
            return None

    def get_quote(self, symbol, access_token):
        """Get current quote for a symbol"""
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        url = f"{self.market_data_url}/quotes"
        params = {'symbols': symbol}
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting quote: {response.status_code} - {response.text}")
            return None

    def get_price_history(self, symbol, access_token, period_type='day', period=1, frequency_type='minute', frequency=1):
        """Get price history for a symbol"""
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        url = f"{self.market_data_url}/pricehistory"
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

    def get_multiple_quotes(self, symbols, access_token):
        """Get quotes for multiple symbols at once"""
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        url = f"{self.market_data_url}/quotes"
        params = {'symbols': ','.join(symbols)}
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting quotes: {response.status_code} - {response.text}")
            return None

    def extract_ohlc_from_quote(self, quote_data, symbol):
        """Extract OHLC from quote data"""
        if quote_data and symbol in quote_data:
            data = quote_data[symbol]
            
            # The actual structure has quote data nested under 'quote' key
            quote_info = data.get('quote', {})
            
            return {
                'symbol': symbol,
                'open': quote_info.get('openPrice'),
                'high': quote_info.get('highPrice'), 
                'low': quote_info.get('lowPrice'),
                'close': quote_info.get('lastPrice'),
                'volume': quote_info.get('totalVolume'),
                'bid': quote_info.get('bidPrice'),
                'ask': quote_info.get('askPrice'),
                'timestamp': datetime.now().isoformat()
            }
        return None

    def extract_ohlc_from_history(self, history_data):
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

    def format_ohlc_display(self, ohlc):
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

    def get_symbol_ohlc(self, symbol, use_history_fallback=True):
        """Get OHLC data for any symbol with automatic token management"""
        
        access_token = self.get_valid_access_token()
        
        if not access_token:
            print("Failed to get access token")
            return None
        
        # Try quote data first
        quote_data = self.get_quote(symbol, access_token)
        
        if quote_data:
            ohlc = self.extract_ohlc_from_quote(quote_data, symbol)
            if ohlc and ohlc['close'] is not None:  # Check if we got valid data
                print(f"{symbol} OHLC from Quote:")
                print(json.dumps(ohlc, indent=2))
                print(self.format_ohlc_display(ohlc))
                return ohlc
        
        # Fallback to price history if quote fails or returns null values
        if use_history_fallback:
            print(f"Quote data unavailable for {symbol}, trying price history...")
            history_data = self.get_price_history(symbol, access_token, period_type='day', period=5)
            
            if history_data:
                ohlc = self.extract_ohlc_from_history(history_data)
                if ohlc:
                    print(f"{symbol} OHLC from Price History:")
                    print(json.dumps(ohlc, indent=2))
                    print(self.format_ohlc_display(ohlc))
                    return ohlc
        
        print(f"No {symbol} data available")
        return None

    def get_spy_ohlc(self):
        """Convenience function to get SPY OHLC data"""
        return self.get_symbol_ohlc('SPY')

    def get_multiple_symbols_ohlc(self, symbols):
        """Get OHLC data for multiple symbols"""
        
        # Get valid access token
        access_token = self.get_valid_access_token()
        
        if not access_token:
            print("Failed to get access token")
            return None
        
        # Get quotes for all symbols at once
        quote_data = self.get_multiple_quotes(symbols, access_token)
        
        if not quote_data:
            print("Failed to get quote data")
            return None
        
        results = {}
        for symbol in symbols:
            ohlc = self.extract_ohlc_from_quote(quote_data, symbol)
            if ohlc:
                results[symbol] = ohlc
                print(self.format_ohlc_display(ohlc))
            else:
                print(f"No data available for {symbol}")
        
        return results


# MAIN EXECUTION EXAMPLES
if __name__ == "__main__":
    # Create an instance of the SchwabDataFetcher
    fetcher = SchwabDataFetcher()
    
    print("Choose data to fetch:")
    print("1. SPY OHLC")
    print("2. Custom symbol OHLC") 
    print("3. Multiple symbols OHLC")
    
    choice = input("Enter 1, 2, or 3: ").strip()
    
    if choice == "1":
        spy_data = fetcher.get_spy_ohlc()
        
    elif choice == "2":
        symbol = input("Enter symbol (e.g., AAPL, TSLA): ").strip().upper()
        symbol_data = fetcher.get_symbol_ohlc(symbol)
        
    elif choice == "3":
        symbols_input = input("Enter symbols separated by commas (e.g., SPY,QQQ,AAPL): ").strip()
        symbols = [s.strip().upper() for s in symbols_input.split(',')]
        multiple_data = fetcher.get_multiple_symbols_ohlc(symbols)
        
    else:
        print("Invalid choice")
        
    print("\nData fetching complete!")