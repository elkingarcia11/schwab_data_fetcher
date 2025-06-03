import base64
import csv
import json
import os
import requests
import time
import threading
from collections import defaultdict, deque
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
        
        # Streaming data storage - track last seen bars to detect new ones
        self.last_seen_bars = defaultdict(lambda: {
            '1min': None,
            '5min': None, 
            '15min': None
        })
        self.is_streaming = False
        self.streaming_thread = None
        self.csv_files = {}  # Track open CSV files

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
        """Get current quote for a symbol using symbol-specific endpoint"""
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        # Use symbol-specific endpoint which works better
        url = f"{self.market_data_url}/{symbol}/quotes"
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting quote: {response.status_code} - {response.text}")
            return None

    def get_price_history(self, symbol, access_token, period_type='day', period=1, frequency_type='minute', frequency=1, start_datetime=None, end_datetime=None):
        """Get price history for a symbol with optional date range targeting"""
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
        
        # Add targeted date range if provided (this is the key for current day data!)
        if start_datetime and end_datetime:
            start_timestamp_ms = int(start_datetime.timestamp() * 1000)
            end_timestamp_ms = int(end_datetime.timestamp() * 1000)
            params.update({
                'startDate': start_timestamp_ms,
                'endDate': end_timestamp_ms,
                'needExtendedHoursData': True,
                'needPreviousClose': False
            })
            print(f"🎯 Targeted price history: {start_datetime.strftime('%Y-%m-%d %H:%M')} to {end_datetime.strftime('%Y-%m-%d %H:%M')}")
            print(f"🔍 DEBUG: API params with dates:")
            print(f"   startDate: {start_timestamp_ms} ({start_datetime})")
            print(f"   endDate: {end_timestamp_ms} ({end_datetime})")
        else:
            print(f"🔍 DEBUG: API params WITHOUT dates (using period: {period} {period_type})")
        
        print(f"🔍 DEBUG: Full API params: {params}")
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            result = response.json()
            print(f"🔍 DEBUG: API response success, candles count: {len(result.get('candles', []))}")
            return result
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

    def get_timeframe_params(self, timeframe):
        """Get API parameters for different timeframes"""
        if timeframe == '1min':
            return {'period_type': 'day', 'period': 1, 'frequency_type': 'minute', 'frequency': 1}
        elif timeframe == '5min':
            return {'period_type': 'day', 'period': 1, 'frequency_type': 'minute', 'frequency': 5}
        elif timeframe == '15min':
            return {'period_type': 'day', 'period': 1, 'frequency_type': 'minute', 'frequency': 15}
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

    def get_hybrid_latest_bars(self, symbol, timeframe, access_token):
        """Hybrid approach: Use price history for historical data, quote for current live data"""
        try:
            # First, try to get recent price history
            params = self.get_timeframe_params(timeframe)
            if params['period_type'] == 'day' and params['period'] == 1:
                params['period'] = 2  # Get last 2 days for better coverage
            
            history_data = self.get_price_history(symbol, access_token, **params)
            historical_bars = []
            
            if history_data and 'candles' in history_data:
                historical_bars = history_data['candles']
            
            # Check if historical data includes TODAY's completed bars
            now = datetime.now()
            today_date = now.strftime('%Y-%m-%d')
            has_current_day_history = False
            
            if historical_bars:
                latest_historical = historical_bars[-1]
                latest_time = datetime.fromtimestamp(latest_historical['datetime'] / 1000)
                latest_date = latest_time.strftime('%Y-%m-%d')
                
                print(f"📈 Price history: Latest bar {latest_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Check if we have today's bars in history
                if latest_date == today_date:
                    has_current_day_history = True
                    print(f"   ✅ Price history includes TODAY's data")
                else:
                    print(f"   ❌ Price history is from {latest_date}, missing today ({today_date})")
            else:
                print(f"📈 No price history data available")
            
            # Always get current session data from quote for live updates
            print(f"🔄 Getting current session data from quote...")
            quote_bars = self.construct_current_bar_from_quote(symbol, timeframe, access_token)
            
            if quote_bars and has_current_day_history:
                # We have both historical and current data
                print(f"✅ Using historical bars + current session data")
                all_bars = historical_bars + quote_bars
                return all_bars[-10:]  # Return last 10 bars
            
            elif quote_bars and not has_current_day_history:
                # Only current session data (price history is delayed)
                print(f"✅ Using current session data only (price history delayed)")
                # Include some historical context if available
                if historical_bars:
                    all_bars = historical_bars[-5:] + quote_bars  # Last 5 historical + current
                    return all_bars
                else:
                    return quote_bars
            
            elif historical_bars and not quote_bars:
                # Only historical data available
                print(f"⚠️  Using historical data only (no current session data)")
                return historical_bars[-10:]
            
            else:
                # No data available
                print(f"❌ No data available from either source")
                return []
            
        except Exception as e:
            print(f"❌ Error in hybrid bar fetching: {e}")
            return []

    def construct_current_bar_from_quote(self, symbol, timeframe, access_token):
        """Construct current live bar from quote data using TODAY's session data"""
        try:
            quote_data = self.get_quote(symbol, access_token)
            if not quote_data or symbol not in quote_data:
                print(f"❌ No quote data available for {symbol}")
                return []
            
            quote_info = quote_data[symbol].get('quote', {})
            
            # Extract TODAY's session OHLC data (this is current day data!)
            session_open = quote_info.get('openPrice')
            session_high = quote_info.get('highPrice') 
            session_low = quote_info.get('lowPrice')
            current_price = quote_info.get('lastPrice')
            session_volume = quote_info.get('totalVolume', 0)
            
            # Get timestamps
            quote_time = quote_info.get('quoteTime', 0)
            trade_time = quote_info.get('tradeTime', 0)
            
            if not all([session_open, session_high, session_low, current_price]):
                print(f"❌ Incomplete OHLC data for {symbol}")
                return []
            
            # Use the most recent timestamp (trade time preferred)
            timestamp_ms = trade_time if trade_time else quote_time
            if not timestamp_ms:
                print(f"❌ No timestamp available for {symbol}")
                return []
            
            # Convert to datetime for bar alignment
            current_time = datetime.fromtimestamp(timestamp_ms / 1000)
            
            # Align timestamp to timeframe boundary
            if timeframe == '1min':
                bar_time = current_time.replace(second=0, microsecond=0)
            elif timeframe == '5min':
                minute = (current_time.minute // 5) * 5
                bar_time = current_time.replace(minute=minute, second=0, microsecond=0)
            elif timeframe == '15min':
                minute = (current_time.minute // 15) * 15
                bar_time = current_time.replace(minute=minute, second=0, microsecond=0)
            else:
                bar_time = current_time.replace(second=0, microsecond=0)
            
            # Create current session bar using TODAY's data
            current_bar = {
                'datetime': int(bar_time.timestamp() * 1000),
                'open': session_open,      # Today's session open
                'high': session_high,      # Today's session high so far
                'low': session_low,        # Today's session low so far  
                'close': current_price,    # Current live price
                'volume': session_volume   # Today's total volume so far
            }
            
            bar_date = bar_time.strftime('%Y-%m-%d')
            today_date = datetime.now().strftime('%Y-%m-%d')
            
            print(f"📊 {symbol} {timeframe} bar from TODAY's session ({bar_date}):")
            print(f"   🕒 Time: {bar_time.strftime('%H:%M:%S')} | "
                  f"O:{session_open:.2f} H:{session_high:.2f} L:{session_low:.2f} C:{current_price:.2f} | "
                  f"Vol:{session_volume:,}")
            
            if bar_date == today_date:
                print(f"   ✅ This is CURRENT DAY data!")
            else:
                print(f"   ⚠️  Data is from {bar_date}, not today ({today_date})")
            
            return [current_bar]
            
        except Exception as e:
            print(f"❌ Error constructing current bar from quote: {e}")
            return []

    def fetch_latest_bars(self, symbol, timeframe, access_token):
        """Main method - now uses hybrid approach"""
        return self.get_hybrid_latest_bars(symbol, timeframe, access_token)

    def format_bar_display(self, symbol, timeframe, bar):
        """Format a single bar for display"""
        timestamp = datetime.fromtimestamp(bar['datetime'] / 1000)
        return (f"📊 {symbol} {timeframe.upper()} | "
                f"{timestamp.strftime('%H:%M:%S')} | "
                f"O:{bar['open']:.2f} H:{bar['high']:.2f} L:{bar['low']:.2f} C:{bar['close']:.2f} | "
                f"Vol:{bar['volume']:,}")

    def get_next_poll_time(self, timeframe):
        """Calculate when the next bar will be completed for a timeframe"""
        now = datetime.now()
        
        if timeframe == '1min':
            # Next minute boundary
            next_time = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        elif timeframe == '5min':
            # Next 5-minute boundary (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
            current_minute = now.minute
            next_5min = ((current_minute // 5) + 1) * 5
            if next_5min >= 60:
                next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_time = now.replace(minute=next_5min, second=0, microsecond=0)
        elif timeframe == '15min':
            # Next 15-minute boundary (0, 15, 30, 45) - Standard market intervals
            current_minute = now.minute
            next_15min = ((current_minute // 15) + 1) * 15
            if next_15min >= 60:
                next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_time = now.replace(minute=next_15min, second=0, microsecond=0)
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        
        return next_time

    def get_polling_schedule_preview(self, timeframes, periods=5):
        """Show the next few polling times for each timeframe"""
        now = datetime.now()
        print(f"📅 Polling Schedule Preview (Current time: {now.strftime('%H:%M:%S')}):")
        
        for timeframe in timeframes:
            print(f"\n{timeframe.upper()} bars (standard market intervals):")
            next_time = self.get_next_poll_time(timeframe)
            
            for i in range(periods):
                time_until = (next_time - now).total_seconds()
                if time_until > 0:
                    minutes, seconds = divmod(int(time_until), 60)
                    print(f"   {next_time.strftime('%H:%M:%S')} (in {minutes}m {seconds}s)")
                else:
                    print(f"   {next_time.strftime('%H:%M:%S')}")
                
                # Calculate next occurrence
                if timeframe == '1min':
                    next_time += timedelta(minutes=1)
                elif timeframe == '5min':
                    next_time += timedelta(minutes=5)
                elif timeframe == '15min':
                    next_time += timedelta(minutes=15)

    def stream_scheduled_bars(self, symbol, timeframes=['1min', '5min', '15min']):
        """Stream bars using smart scheduled polling - poll exactly when bars complete"""
        print(f"🚀 Starting scheduled bars stream for {symbol}")
        print(f"📈 Timeframes: {', '.join(timeframes)}")
        
        # Show detailed polling schedule
        self.get_polling_schedule_preview(timeframes)
        
        # Initialize CSV files for each timeframe
        for tf in timeframes:
            self.initialize_csv_file(symbol, tf)
            
            # Load last saved bar to avoid duplicates
            last_saved = self.load_last_bar_from_csv(symbol, tf)
            if last_saved:
                self.last_seen_bars[symbol][tf] = last_saved
                saved_time = datetime.fromtimestamp(last_saved['datetime'] / 1000)
                print(f"\n💾 {tf}: Last saved bar at {saved_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"\n💾 {tf}: No previous data found")
        
        print(f"\n🛑 Press Ctrl+C to stop streaming\n")
        
        self.is_streaming = True
        
        try:
            while self.is_streaming:
                try:
                    # Get access token
                    access_token = self.get_valid_access_token()
                    if not access_token:
                        print("❌ Failed to get access token, retrying in 30 seconds...")
                        time.sleep(30)
                        continue
                    
                    current_time = datetime.now()
                    
                    # Check each timeframe to see if it's time to poll
                    for timeframe in timeframes:
                        next_poll = self.get_next_poll_time(timeframe)
                        
                        # If we're within 10 seconds of the next poll time, check for new bar
                        if (next_poll - current_time).total_seconds() <= 10:
                            bars = self.fetch_latest_bars(symbol, timeframe, access_token)
                            
                            if bars:
                                latest_bar = bars[-1]
                                last_seen = self.last_seen_bars[symbol][timeframe]
                                
                                # Check if this is a new bar
                                if last_seen is None or latest_bar['datetime'] != last_seen['datetime']:
                                    bar_time = datetime.fromtimestamp(latest_bar['datetime'] / 1000)
                                    print(f"\n🆕 NEW {timeframe.upper()} BAR COMPLETED:")
                                    print(f"   📊 {symbol} | {bar_time.strftime('%H:%M:%S')} | "
                                          f"O:{latest_bar['open']:.2f} H:{latest_bar['high']:.2f} "
                                          f"L:{latest_bar['low']:.2f} C:{latest_bar['close']:.2f} | "
                                          f"Vol:{latest_bar['volume']:,}")
                                    
                                    # Show interval alignment for 15min bars
                                    if timeframe == '15min':
                                        minute = bar_time.minute
                                        if minute in [0, 15, 30, 45]:
                                            print(f"   ✅ Perfect 15-min alignment: :{minute:02d} minute mark")
                                        else:
                                            print(f"   ⚠️  15-min bar not aligned: :{minute:02d} minute mark")
                                    
                                    # Save to CSV file
                                    self.save_bar_to_csv(symbol, timeframe, latest_bar)
                                    
                                    # Update last seen bar
                                    self.last_seen_bars[symbol][timeframe] = latest_bar
                    
                    # Sleep for 10 seconds and check again
                    time.sleep(10)
                    
                except Exception as e:
                    print(f"❌ Error during scheduled streaming: {e}")
                    time.sleep(10)
        
        except KeyboardInterrupt:
            print(f"\n🛑 Stopping scheduled bars stream for {symbol}")
            self.is_streaming = False

    def stop_streaming(self):
        """Stop the streaming data"""
        self.is_streaming = False
        if self.streaming_thread:
            self.streaming_thread.join(timeout=5)

    def get_last_completed_bars(self, symbol, timeframe):
        """Get the last completed bar for a symbol and timeframe"""
        return self.last_seen_bars[symbol].get(timeframe)

    def print_streaming_summary(self, symbol):
        """Print a summary of latest bars seen during streaming"""
        if symbol not in self.last_seen_bars:
            print(f"No streaming data available for {symbol}")
            return
        
        print(f"\n📊 Latest Completed Bars for {symbol}")
        print("=" * 60)
        
        for timeframe in ['1min', '5min', '15min']:
            bar = self.last_seen_bars[symbol][timeframe]
            
            if bar:
                timestamp = datetime.fromtimestamp(bar['datetime'] / 1000)
                print(f"{timeframe.upper()}: {timestamp.strftime('%H:%M:%S')} | "
                      f"O:{bar['open']:.2f} H:{bar['high']:.2f} "
                      f"L:{bar['low']:.2f} C:{bar['close']:.2f} | Vol:{bar['volume']:,}")
            else:
                print(f"{timeframe.upper()}: No data captured")

    def get_csv_filename(self, symbol, timeframe):
        """Generate CSV filename for symbol and timeframe in data folder"""
        # Create data folder if it doesn't exist
        data_folder = 'data'
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
            print(f"📁 Created data folder: {data_folder}")
        
        tf_suffix = timeframe.replace('min', 'm')
        return os.path.join(data_folder, f"{symbol}_{tf_suffix}.csv")

    def initialize_csv_file(self, symbol, timeframe):
        """Initialize CSV file with headers if it doesn't exist"""
        filename = self.get_csv_filename(symbol, timeframe)
        
        # Check if file exists
        if not os.path.exists(filename):
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
                print(f"📁 Created new CSV file: {filename}")
        else:
            print(f"📁 Using existing CSV file: {filename}")
        
        return filename

    def save_bar_to_csv(self, symbol, timeframe, bar):
        """Save a single OHLC bar to CSV file"""
        try:
            filename = self.get_csv_filename(symbol, timeframe)
            
            # Convert timestamp from milliseconds to readable format
            timestamp = datetime.fromtimestamp(bar['datetime'] / 1000)
            
            # Prepare data row
            row = [
                bar['datetime'],  # Unix timestamp in milliseconds
                timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # Human readable datetime
                bar['open'],
                bar['high'], 
                bar['low'],
                bar['close'],
                bar['volume']
            ]
            
            # Append to CSV file
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(row)
            
            print(f"💾 Saved to {filename}: {timestamp.strftime('%H:%M:%S')} | OHLC: {bar['open']:.2f}/{bar['high']:.2f}/{bar['low']:.2f}/{bar['close']:.2f}")
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")

    def load_last_bar_from_csv(self, symbol, timeframe):
        """Load the last bar from CSV to avoid duplicates"""
        try:
            filename = self.get_csv_filename(symbol, timeframe)
            
            if not os.path.exists(filename):
                return None
            
            # Read the last line of the CSV file
            with open(filename, 'r') as csvfile:
                lines = csvfile.readlines()
                if len(lines) <= 1:  # Only header or empty
                    return None
                
                # Parse last data line
                last_line = lines[-1].strip()
                if last_line:
                    parts = last_line.split(',')
                    if len(parts) >= 7:
                        return {
                            'datetime': int(parts[0]),
                            'open': float(parts[2]),
                            'high': float(parts[3]),
                            'low': float(parts[4]),
                            'close': float(parts[5]),
                            'volume': int(parts[6])
                        }
        except Exception as e:
            print(f"⚠️  Error reading last bar from CSV: {e}")
        
        return None

    def test_15min_alignment(self):
        """Test function to verify 15-minute polling alignment"""
        print("🧪 Testing 15-minute bar alignment:")
        print("Testing various current times to show next 15-min poll times...")
        
        test_times = [
            datetime(2024, 1, 15, 9, 32, 45),   # 9:32:45 → should poll at 9:45
            datetime(2024, 1, 15, 10, 0, 15),   # 10:00:15 → should poll at 10:15  
            datetime(2024, 1, 15, 10, 14, 59),  # 10:14:59 → should poll at 10:15
            datetime(2024, 1, 15, 10, 28, 30),  # 10:28:30 → should poll at 10:30
            datetime(2024, 1, 15, 10, 47, 12),  # 10:47:12 → should poll at 11:00
            datetime(2024, 1, 15, 11, 59, 55),  # 11:59:55 → should poll at 12:00
        ]
        
        for test_time in test_times:
            # Temporarily override datetime.now for testing
            original_now = datetime.now
            datetime.now = lambda: test_time
            
            next_poll = self.get_next_poll_time('15min')
            minute = next_poll.minute
            
            # Restore original datetime.now
            datetime.now = original_now
            
            alignment_check = "✅" if minute in [0, 15, 30, 45] else "❌"
            print(f"   Current: {test_time.strftime('%H:%M:%S')} → Next poll: {next_poll.strftime('%H:%M:%S')} {alignment_check}")
        
        print("✅ All 15-minute polls should align with :00, :15, :30, :45 minute marks")

    def aggregate_1min_to_5min(self, one_min_bars):
        """Aggregate 1-minute bars into 5-minute bars at proper boundaries (0:00, 0:05, 0:10, 0:15...)"""
        if not one_min_bars:
            return []
        
        five_min_bars = []
        current_5min_bar = None
        current_5min_bars_collected = []
        
        for bar in one_min_bars:
            # Convert timestamp to datetime
            bar_time = datetime.fromtimestamp(bar['datetime'] / 1000)
            
            # Calculate which 5-minute boundary this bar belongs to (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
            minute = bar_time.minute
            five_min_boundary = (minute // 5) * 5
            boundary_time = bar_time.replace(minute=five_min_boundary, second=0, microsecond=0)
            boundary_timestamp = int(boundary_time.timestamp() * 1000)
            
            # If this is a new 5-minute period, finalize the previous one and start new
            if current_5min_bar is None or current_5min_bar['datetime'] != boundary_timestamp:
                # Finalize previous 5-minute bar if we have collected bars
                if current_5min_bar is not None and current_5min_bars_collected:
                    # Aggregate the collected 1-minute bars
                    current_5min_bar['open'] = current_5min_bars_collected[0]['open']  # First open
                    current_5min_bar['close'] = current_5min_bars_collected[-1]['close']  # Last close
                    current_5min_bar['high'] = max(b['high'] for b in current_5min_bars_collected)  # Highest high
                    current_5min_bar['low'] = min(b['low'] for b in current_5min_bars_collected)  # Lowest low
                    current_5min_bar['volume'] = sum(b['volume'] for b in current_5min_bars_collected)  # Sum volume
                    
                    five_min_bars.append(current_5min_bar)
                
                # Start new 5-minute bar
                current_5min_bar = {
                    'datetime': boundary_timestamp,
                    'open': None,  # Will be set from first bar
                    'high': None,  # Will be calculated from all bars
                    'low': None,   # Will be calculated from all bars
                    'close': None, # Will be set from last bar
                    'volume': 0    # Will be sum of all bars
                }
                current_5min_bars_collected = []
            
            # Add this 1-minute bar to the current 5-minute collection
            current_5min_bars_collected.append(bar)
        
        # Don't forget the last 5-minute bar
        if current_5min_bar is not None and current_5min_bars_collected:
            current_5min_bar['open'] = current_5min_bars_collected[0]['open']  # First open
            current_5min_bar['close'] = current_5min_bars_collected[-1]['close']  # Last close
            current_5min_bar['high'] = max(b['high'] for b in current_5min_bars_collected)  # Highest high
            current_5min_bar['low'] = min(b['low'] for b in current_5min_bars_collected)  # Lowest low
            current_5min_bar['volume'] = sum(b['volume'] for b in current_5min_bars_collected)  # Sum volume
            
            five_min_bars.append(current_5min_bar)
        
        return five_min_bars

    def aggregate_5min_to_15min(self, five_min_bars):
        """Aggregate 5-minute bars into 15-minute bars at proper boundaries (0:00, 0:15, 0:30, 0:45)"""
        if not five_min_bars:
            return []
        
        fifteen_min_bars = []
        current_15min_bar = None
        current_15min_bars_collected = []
        
        for bar in five_min_bars:
            # Convert timestamp to datetime
            bar_time = datetime.fromtimestamp(bar['datetime'] / 1000)
            
            # Calculate which 15-minute boundary this bar belongs to (0, 15, 30, 45)
            minute = bar_time.minute
            fifteen_min_boundary = (minute // 15) * 15
            boundary_time = bar_time.replace(minute=fifteen_min_boundary, second=0, microsecond=0)
            boundary_timestamp = int(boundary_time.timestamp() * 1000)
            
            # If this is a new 15-minute period, finalize the previous one and start new
            if current_15min_bar is None or current_15min_bar['datetime'] != boundary_timestamp:
                # Finalize previous 15-minute bar if we have collected bars
                if current_15min_bar is not None and current_15min_bars_collected:
                    # Aggregate the collected 5-minute bars (should be 3 bars: 0, 5, 10 for a 15-min period)
                    current_15min_bar['open'] = current_15min_bars_collected[0]['open']  # First open
                    current_15min_bar['close'] = current_15min_bars_collected[-1]['close']  # Last close  
                    current_15min_bar['high'] = max(b['high'] for b in current_15min_bars_collected)  # Highest high
                    current_15min_bar['low'] = min(b['low'] for b in current_15min_bars_collected)  # Lowest low
                    current_15min_bar['volume'] = sum(b['volume'] for b in current_15min_bars_collected)  # Sum volume
                    
                    fifteen_min_bars.append(current_15min_bar)
                
                # Start new 15-minute bar
                current_15min_bar = {
                    'datetime': boundary_timestamp,
                    'open': None,  # Will be set from first 5-min bar
                    'high': None,  # Will be calculated from all 5-min bars
                    'low': None,   # Will be calculated from all 5-min bars
                    'close': None, # Will be set from last 5-min bar
                    'volume': 0    # Will be sum of all 5-min bars
                }
                current_15min_bars_collected = []
            
            # Add this 5-minute bar to the current 15-minute collection
            current_15min_bars_collected.append(bar)
        
        # Don't forget the last 15-minute bar
        if current_15min_bar is not None and current_15min_bars_collected:
            current_15min_bar['open'] = current_15min_bars_collected[0]['open']  # First open
            current_15min_bar['close'] = current_15min_bars_collected[-1]['close']  # Last close
            current_15min_bar['high'] = max(b['high'] for b in current_15min_bars_collected)  # Highest high
            current_15min_bar['low'] = min(b['low'] for b in current_15min_bars_collected)  # Lowest low
            current_15min_bar['volume'] = sum(b['volume'] for b in current_15min_bars_collected)  # Sum volume
            
            fifteen_min_bars.append(current_15min_bar)
        
        return fifteen_min_bars

    def aggregate_1min_to_15min(self, one_min_bars):
        """Aggregate 1-minute bars into 15-minute bars (DEPRECATED - use 5min aggregation instead)"""
        # This method is now deprecated - we should use 5min aggregation for better accuracy
        # First aggregate to 5min, then to 15min
        five_min_bars = self.aggregate_1min_to_5min(one_min_bars)
        return self.aggregate_5min_to_15min(five_min_bars)

    def load_5min_data_from_csv(self, symbol, lookback_periods=50):
        """Load recent 5-minute data from CSV for 15-minute aggregation"""
        try:
            filename = self.get_csv_filename(symbol, '5min')
            
            if not os.path.exists(filename):
                return []
            
            bars = []
            with open(filename, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header
                
                for row in reader:
                    if len(row) >= 7:
                        bar = {
                            'datetime': int(row[0]),
                            'open': float(row[2]),
                            'high': float(row[3]),
                            'low': float(row[4]),
                            'close': float(row[5]),
                            'volume': int(row[6])
                        }
                        bars.append(bar)
            
            # Return last N periods for aggregation
            return bars[-lookback_periods:] if bars else []
            
        except Exception as e:
            print(f"⚠️  Error loading 5min data from CSV: {e}")
            return []

    def load_1min_data_from_csv(self, symbol, lookback_periods=100):
        """Load recent 1-minute data from CSV for aggregation"""
        try:
            filename = self.get_csv_filename(symbol, '1min')
            
            if not os.path.exists(filename):
                return []
            
            bars = []
            with open(filename, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header
                
                for row in reader:
                    if len(row) >= 7:
                        bar = {
                            'datetime': int(row[0]),
                            'open': float(row[2]),
                            'high': float(row[3]),
                            'low': float(row[4]),
                            'close': float(row[5]),
                            'volume': int(row[6])
                        }
                        bars.append(bar)
            
            # Return last N periods for aggregation
            return bars[-lookback_periods:] if bars else []
            
        except Exception as e:
            print(f"⚠️  Error loading 1min data from CSV: {e}")
            return []

    def stream_1min_with_aggregation(self, symbol):
        """Simplified streaming: Check CSV timestamp and fetch missing data every minute"""
        print(f"🚀 Starting 1-minute data stream with targeted fetching for {symbol}")
        print(f"📊 Strategy: Check last CSV timestamp → Fetch missing data → Aggregate locally")
        print(f"💾 Files: {symbol}_1m.csv, {symbol}_5m.csv, {symbol}_15m.csv")
        print(f"⏰ Polling 5-15 seconds after each minute boundary (ensures complete bars)")
        print(f"🛑 Press Ctrl+C to stop streaming\n")
        
        # Initialize CSV files
        self.initialize_csv_file(symbol, '1min')
        self.initialize_csv_file(symbol, '5min')  
        self.initialize_csv_file(symbol, '15min')
        
        # STEP 1: Initial backfill to ensure we start with complete data
        print(f"🔄 STEP 1: Initial backfill to ensure complete dataset...")
        backfill_success = self.backfill_missing_1min_data(symbol)
        
        if not backfill_success:
            print(f"❌ Failed initial backfill. Continuing with real-time streaming...")
        
        print(f"\n🔄 STEP 2: Starting real-time streaming with dynamic fetching...")
        
        self.is_streaming = True
        
        try:
            while self.is_streaming:
                try:
                    current_time = datetime.now()
                    
                    # Check if we're 5+ seconds past a minute boundary
                    # This ensures the previous minute bar is complete
                    seconds_past_minute = current_time.second
                    
                    # If we're 5-15 seconds past the minute boundary, fetch data
                    if 5 <= seconds_past_minute <= 15:
                        print(f"\n⏰ {current_time.strftime('%H:%M:%S')} - Checking for missing data (5s after minute boundary)...")
                        
                        # Get last CSV timestamp to know where to start fetching
                        last_csv_timestamp = self.get_last_csv_timestamp(symbol)
                        
                        if last_csv_timestamp:
                            last_time = datetime.fromtimestamp(last_csv_timestamp / 1000)
                            print(f"📊 Last CSV entry: {last_time.strftime('%H:%M:%S')}")
                        else:
                            print(f"📊 No CSV data found")
                        
                        # Use targeted approach to fetch from last CSV timestamp to now
                        new_candles = self.get_minute_data_from_csv_to_now(symbol)
                        
                        if new_candles:
                            print(f"🆕 Retrieved {len(new_candles)} new minute candles!")
                            
                            # Sort by timestamp to ensure correct order
                            new_candles.sort(key=lambda x: x['datetime'])
                            
                            # Save each new candle
                            for candle in new_candles:
                                self.save_bar_to_csv_direct(symbol, '1min', candle)
                                
                                # Show the new bar
                                candle_time = datetime.fromtimestamp(candle['datetime'] / 1000)
                                print(f"   📊 {symbol} | {candle_time.strftime('%H:%M:%S')} | "
                                      f"O:{candle['open']:.2f} H:{candle['high']:.2f} "
                                      f"L:{candle['low']:.2f} C:{candle['close']:.2f} | "
                                      f"Vol:{candle['volume']:,}")
                            
                            # Now aggregate and save 5min and 15min bars
                            print(f"🔄 Aggregating to higher timeframes...")
                            self.aggregate_and_save_higher_timeframes(symbol, rebuild_all=False)
                            
                            # Show latest aggregated bar times
                            latest_5min = self.load_last_bar_from_csv(symbol, '5min')
                            latest_15min = self.load_last_bar_from_csv(symbol, '15min')
                            
                            if latest_5min:
                                time_5min = datetime.fromtimestamp(latest_5min['datetime'] / 1000)
                                print(f"   📈 Latest 5min bar: {time_5min.strftime('%H:%M:%S')}")
                            
                            if latest_15min:
                                time_15min = datetime.fromtimestamp(latest_15min['datetime'] / 1000)
                                print(f"   📊 Latest 15min bar: {time_15min.strftime('%H:%M:%S')}")
                        else:
                            print(f"✅ No new data - CSV is up to date")
                    
                    # Sleep for 10 seconds and check again
                    time.sleep(10)
                    
                except Exception as e:
                    print(f"❌ Error during streaming: {e}")
                    time.sleep(10)
        
        except KeyboardInterrupt:
            print(f"\n🛑 Stopping targeted data stream for {symbol}")
            self.is_streaming = False

    def get_last_csv_timestamp(self, symbol):
        """Get the timestamp of the last entry in 1min CSV file"""
        try:
            filename = self.get_csv_filename(symbol, '1min')
            
            if not os.path.exists(filename):
                return None
            
            with open(filename, 'r') as csvfile:
                lines = csvfile.readlines()
                if len(lines) <= 1:  # Only header or empty
                    return None
                
                # Parse last data line
                last_line = lines[-1].strip()
                if last_line:
                    parts = last_line.split(',')
                    if len(parts) >= 1:
                        return int(parts[0])  # Return timestamp in milliseconds
        except Exception as e:
            print(f"⚠️  Error reading last CSV timestamp: {e}")
        
        return None

    def get_today_market_open_timestamp(self):
        """Get today's market open timestamp (9:30 AM ET)"""
        today = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        return int(today.timestamp() * 1000)

    def fetch_complete_1min_history(self, symbol, access_token, from_timestamp=None):
        """Fetch complete 1min history from specified timestamp to now, or from 9:30 AM today if from_timestamp is None"""
        try:
            today_market_open = self.get_today_market_open_timestamp()
            current_time = datetime.now()
            
            # If no from_timestamp, start from today's market open (9:30 AM)
            if from_timestamp is None:
                from_timestamp = today_market_open
                print(f"📥 Fetching today's 1min data from 9:30 AM for {symbol}...")
            else:
                # Make sure we don't go earlier than today's market open
                if from_timestamp < today_market_open:
                    from_timestamp = today_market_open
                
                from_time = datetime.fromtimestamp(from_timestamp / 1000)
                print(f"📥 Fetching 1min data from {from_time.strftime('%H:%M')} to current time for {symbol}...")
            
            # Get today's data only
            params = {'period_type': 'day', 'period': 1, 'frequency_type': 'minute', 'frequency': 1}
            history_data = self.get_price_history(symbol, access_token, **params)
            
            if not history_data or 'candles' not in history_data:
                print(f"❌ No historical data received for {symbol}")
                return []
            
            all_bars = history_data['candles']
            
            # Filter bars to only include from specified timestamp to now
            # Also only include today's session (from 9:30 AM onwards)
            filtered_bars = []
            for bar in all_bars:
                bar_time = datetime.fromtimestamp(bar['datetime'] / 1000)
                
                # Only include bars from today's session and after from_timestamp
                if (bar['datetime'] >= from_timestamp and 
                    bar['datetime'] >= today_market_open and
                    bar_time.date() == current_time.date()):
                    filtered_bars.append(bar)
            
            if filtered_bars:
                first_bar_time = datetime.fromtimestamp(filtered_bars[0]['datetime'] / 1000)
                last_bar_time = datetime.fromtimestamp(filtered_bars[-1]['datetime'] / 1000)
                print(f"📊 Retrieved {len(filtered_bars)} 1min bars from {first_bar_time.strftime('%H:%M')} to {last_bar_time.strftime('%H:%M')}")
            else:
                print(f"📊 No bars found for today's session")
            
            return filtered_bars
            
        except Exception as e:
            print(f"❌ Error fetching today's 1min history: {e}")
            return []

    def get_current_session_bar_if_newer(self, symbol, access_token, last_csv_timestamp):
        """Get current session bar if it's newer than the last CSV entry"""
        try:
            current_bars = self.construct_current_bar_from_quote(symbol, '1min', access_token)
            
            if current_bars:
                current_bar = current_bars[0]
                if last_csv_timestamp is None or current_bar['datetime'] > last_csv_timestamp:
                    print(f"📊 Current session bar is newer than CSV data")
                    return current_bars
                else:
                    print(f"📊 Current session bar already in CSV")
            
            return []
            
        except Exception as e:
            print(f"⚠️  Error getting current session bar: {e}")
            return []

    def backfill_missing_1min_data(self, symbol):
        """Check CSV and backfill any missing 1min data using targeted API approach"""
        try:
            print(f"\n🔍 Checking {symbol}_1m.csv for missing data...")
            
            # Initialize CSV file if it doesn't exist
            self.initialize_csv_file(symbol, '1min')
            
            # Use the new targeted approach to get all missing minute data
            missing_candles = self.get_minute_data_from_csv_to_now(symbol)
            
            if missing_candles:
                print(f"💾 Saving {len(missing_candles)} missing 1min bars...")
                
                # Sort by timestamp to ensure correct order
                missing_candles.sort(key=lambda x: x['datetime'])
                
                # Save each bar using the direct CSV method
                for candle in missing_candles:
                    self.save_bar_to_csv_direct(symbol, '1min', candle)
                
                print(f"✅ Backfilled {len(missing_candles)} 1min bars using targeted API")
                
                # Now aggregate and update higher timeframes
                print(f"🔄 Updating 5min and 15min aggregations...")
                self.aggregate_and_save_higher_timeframes(symbol, rebuild_all=True)
                
                return True
            else:
                print(f"✅ No missing 1min data - CSV is up to date")
                return True
                
        except Exception as e:
            print(f"❌ Error during targeted backfill: {e}")
            return False

    def get_minute_data_from_csv_to_now(self, symbol):
        """
        Get 1-minute data from last CSV timestamp to current time
        If no CSV file or empty, start from today's 9:30 AM
        Uses targeted startDate/endDate for current day data
        """
        print(f"🚀 Getting minute data for {symbol} from CSV to now")
        print("=" * 50)
        
        # Get access token
        access_token = self.get_valid_access_token()
        if not access_token:
            print("❌ Failed to get access token")
            return []
        
        # Get last timestamp from CSV or use 9:30 AM today
        last_timestamp_ms = self.get_last_csv_timestamp(symbol)
        
        print(f"🔍 DEBUG: Raw last_timestamp_ms from CSV: {last_timestamp_ms}")
        
        if last_timestamp_ms is None:
            # No CSV or empty CSV - start from 9:30 AM today
            start_time = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
            print(f"📅 No CSV data found, starting from market open: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            # Start from next minute after last CSV entry
            last_datetime = datetime.fromtimestamp(last_timestamp_ms / 1000)
            start_time = last_datetime + timedelta(minutes=1)
            print(f"📅 Last CSV timestamp: {last_timestamp_ms} = {last_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"📅 Continuing from CSV, next minute: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # End time should be the PREVIOUS completed minute to avoid getting incomplete bars
        current_time = datetime.now()
        # If we're at 14:53:25, we want end_time = 14:52:00 (last completed minute)
        end_time = current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
        
        print(f"🔍 DEBUG: Current time: {current_time}")
        print(f"🔍 DEBUG: Start time: {start_time}")
        print(f"🔍 DEBUG: End time: {end_time} (previous completed minute)")
        
        # Check if we need to fetch any data
        if start_time >= end_time:
            print(f"✅ CSV is already up to date!")
            return []
        
        # Calculate expected number of minutes
        minutes_to_fetch = int((end_time - start_time).total_seconds() / 60)
        print(f"📊 Need to fetch ~{minutes_to_fetch} minutes of data")
        print(f"📊 From: {start_time.strftime('%Y-%m-%d %H:%M:%S')} To: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Make targeted API call with date range
        print(f"🔍 DEBUG: Calling get_price_history with start_datetime={start_time}, end_datetime={end_time}")
        
        history_data = self.get_price_history(
            symbol, 
            access_token, 
            period_type='day', 
            period=10,  # Max period to ensure we get data
            frequency_type='minute', 
            frequency=1,
            start_datetime=start_time,  # KEY: This enables current day data!
            end_datetime=end_time
        )
        
        if history_data and 'candles' in history_data:
            candles = history_data['candles']
            print(f"✅ Successfully retrieved {len(candles)} minute candles!")
            
            if candles:
                first_candle_time = datetime.fromtimestamp(candles[0]['datetime'] / 1000)
                last_candle_time = datetime.fromtimestamp(candles[-1]['datetime'] / 1000)
                print(f"📈 Range: {first_candle_time.strftime('%Y-%m-%d %H:%M')} to {last_candle_time.strftime('%Y-%m-%d %H:%M')}")
                print(f"🔍 DEBUG: First candle timestamp: {candles[0]['datetime']}")
                print(f"🔍 DEBUG: Expected start after: {last_timestamp_ms}")
                
                # Filter out any candles that are already in CSV (shouldn't happen with proper targeting)
                if last_timestamp_ms is not None:
                    filtered_candles = [c for c in candles if c['datetime'] > last_timestamp_ms]
                    if len(filtered_candles) != len(candles):
                        print(f"🔍 DEBUG: Filtered out {len(candles) - len(filtered_candles)} duplicate candles")
                        candles = filtered_candles
            
            return candles
        else:
            print(f"❌ Failed to retrieve minute data")
            return []

    def aggregate_and_save_higher_timeframes(self, symbol, rebuild_all=False):
        """Load 1min data and aggregate into 5min and 15min, then save to CSV"""
        try:
            # Determine how much 1-minute data we need to load
            if rebuild_all:
                # Load all available data for complete rebuild
                one_min_bars = self.load_1min_data_from_csv(symbol, lookback_periods=10000)  # Get all data
                print(f"📊 Loaded {len(one_min_bars)} 1min bars for complete rebuild")
            else:
                # Determine the range we need to cover based on last saved bars
                last_saved_5min = self.load_last_bar_from_csv(symbol, '5min')
                last_saved_15min = self.load_last_bar_from_csv(symbol, '15min')
                last_1min = self.load_last_bar_from_csv(symbol, '1min')
                
                if last_1min is None:
                    print(f"📊 No 1min data available for aggregation")
                    return
                
                # Determine how far back we need to go
                earliest_timestamp_needed = None
                
                if last_saved_5min:
                    # Start from the 5-minute boundary that contains the last saved 5min bar
                    last_5min_time = datetime.fromtimestamp(last_saved_5min['datetime'] / 1000)
                    print(f"📊 Last saved 5min bar: {last_5min_time.strftime('%H:%M')}")
                    earliest_timestamp_needed = last_saved_5min['datetime']
                
                if last_saved_15min:
                    # Also consider 15-minute boundary
                    last_15min_time = datetime.fromtimestamp(last_saved_15min['datetime'] / 1000)
                    print(f"📊 Last saved 15min bar: {last_15min_time.strftime('%H:%M')}")
                    if earliest_timestamp_needed is None or last_saved_15min['datetime'] < earliest_timestamp_needed:
                        earliest_timestamp_needed = last_saved_15min['datetime']
                
                if earliest_timestamp_needed is None:
                    # No saved aggregated data, start from beginning of day
                    today_start = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
                    earliest_timestamp_needed = int(today_start.timestamp() * 1000)
                    print(f"📊 No saved aggregated data, starting from market open")
                
                # Load all 1-minute data from the earliest needed timestamp
                all_1min_bars = self.load_1min_data_from_csv(symbol, lookback_periods=10000)
                
                # Filter to only include bars from earliest_timestamp_needed onwards
                one_min_bars = []
                for bar in all_1min_bars:
                    if bar['datetime'] >= earliest_timestamp_needed:
                        one_min_bars.append(bar)
                
                print(f"📊 Loaded {len(one_min_bars)} 1min bars from {datetime.fromtimestamp(earliest_timestamp_needed / 1000).strftime('%H:%M')} onwards")
            
            if not one_min_bars:
                print(f"📊 No 1min data available for aggregation")
                return
            
            # Show the time range we're working with
            first_bar_time = datetime.fromtimestamp(one_min_bars[0]['datetime'] / 1000)
            last_bar_time = datetime.fromtimestamp(one_min_bars[-1]['datetime'] / 1000)
            print(f"📊 Aggregating 1min data from {first_bar_time.strftime('%H:%M')} to {last_bar_time.strftime('%H:%M')}")
            
            # Aggregate to 5-minute bars
            five_min_bars = self.aggregate_1min_to_5min(one_min_bars)
            if five_min_bars:
                self.initialize_csv_file(symbol, '5min')
                
                if rebuild_all:
                    # Rebuild entire 5min file
                    print(f"🔄 Rebuilding complete 5min dataset...")
                    filename = self.get_csv_filename(symbol, '5min')
                    with open(filename, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
                    
                    # Save all bars
                    for bar in five_min_bars:
                        self.save_bar_to_csv_direct(symbol, '5min', bar)
                    
                    print(f"💾 Rebuilt {len(five_min_bars)} 5min bars")
                else:
                    # Identify which 5min bars are actually new
                    last_saved_5min = self.load_last_bar_from_csv(symbol, '5min')
                    
                    new_5min_bars = []
                    updated_5min_bars = []
                    
                    for bar in five_min_bars:
                        if last_saved_5min is None or bar['datetime'] > last_saved_5min['datetime']:
                            new_5min_bars.append(bar)
                        elif bar['datetime'] == last_saved_5min['datetime']:
                            # This might be an updated version of the last bar (with more 1min data)
                            updated_5min_bars.append(bar)
                    
                    # If we have updated bars, we need to rebuild recent data to avoid duplicates
                    if updated_5min_bars:
                        print(f"🔄 Found {len(updated_5min_bars)} updated 5min bars, rebuilding recent data...")
                        # Remove the last few bars from CSV and re-add the corrected ones
                        all_existing_5min = self.load_5min_data_from_csv(symbol, lookback_periods=10000)
                        
                        # Keep only bars older than the earliest updated bar
                        earliest_updated = min(bar['datetime'] for bar in updated_5min_bars)
                        cleaned_5min_bars = [bar for bar in all_existing_5min if bar['datetime'] < earliest_updated]
                        
                        # Rebuild the file with cleaned data + all new data
                        filename = self.get_csv_filename(symbol, '5min')
                        with open(filename, 'w', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
                        
                        # Save cleaned existing data + all new aggregated data
                        all_bars_to_save = cleaned_5min_bars + updated_5min_bars + new_5min_bars
                        all_bars_to_save.sort(key=lambda x: x['datetime'])  # Ensure chronological order
                        
                        for bar in all_bars_to_save:
                            self.save_bar_to_csv_direct(symbol, '5min', bar)
                        
                        print(f"💾 Rebuilt 5min file with {len(all_bars_to_save)} total bars")
                    else:
                        # Just save new bars
                        for bar in new_5min_bars:
                            self.save_bar_to_csv_direct(symbol, '5min', bar)
                        
                        if new_5min_bars:
                            print(f"💾 Saved {len(new_5min_bars)} new 5min bars")
            
            # Aggregate to 15-minute bars using 5-minute data
            # Load all current 5-minute data to ensure we have complete coverage
            all_5min_bars = self.load_5min_data_from_csv(symbol, lookback_periods=10000)
            fifteen_min_bars = self.aggregate_5min_to_15min(all_5min_bars)
            
            if fifteen_min_bars:
                self.initialize_csv_file(symbol, '15min')
                
                if rebuild_all:
                    # Rebuild entire 15min file
                    print(f"🔄 Rebuilding complete 15min dataset...")
                    filename = self.get_csv_filename(symbol, '15min')
                    with open(filename, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
                    
                    # Save all bars
                    for bar in fifteen_min_bars:
                        self.save_bar_to_csv_direct(symbol, '15min', bar)
                    
                    print(f"💾 Rebuilt {len(fifteen_min_bars)} 15min bars")
                else:
                    # Save new 15min bars (similar logic as 5min)
                    last_saved_15min = self.load_last_bar_from_csv(symbol, '15min')
                    
                    new_15min_bars = []
                    updated_15min_bars = []
                    
                    for bar in fifteen_min_bars:
                        if last_saved_15min is None or bar['datetime'] > last_saved_15min['datetime']:
                            new_15min_bars.append(bar)
                        elif bar['datetime'] == last_saved_15min['datetime']:
                            updated_15min_bars.append(bar)
                    
                    if updated_15min_bars:
                        print(f"🔄 Found {len(updated_15min_bars)} updated 15min bars, rebuilding recent data...")
                        all_existing_15min = self.load_15min_data_from_csv(symbol, lookback_periods=10000)
                        
                        earliest_updated = min(bar['datetime'] for bar in updated_15min_bars)
                        cleaned_15min_bars = [bar for bar in all_existing_15min if bar['datetime'] < earliest_updated]
                        
                        filename = self.get_csv_filename(symbol, '15min')
                        with open(filename, 'w', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
                        
                        all_bars_to_save = cleaned_15min_bars + updated_15min_bars + new_15min_bars
                        all_bars_to_save.sort(key=lambda x: x['datetime'])
                        
                        for bar in all_bars_to_save:
                            self.save_bar_to_csv_direct(symbol, '15min', bar)
                        
                        print(f"💾 Rebuilt 15min file with {len(all_bars_to_save)} total bars")
                    else:
                        for bar in new_15min_bars:
                            self.save_bar_to_csv_direct(symbol, '15min', bar)
                        
                        if new_15min_bars:
                            print(f"💾 Saved {len(new_15min_bars)} new 15min bars")
            
        except Exception as e:
            print(f"❌ Error in enhanced aggregation: {e}")

    def save_bar_to_csv_direct(self, symbol, timeframe, bar):
        """Save a bar directly to CSV (used for aggregated bars)"""
        try:
            filename = self.get_csv_filename(symbol, timeframe)
            
            # Convert timestamp from milliseconds to readable format
            timestamp = datetime.fromtimestamp(bar['datetime'] / 1000)
            
            # Prepare data row
            row = [
                bar['datetime'],  # Unix timestamp in milliseconds
                timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # Human readable datetime
                bar['open'],
                bar['high'], 
                bar['low'],
                bar['close'],
                bar['volume']
            ]
            
            # Append to CSV file
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(row)
            
        except Exception as e:
            print(f"❌ Error saving aggregated bar to CSV: {e}")

    def load_15min_data_from_csv(self, symbol, lookback_periods=100):
        """Load recent 15-minute data from CSV"""
        try:
            filename = self.get_csv_filename(symbol, '15min')
            
            if not os.path.exists(filename):
                return []
            
            bars = []
            with open(filename, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header
                
                for row in reader:
                    if len(row) >= 7:
                        bar = {
                            'datetime': int(row[0]),
                            'open': float(row[2]),
                            'high': float(row[3]),
                            'low': float(row[4]),
                            'close': float(row[5]),
                            'volume': int(row[6])
                        }
                        bars.append(bar)
            
            # Return last N periods
            return bars[-lookback_periods:] if bars else []
            
        except Exception as e:
            print(f"⚠️  Error loading 15min data from CSV: {e}")
            return []


# MAIN EXECUTION
if __name__ == "__main__":
    # Create an instance of the SchwabDataFetcher
    fetcher = SchwabDataFetcher()
    
    print("🚀 Schwab Real-Time OHLC Data Streamer")
    print("=" * 50)
    print("📊 Features:")
    print("   • Fetches 1-minute bars from Schwab API")
    print("   • Aggregates locally to 5min & 15min bars")
    print("   • Saves to CSV in data folder: symbol_1m.csv, symbol_5m.csv, symbol_15m.csv")
    print("   • Perfect timeframe alignment (:00, :15, :30, :45 for 15min)")
    print("   • Uses current day session data (not delayed)")
    print("   • Only 1 API call per minute (efficient)")
    print()
    
    symbol = input("Enter symbol to stream (e.g., SPY, AAPL, TSLA): ").strip().upper()
    
    if not symbol:
        print("❌ No symbol entered. Exiting.")
        exit()
    
    print(f"\n🎯 Starting real-time streaming for {symbol}")
    print(f"📈 Strategy: 1min API data → Local aggregation to 5min & 15min")
    print(f"💾 Output files in data folder:")
    print(f"   📄 data/{symbol}_1m.csv  (1-minute bars)")
    print(f"   📄 data/{symbol}_5m.csv  (5-minute bars)")  
    print(f"   📄 data/{symbol}_15m.csv (15-minute bars)")
    print(f"\n🔄 Program will run continuously...")
    print(f"🛑 Press Ctrl+C to stop\n")
    
    try:
        fetcher.stream_1min_with_aggregation(symbol)
    except KeyboardInterrupt:
        print("\n🛑 Streaming stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        fetcher.stop_streaming()
        print("✅ Program ended gracefully")
        print(f"📊 Check the data folder for your {symbol} CSV files!")