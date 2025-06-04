#!/usr/bin/env python3
"""
Market Data Updater for Schwab API
Functions to:
1. Get latest timestamp from CSV files
2. Convert ET market hours to UNIX epoch milliseconds  
3. Retrieve price history from Schwab API
4. Filter and append new data to CSV files
5. Aggregate 1m data into 5m and 15m candles
6. Run continuously during market hours with smart token management
7. Track trading positions based on technical indicators with email alerts
"""

import requests
import json
import os
import csv
import pandas as pd
from datetime import datetime, timezone, time
import pytz
from typing import Optional, Dict, List, Tuple
import base64
import time as time_module
import signal
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class MarketDataUpdater:
    def __init__(self):
        self.data_dir = "data"
        self.et_timezone = pytz.timezone('US/Eastern')
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET
        self.last_token_refresh = None
        self.token_refresh_interval = 20 * 60  # 20 minutes in seconds
        self.running = True
        
        # Position tracking for all timeframes
        self.positions = {
            '1m': 'CLOSED',
            '5m': 'CLOSED', 
            '15m': 'CLOSED'
        }
        
        # Track opening prices for P&L calculation
        self.opening_prices = {
            '1m': None,
            '5m': None,
            '15m': None
        }
        
        # Email configuration
        self.email_config = self.load_email_config()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def load_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """Load Schwab API credentials from environment file"""
        credentials_file = 'schwab_credentials.env'
        
        if os.path.exists(credentials_file):
            try:
                with open(credentials_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key.strip()] = value.strip()
            except Exception as e:
                print(f"⚠️  Error loading credentials file: {e}")
        
        app_key = os.getenv('SCHWAB_APP_KEY')
        app_secret = os.getenv('SCHWAB_APP_SECRET')
        
        if not app_key or not app_secret:
            print("❌ Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET")
            return None, None
            
        return app_key, app_secret

    def is_token_valid(self) -> bool:
        """Check if current access token is still valid"""
        try:
            with open('schwab_access_token.txt', 'r') as f:
                token_data = json.load(f)
            
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            # Consider token expired if it expires within 5 minutes
            buffer_time = datetime.now() + pd.Timedelta(minutes=5)
            
            return buffer_time < expires_at
            
        except (FileNotFoundError, KeyError, ValueError):
            return False

    def should_refresh_token_proactively(self) -> bool:
        """Check if we should proactively refresh token (every 20 minutes)"""
        if self.last_token_refresh is None:
            return True
            
        time_since_refresh = time_module.time() - self.last_token_refresh
        return time_since_refresh >= self.token_refresh_interval

    def get_access_token(self) -> Optional[str]:
        """Get current access token, refresh if needed"""
        try:
            # Check if we should proactively refresh
            if self.should_refresh_token_proactively():
                print("🕒 Proactive token refresh (20-minute interval)")
                if self.refresh_access_token():
                    self.last_token_refresh = time_module.time()
                else:
                    print("⚠️  Proactive token refresh failed")
            
            # Check if token is still valid
            if not self.is_token_valid():
                print("🔄 Access token expired, refreshing...")
                if self.refresh_access_token():
                    self.last_token_refresh = time_module.time()
                else:
                    return None
            
            with open('schwab_access_token.txt', 'r') as f:
                token_data = json.load(f)
            return token_data['access_token']
                
        except FileNotFoundError:
            print("❌ Access token file not found")
            return None
        except Exception as e:
            print(f"❌ Error loading access token: {e}")
            return None

    def refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token"""
        print("🔄 Refreshing access token...")
        
        app_key, app_secret = self.load_credentials()
        if not app_key or not app_secret:
            return False
        
        try:
            with open('schwab_refresh_token.txt', 'r') as f:
                refresh_token = f.read().strip()
        except Exception as e:
            print(f"❌ Failed to load refresh token: {e}")
            return False
        
        token_url = "https://api.schwabapi.com/v1/oauth/token"
        credentials = f"{app_key}:{app_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        
        try:
            response = requests.post(token_url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                current_time = datetime.now()
                expires_in = token_data.get('expires_in', 1800)
                expires_at = current_time.timestamp() + expires_in
                
                token_info = {
                    'access_token': token_data['access_token'],
                    'created_at': current_time.isoformat(),
                    'expires_at': datetime.fromtimestamp(expires_at).isoformat(),
                    'expires_in': expires_in
                }
                
                with open('schwab_access_token.txt', 'w') as f:
                    json.dump(token_info, f)
                
                if 'refresh_token' in token_data:
                    with open('schwab_refresh_token.txt', 'w') as f:
                        f.write(token_data['refresh_token'])
                
                print("✅ Access token refreshed successfully")
                return True
            else:
                print(f"❌ Token refresh failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error during token refresh: {e}")
            return False

    def is_market_hours(self) -> bool:
        """Check if current time is within market hours"""
        current_time = datetime.now(self.et_timezone).time()
        return self.market_open <= current_time <= self.market_close

    def is_market_day(self) -> bool:
        """Check if today is a market day (weekday)"""
        current_date = datetime.now(self.et_timezone)
        return current_date.weekday() < 5  # Monday = 0, Friday = 4

    def wait_for_next_minute_boundary(self, offset_seconds: int = 5):
        """Wait until offset_seconds after the next minute boundary"""
        while self.running:
            now = datetime.now()
            
            # Calculate next minute boundary + offset
            next_minute = now.replace(second=0, microsecond=0) + pd.Timedelta(minutes=1)
            target_time = next_minute + pd.Timedelta(seconds=offset_seconds)
            
            # If we've already passed this minute's target time, wait for next minute
            if now >= target_time:
                next_minute = next_minute + pd.Timedelta(minutes=1)
                target_time = next_minute + pd.Timedelta(seconds=offset_seconds)
            
            wait_seconds = (target_time - now).total_seconds()
            
            if wait_seconds > 0:
                print(f"⏰ Waiting {wait_seconds:.1f} seconds until {target_time.strftime('%H:%M:%S')} ET...")
                
                # Sleep in chunks to allow for graceful shutdown
                while wait_seconds > 0 and self.running:
                    sleep_time = min(1.0, wait_seconds)
                    time_module.sleep(sleep_time)
                    wait_seconds -= sleep_time
            
            # Check current time after sleeping
            current_time = datetime.now()
            if current_time >= target_time:
                break

    def run_continuous_data_collection(self, symbol: str = "SPY"):
        """
        Run continuous market data collection during market hours
        
        Args:
            symbol: Stock symbol to collect data for
        """
        print("🚀 Starting Continuous Market Data Collection")
        print("=" * 60)
        print(f"📊 Symbol: {symbol}")
        print(f"🕒 Market Hours: {self.market_open} - {self.market_close} ET")
        print(f"🔄 Token Refresh: Every {self.token_refresh_interval // 60} minutes")
        print(f"⏰ API Calls: 5 seconds after each minute")
        print(f"🎯 Position Tracking: ENABLED for all timeframes")
        print("=" * 60)
        
        if not self.is_market_day():
            print("📅 Not a market day (weekend). Exiting.")
            return
        
        # Initial token refresh
        if not self.get_access_token():
            print("❌ Failed to get initial access token. Exiting.")
            return
        
        # Bootstrap Phase: Complete update to fill any missing data
        print("\n🔄 BOOTSTRAP PHASE: Filling any missing data...")
        print("-" * 60)
        bootstrap_success = self.update_market_data_with_aggregation(symbol)
        
        if bootstrap_success:
            print("✅ Bootstrap phase completed successfully")
            
            # Analyze historical positions after bootstrap
            print("\n🎯 POSITION ANALYSIS: Analyzing historical data...")
            print("-" * 60)
            try:
                historical_results = self.analyze_historical_positions(symbol)
                print(f"✅ Historical position analysis completed")
                print(f"   Found {historical_results['total_signals']} total position signals")
            except Exception as e:
                print(f"⚠️  Historical analysis had issues: {e}")
        else:
            print("⚠️  Bootstrap phase had some issues, but continuing...")
        
        # Start incremental updates
        print(f"\n🔄 INCREMENTAL PHASE: Starting real-time updates...")
        print("-" * 60)
        
        iteration_count = 0
        
        while self.running:
            iteration_count += 1
            
            try:
                # Check if we're in market hours
                if not self.is_market_hours():
                    current_time = datetime.now(self.et_timezone)
                    if current_time.time() < self.market_open:
                        print(f"🌅 Market hasn't opened yet. Current time: {current_time.strftime('%H:%M:%S')} ET")
                        print(f"⏰ Waiting for market open at {self.market_open}...")
                    else:
                        print(f"🌙 Market has closed. Current time: {current_time.strftime('%H:%M:%S')} ET")
                        print("✅ End of trading day. Shutting down.")
                        break
                    
                    # Wait 60 seconds before checking again
                    time_module.sleep(60)
                    continue
                
                # Wait for the next minute boundary + 5 seconds
                self.wait_for_next_minute_boundary(offset_seconds=5)
                
                if not self.running:
                    break
                
                current_time = datetime.now(self.et_timezone)
                print(f"\n🔄 Incremental Update Cycle #{iteration_count}")
                print(f"🕒 {current_time.strftime('%Y-%m-%d %H:%M:%S')} ET")
                print("-" * 50)
                
                # Run the complete market data update workflow
                success = self.update_market_data_with_aggregation(symbol)
                
                if success:
                    print(f"✅ Incremental cycle #{iteration_count} completed successfully")
                    
                    # Check for live position signals after data update
                    print(f"🎯 Checking live position signals...")
                    signals_found = self.check_live_position_signals(symbol)
                    
                    if signals_found:
                        print(f"🚨 New position signals detected and processed!")
                    else:
                        print(f"📊 No new position signals at this time")
                        
                    # Show current position status
                    print(f"📊 Current Positions: 1m:{self.positions['1m']} | 5m:{self.positions['5m']} | 15m:{self.positions['15m']}")
                else:
                    print(f"⚠️  Incremental cycle #{iteration_count} had some issues")
                
                # Brief pause before next cycle
                time_module.sleep(1)
                
            except KeyboardInterrupt:
                print("\n🛑 Interrupted by user")
                break
            except Exception as e:
                print(f"❌ Error in incremental cycle #{iteration_count}: {e}")
                print("⏳ Waiting 30 seconds before retry...")
                time_module.sleep(30)
        
        print("\n🏁 Continuous data collection stopped")
        print(f"📊 Bootstrap: ✅ Completed")
        print(f"📊 Incremental cycles: {iteration_count}")
        print(f"🎯 Final Positions: 1m:{self.positions['1m']} | 5m:{self.positions['5m']} | 15m:{self.positions['15m']}")
        print(f"📊 Total data collection session complete")

    def get_latest_timestamp_from_csv(self, symbol: str, period: str) -> Optional[int]:
        """
        Get the latest timestamp from a ticker CSV file
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            
        Returns:
            Latest timestamp in milliseconds, or None if file is empty/doesn't exist
        """
        csv_path = os.path.join(self.data_dir, f"{symbol}_{period}.csv")
        
        if not os.path.exists(csv_path):
            print(f"📁 CSV file {csv_path} does not exist")
            return None
        
        try:
            df = pd.read_csv(csv_path)
            
            if df.empty or 'timestamp' not in df.columns:
                print(f"📊 CSV file {csv_path} is empty or missing timestamp column")
                return None
            
            # Remove any empty rows
            df = df.dropna(subset=['timestamp'])
            
            if df.empty:
                print(f"📊 No valid timestamps found in {csv_path}")
                return None
            
            latest_timestamp = int(df['timestamp'].max())
            latest_datetime = datetime.fromtimestamp(latest_timestamp / 1000)
            
            print(f"📅 Latest timestamp in {csv_path}: {latest_timestamp} ({latest_datetime})")
            return latest_timestamp
            
        except Exception as e:
            print(f"❌ Error reading CSV file {csv_path}: {e}")
            return None

    def convert_et_to_epoch_ms(self, target_date: datetime.date = None) -> Tuple[int, int]:
        """
        Convert 9:30 AM ET and 4:00 PM ET to UNIX epoch milliseconds
        
        Args:
            target_date: Date to use (defaults to today)
            
        Returns:
            Tuple of (start_time_ms, end_time_ms)
        """
        if target_date is None:
            target_date = datetime.now(self.et_timezone).date()
        
        # Create datetime objects for market open and close in ET
        market_open_et = self.et_timezone.localize(
            datetime.combine(target_date, self.market_open)
        )
        market_close_et = self.et_timezone.localize(
            datetime.combine(target_date, self.market_close)
        )
        
        # Convert to UTC and then to epoch milliseconds
        start_time_ms = int(market_open_et.astimezone(timezone.utc).timestamp() * 1000)
        end_time_ms = int(market_close_et.astimezone(timezone.utc).timestamp() * 1000)
        
        print(f"🕘 Market hours for {target_date}:")
        print(f"   Start: {market_open_et} ({start_time_ms})")
        print(f"   End: {market_close_et} ({end_time_ms})")
        
        return start_time_ms, end_time_ms

    def get_price_history_from_schwab(self, symbol: str, start_time_ms: int, end_time_ms: int) -> Optional[List[Dict]]:
        """
        Retrieve price history from Schwab API
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            start_time_ms: Start time in UNIX epoch milliseconds
            end_time_ms: End time in UNIX epoch milliseconds
            
        Returns:
            List of price data dictionaries, or None if failed
        """
        access_token = self.get_access_token()
        if not access_token:
            print("❌ No valid access token available")
            return None
        
        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        params = {
            'symbol': symbol,
            'periodType': 'day',
            'period': 1,
            'frequencyType': 'minute',
            'frequency': 1,
            'startDate': start_time_ms,
            'endDate': end_time_ms,
            'needExtendedHoursData': 'false',
            'needPreviousClose': 'false'
        }
        
        print(f"📡 Fetching price history for {symbol} from Schwab API...")
        print(f"   URL: {url}")
        print(f"   Params: {params}")
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'candles' in data and data['candles']:
                    print(f"✅ Retrieved {len(data['candles'])} candles from Schwab API")
                    return data['candles']
                else:
                    print("📊 No candle data found in API response")
                    return []
            else:
                print(f"❌ API request failed: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching price history: {e}")
            return None

    def filter_new_data(self, candles: List[Dict], last_timestamp_ms: Optional[int]) -> List[Dict]:
        """
        Filter candles to only include data after the last recorded timestamp
        and exclude the current minute that's still forming
        
        Args:
            candles: List of candle data from API
            last_timestamp_ms: Last timestamp in CSV (in milliseconds)
            
        Returns:
            Filtered list of new candles (excluding current forming minute)
        """
        if not candles:
            return []
        
        # Get current time and calculate the current minute boundary
        current_time = datetime.now(self.et_timezone)
        current_minute_boundary = current_time.replace(second=0, microsecond=0)
        current_minute_ms = int(current_minute_boundary.timestamp() * 1000)
        
        print(f"🕐 Current time: {current_time.strftime('%H:%M:%S')} ET")
        print(f"🕐 Current minute boundary: {current_minute_boundary.strftime('%H:%M:%S')} ({current_minute_ms})")
        
        # Filter out the current forming minute
        completed_candles = []
        for candle in candles:
            candle_timestamp = candle.get('datetime')
            if candle_timestamp and candle_timestamp < current_minute_ms:
                completed_candles.append(candle)
        
        print(f"🔍 Filtered out current forming minute: {len(candles)} → {len(completed_candles)} completed candles")
        
        # Now filter based on last recorded timestamp
        if last_timestamp_ms is None:
            print("📊 No previous timestamp found, returning all completed candles")
            new_candles = completed_candles
        else:
            new_candles = []
            for candle in completed_candles:
                candle_timestamp = candle.get('datetime')
                if candle_timestamp and candle_timestamp > last_timestamp_ms:
                    new_candles.append(candle)
        
        print(f"🔍 Filtered {len(new_candles)} new completed candles from {len(completed_candles)} total completed candles")
        print(f"   Last recorded: {datetime.fromtimestamp(last_timestamp_ms / 1000) if last_timestamp_ms else 'None'}")
        
        if new_candles:
            first_new = datetime.fromtimestamp(new_candles[0]['datetime'] / 1000)
            last_new = datetime.fromtimestamp(new_candles[-1]['datetime'] / 1000)
            print(f"   New data range: {first_new} to {last_new}")
        else:
            print("   No new completed candles to save")
        
        return new_candles

    def calculate_ema(self, prices: List[float], period: int = 7) -> List[float]:
        """
        Calculate Exponential Moving Average (EMA)
        
        Args:
            prices: List of closing prices
            period: EMA period (default 7)
            
        Returns:
            List of EMA values
        """
        if len(prices) < period:
            return [None] * len(prices)
        
        ema_values = [None] * len(prices)
        multiplier = 2.0 / (period + 1)
        
        # First EMA value is SMA of first 'period' prices
        ema_values[period - 1] = sum(prices[:period]) / period
        
        # Calculate subsequent EMA values
        for i in range(period, len(prices)):
            ema_values[i] = (prices[i] * multiplier) + (ema_values[i - 1] * (1 - multiplier))
        
        return ema_values

    def calculate_macd(self, prices: List[float]) -> Tuple[List[float], List[float]]:
        """
        Calculate MACD (Moving Average Convergence Divergence) and Signal Line
        
        Args:
            prices: List of closing prices
            
        Returns:
            Tuple of (MACD line, MACD signal line)
        """
        if len(prices) < 26:  # Need at least 26 periods for 26 EMA
            return [None] * len(prices), [None] * len(prices)
        
        # Calculate 12 EMA and 26 EMA
        ema_12 = self.calculate_ema(prices, 12)
        ema_26 = self.calculate_ema(prices, 26)
        
        # Calculate MACD line (12 EMA - 26 EMA)
        macd_line = []
        for i in range(len(prices)):
            if ema_12[i] is not None and ema_26[i] is not None:
                macd_line.append(ema_12[i] - ema_26[i])
            else:
                macd_line.append(None)
        
        # Calculate MACD Signal Line (9 EMA of MACD line)
        # Filter out None values for EMA calculation
        macd_for_signal = [x for x in macd_line if x is not None]
        if len(macd_for_signal) < 9:
            signal_line = [None] * len(prices)
        else:
            # Calculate 9 EMA of MACD values
            signal_ema = self.calculate_ema(macd_for_signal, 9)
            
            # Map back to original length with None padding
            signal_line = [None] * len(prices)
            signal_start_idx = next(i for i, x in enumerate(macd_line) if x is not None)
            
            for i, val in enumerate(signal_ema):
                if val is not None and signal_start_idx + i < len(signal_line):
                    signal_line[signal_start_idx + i] = val
        
        return macd_line, signal_line

    def calculate_roc(self, prices: List[float], period: int = 8) -> List[float]:
        """
        Calculate Rate of Change (ROC)
        
        Args:
            prices: List of closing prices
            period: ROC period (default 8)
            
        Returns:
            List of ROC values as percentages
        """
        if len(prices) < period + 1:
            return [None] * len(prices)
        
        roc_values = [None] * len(prices)
        
        for i in range(period, len(prices)):
            current_price = prices[i]
            past_price = prices[i - period]
            
            if past_price and past_price != 0:
                roc_values[i] = ((current_price - past_price) / past_price) * 100
            else:
                roc_values[i] = None
        
        return roc_values

    def calculate_vwma(self, prices: List[float], volumes: List[float], period: int = 17) -> List[float]:
        """
        Calculate Volume Weighted Moving Average (VWMA)
        
        Args:
            prices: List of closing prices
            volumes: List of volumes
            period: VWMA period (default 17)
            
        Returns:
            List of VWMA values
        """
        if len(prices) < period or len(volumes) < period:
            return [None] * len(prices)
        
        vwma_values = [None] * len(prices)
        
        for i in range(period - 1, len(prices)):
            # Get the last 'period' prices and volumes
            period_prices = prices[i - period + 1:i + 1]
            period_volumes = volumes[i - period + 1:i + 1]
            
            # Calculate VWMA: Sum(Price × Volume) / Sum(Volume)
            weighted_sum = sum(p * v for p, v in zip(period_prices, period_volumes))
            volume_sum = sum(period_volumes)
            
            if volume_sum > 0:
                vwma_values[i] = weighted_sum / volume_sum
            else:
                vwma_values[i] = None
        
        return vwma_values

    def update_indicators_in_csv(self, symbol: str, period: str) -> bool:
        """
        Calculate and update all technical indicators in CSV file
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            
        Returns:
            True if successful, False otherwise
        """
        csv_path = os.path.join(self.data_dir, f"{symbol}_{period}.csv")
        
        if not os.path.exists(csv_path):
            print(f"❌ CSV file not found: {csv_path}")
            return False
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_path)
            
            # Clean up any extra columns that might have been created
            expected_columns = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 
                              'ema_7', 'vwma_17', 'ema_12', 'ema_26', 'macd_line', 'macd_signal', 'roc_8']
            df = df.reindex(columns=expected_columns)
            
            if df.empty or 'close' not in df.columns or 'volume' not in df.columns:
                print(f"📊 Insufficient data for indicator calculation in {csv_path}")
                return False
            
            # Extract prices and volumes, handling any NaN values
            prices = pd.to_numeric(df['close'], errors='coerce').fillna(0).tolist()
            volumes = pd.to_numeric(df['volume'], errors='coerce').fillna(0).tolist()
            
            # Calculate all indicators
            ema_7 = self.calculate_ema(prices, period=7)
            ema_12 = self.calculate_ema(prices, period=12)
            ema_26 = self.calculate_ema(prices, period=26)
            vwma_17 = self.calculate_vwma(prices, volumes, period=17)
            macd_line, macd_signal = self.calculate_macd(prices)
            roc_8 = self.calculate_roc(prices, period=8)
            
            # Update the dataframe with proper formatting
            df['ema_7'] = [f"{val:.4f}" if val is not None else "" for val in ema_7]
            df['ema_12'] = [f"{val:.4f}" if val is not None else "" for val in ema_12]
            df['ema_26'] = [f"{val:.4f}" if val is not None else "" for val in ema_26]
            df['vwma_17'] = [f"{val:.4f}" if val is not None else "" for val in vwma_17]
            df['macd_line'] = [f"{val:.6f}" if val is not None else "" for val in macd_line]
            df['macd_signal'] = [f"{val:.6f}" if val is not None else "" for val in macd_signal]
            df['roc_8'] = [f"{val:.2f}" if val is not None else "" for val in roc_8]
            
            # Save back to CSV with proper formatting
            df.to_csv(csv_path, index=False)
            
            # Count how many indicators were calculated
            ema_7_count = sum(1 for val in ema_7 if val is not None)
            ema_12_count = sum(1 for val in ema_12 if val is not None)
            ema_26_count = sum(1 for val in ema_26 if val is not None)
            vwma_count = sum(1 for val in vwma_17 if val is not None)
            macd_count = sum(1 for val in macd_line if val is not None)
            signal_count = sum(1 for val in macd_signal if val is not None)
            roc_count = sum(1 for val in roc_8 if val is not None)
            
            print(f"📈 Updated indicators for {symbol}_{period}:")
            print(f"   EMA7: {ema_7_count}, EMA12: {ema_12_count}, EMA26: {ema_26_count}")
            print(f"   VWMA17: {vwma_count}, MACD: {macd_count}, Signal: {signal_count}, ROC8: {roc_count}")
            return True
            
        except Exception as e:
            print(f"❌ Error updating indicators for {csv_path}: {e}")
            return False

    def append_to_csv(self, symbol: str, period: str, new_candles: List[Dict]) -> bool:
        """
        Append new candle data to CSV file
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            new_candles: List of new candle data to append
            
        Returns:
            True if successful, False otherwise
        """
        if not new_candles:
            print(f"📊 No new candles to append for {symbol}_{period}")
            return True
        
        csv_path = os.path.join(self.data_dir, f"{symbol}_{period}.csv")
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Check if file exists and has headers
        file_exists = os.path.exists(csv_path)
        headers = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 
                  'ema_7', 'vwma_17', 'ema_12', 'ema_26', 'macd_line', 'macd_signal', 'roc_8']
        
        try:
            with open(csv_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write headers if file is new or empty
                if not file_exists or os.path.getsize(csv_path) == 0:
                    writer.writerow(headers)
                
                # Append new candles
                for candle in new_candles:
                    timestamp = candle.get('datetime')
                    dt = datetime.fromtimestamp(timestamp / 1000) if timestamp else None
                    
                    row = [
                        timestamp,
                        dt.strftime('%Y-%m-%d %H:%M:%S') if dt else '',
                        candle.get('open', ''),
                        candle.get('high', ''),
                        candle.get('low', ''),
                        candle.get('close', ''),
                        candle.get('volume', ''),
                        '',  # ema_7 placeholder - will be calculated
                        '',  # vwma_17 placeholder - will be calculated
                        '',  # ema_12 placeholder - will be calculated  
                        '',  # ema_26 placeholder - will be calculated
                        '',  # macd_line placeholder - will be calculated
                        '',  # macd_signal placeholder - will be calculated
                        ''   # roc_8 placeholder - will be calculated
                    ]
                    writer.writerow(row)
            
            print(f"✅ Successfully appended {len(new_candles)} candles to {csv_path}")
            
            # Calculate and update all indicators after adding new data
            print(f"📊 Calculating technical indicators for {symbol}_{period}...")
            indicator_success = self.update_indicators_in_csv(symbol, period)
            
            if not indicator_success:
                print(f"⚠️  Indicators calculation failed for {symbol}_{period}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error writing to CSV file {csv_path}: {e}")
            return False

    def get_minute_boundary_timestamp(self, timestamp_ms: int, minutes: int) -> int:
        """
        Get the boundary timestamp for aggregation periods
        
        Args:
            timestamp_ms: Timestamp in milliseconds
            minutes: Period in minutes (5 or 15)
            
        Returns:
            Boundary timestamp in milliseconds
        """
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        
        # For 5m: round down to nearest 5-minute boundary (9:30, 9:35, 9:40...)
        # For 15m: round down to nearest 15-minute boundary (9:30, 9:45, 10:00...)
        minute_boundary = (dt.minute // minutes) * minutes
        
        boundary_dt = dt.replace(minute=minute_boundary, second=0, microsecond=0)
        return int(boundary_dt.timestamp() * 1000)

    def aggregate_candles_to_period(self, symbol: str, target_period: str) -> bool:
        """
        Aggregate 1-minute candles to create 5-minute or 15-minute candles
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            target_period: Target period ('5m' or '15m')
            
        Returns:
            True if successful, False otherwise
        """
        if target_period not in ['5m', '15m']:
            print(f"❌ Unsupported aggregation period: {target_period}")
            return False
        
        period_minutes = 5 if target_period == '5m' else 15
        
        print(f"🔄 Aggregating 1m data to {target_period} for {symbol}...")
        
        # Read 1-minute data
        csv_1m_path = os.path.join(self.data_dir, f"{symbol}_1m.csv")
        if not os.path.exists(csv_1m_path):
            print(f"❌ 1-minute data file not found: {csv_1m_path}")
            return False
        
        try:
            df_1m = pd.read_csv(csv_1m_path)
            if df_1m.empty:
                print(f"📊 No 1-minute data available for aggregation")
                return False
            
            # Get last timestamp from target period file
            last_aggregated_timestamp = self.get_latest_timestamp_from_csv(symbol, target_period)
            
            # Filter 1m data to only process data that hasn't been aggregated yet
            if last_aggregated_timestamp:
                # Only process 1m data that comes AFTER the last complete aggregated period
                # We need to add the period duration to ensure we don't re-aggregate existing periods
                next_period_start = last_aggregated_timestamp + (period_minutes * 60 * 1000)
                df_1m = df_1m[df_1m['timestamp'] >= next_period_start]
                print(f"📊 Filtering 1m data from {datetime.fromtimestamp(next_period_start / 1000)} onwards")
            else:
                print(f"📊 No existing {target_period} data, aggregating all 1m data")
                
            if df_1m.empty:
                print(f"📊 No new 1m data to aggregate for {target_period}")
                return True
            
            # Convert timestamp to datetime for grouping
            df_1m['datetime_obj'] = pd.to_datetime(df_1m['timestamp'], unit='ms')
            
            # Create period boundaries for grouping
            df_1m['period_boundary'] = df_1m['timestamp'].apply(
                lambda x: self.get_minute_boundary_timestamp(x, period_minutes)
            )
            
            # Group by period boundary and aggregate
            aggregated_candles = []
            
            for boundary_ts, group in df_1m.groupby('period_boundary'):
                if len(group) == 0:
                    continue
                
                # Only create complete periods (except for the most recent one)
                current_time_ms = int(datetime.now(self.et_timezone).timestamp() * 1000)
                period_end_ms = boundary_ts + (period_minutes * 60 * 1000)
                
                # Skip incomplete periods (except if it's market close time)
                if period_end_ms > current_time_ms:
                    market_close_ms = int(datetime.now(self.et_timezone).replace(hour=16, minute=0, second=0, microsecond=0).timestamp() * 1000)
                    if current_time_ms < market_close_ms:
                        print(f"⏳ Skipping incomplete {target_period} period: {datetime.fromtimestamp(boundary_ts / 1000)}")
                        continue
                
                # OHLC aggregation
                aggregated_candle = {
                    'datetime': boundary_ts,
                    'open': group.iloc[0]['open'],  # First open
                    'high': group['high'].max(),    # Highest high
                    'low': group['low'].min(),      # Lowest low
                    'close': group.iloc[-1]['close'],  # Last close
                    'volume': group['volume'].sum()    # Sum of volumes
                }
                
                aggregated_candles.append(aggregated_candle)
                print(f"✅ Aggregated {target_period} period: {datetime.fromtimestamp(boundary_ts / 1000)} ({len(group)} 1m candles)")
            
            if aggregated_candles:
                # Sort by timestamp
                aggregated_candles.sort(key=lambda x: x['datetime'])
                
                # Append to target period CSV
                success = self.append_to_csv(symbol, target_period, aggregated_candles)
                
                if success:
                    print(f"✅ Aggregated {len(aggregated_candles)} new {target_period} candles for {symbol}")
                else:
                    print(f"❌ Failed to save {target_period} candles for {symbol}")
                    
                return success
            else:
                print(f"📊 No complete {target_period} periods to aggregate")
                return True
                
        except Exception as e:
            print(f"❌ Error aggregating to {target_period}: {e}")
            return False

    def update_market_data_with_aggregation(self, symbol: str, target_date: datetime.date = None) -> bool:
        """
        Complete workflow: fetch 1m data, then aggregate to 5m and 15m
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            target_date: Date to fetch data for (defaults to today)
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n🚀 Starting complete market data update for {symbol}")
        print("=" * 60)
        
        # Step 1: Update 1-minute data
        print("📊 Step 1: Updating 1-minute data...")
        success_1m = self.update_market_data(symbol, '1m', target_date)
        
        if not success_1m:
            print("❌ Failed to update 1-minute data, skipping aggregation")
            return False
        
        # Step 2: Aggregate to 5-minute data
        print("\n📊 Step 2: Aggregating to 5-minute data...")
        success_5m = self.aggregate_candles_to_period(symbol, '5m')
        
        # Step 3: Aggregate to 15-minute data
        print("\n📊 Step 3: Aggregating to 15-minute data...")
        success_15m = self.aggregate_candles_to_period(symbol, '15m')
        
        # Summary
        print(f"\n📈 Market Data Update Summary for {symbol}:")
        print(f"   1m data: {'✅ Success' if success_1m else '❌ Failed'}")
        print(f"   5m data: {'✅ Success' if success_5m else '❌ Failed'}")
        print(f"   15m data: {'✅ Success' if success_15m else '❌ Failed'}")
        
        overall_success = success_1m and success_5m and success_15m
        
        if overall_success:
            print(f"🎉 Complete market data update successful for {symbol}!")
        else:
            print(f"⚠️  Partial success - some timeframes may have failed")
        
        return overall_success

    def update_market_data(self, symbol: str, period: str = '1m', target_date: datetime.date = None) -> bool:
        """
        Update market data for a specific symbol and period (used internally)
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            target_date: Date to fetch data for (defaults to today)
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n🚀 Starting market data update for {symbol}_{period}")
        print("=" * 50)
        
        # Step 1: Get latest timestamp from CSV
        last_timestamp = self.get_latest_timestamp_from_csv(symbol, period)
        
        # Step 2: Get market hours in epoch milliseconds
        start_time_ms, end_time_ms = self.convert_et_to_epoch_ms(target_date)
        
        # Step 3: Retrieve price history from Schwab API
        candles = self.get_price_history_from_schwab(symbol, start_time_ms, end_time_ms)
        if candles is None:
            print("❌ Failed to retrieve price history")
            return False
        
        # Step 4: Filter to only new data
        new_candles = self.filter_new_data(candles, last_timestamp)
        
        # Step 5: Append new data to CSV
        success = self.append_to_csv(symbol, period, new_candles)
        
        if success:
            print(f"✅ Market data update completed for {symbol}_{period}")
        else:
            print(f"❌ Market data update failed for {symbol}_{period}")
        
        return success

    def load_email_config(self) -> Dict:
        """Load email configuration from email_credentials.env file"""
        email_config = {
            'enabled': False,
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'sender': '',
            'password': '',
            'recipients': []
        }
        
        credentials_file = 'email_credentials.env'
        
        if os.path.exists(credentials_file):
            try:
                with open(credentials_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            if key == 'EMAIL_ALERTS_ENABLED':
                                email_config['enabled'] = value.lower() in ['true', '1', 'yes']
                            elif key == 'EMAIL_SENDER':
                                email_config['sender'] = value
                            elif key == 'EMAIL_PASSWORD':
                                email_config['password'] = value
                            elif key == 'EMAIL_TO':
                                # Handle comma-delimited recipients
                                recipients = [email.strip() for email in value.split(',') if email.strip()]
                                email_config['recipients'] = recipients
                
                # Validate configuration
                if (email_config['enabled'] and 
                    email_config['sender'] and 
                    email_config['password'] and 
                    email_config['recipients']):
                    print(f"📧 Email notifications enabled")
                    print(f"   Sender: {email_config['sender']}")
                    print(f"   Recipients: {', '.join(email_config['recipients'])}")
                else:
                    email_config['enabled'] = False
                    print("📧 Email notifications disabled (missing or invalid configuration)")
                    
            except Exception as e:
                print(f"⚠️  Error loading email configuration: {e}")
                email_config['enabled'] = False
        else:
            print("📧 Email notifications disabled (email_credentials.env not found)")
        
        return email_config

    def send_email_notification(self, subject: str, message: str) -> bool:
        """Send email notification for position changes to multiple recipients"""
        if not self.email_config['enabled']:
            print(f"📧 Email disabled - would send: {subject}")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender']
            msg['To'] = ', '.join(self.email_config['recipients'])
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['sender'], self.email_config['password'])
            
            # Send to all recipients
            server.send_message(msg, to_addrs=self.email_config['recipients'])
            server.quit()
            
            recipients_str = ', '.join(self.email_config['recipients'])
            print(f"📧 Email sent to {len(self.email_config['recipients'])} recipients: {subject}")
            print(f"   Recipients: {recipients_str}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            return False

    def check_trading_signals(self, symbol: str, period: str, df: pd.DataFrame) -> Tuple[bool, bool, Dict]:
        """
        Check trading signals for position management
        
        Args:
            symbol: Stock symbol
            period: Time period ('1m', '5m', '15m')
            df: DataFrame with OHLC and indicator data
            
        Returns:
            Tuple of (should_open, should_close, signal_details)
        """
        if df.empty or len(df) < 2:
            return False, False, {}
        
        # Get the latest row with non-empty indicators
        latest_row = None
        for i in range(len(df) - 1, -1, -1):
            row = df.iloc[i]
            if (pd.notna(row.get('ema_7')) and row.get('ema_7') != '' and
                pd.notna(row.get('vwma_17')) and row.get('vwma_17') != '' and
                pd.notna(row.get('macd_line')) and row.get('macd_line') != '' and
                pd.notna(row.get('macd_signal')) and row.get('macd_signal') != '' and
                pd.notna(row.get('roc_8')) and row.get('roc_8') != ''):
                latest_row = row
                break
        
        if latest_row is None:
            return False, False, {}
        
        try:
            # Convert indicator values to float
            ema_7 = float(latest_row['ema_7'])
            vwma_17 = float(latest_row['vwma_17'])
            macd_line = float(latest_row['macd_line'])
            macd_signal = float(latest_row['macd_signal'])
            roc_8 = float(latest_row['roc_8'])
            
            # Check the 3 conditions
            condition_1 = ema_7 > vwma_17  # 7 EMA > 17 VWMA
            condition_2 = macd_line > macd_signal  # MACD Line > MACD Signal
            condition_3 = roc_8 > 0  # ROC > 0
            
            conditions_met = sum([condition_1, condition_2, condition_3])
            
            signal_details = {
                'timestamp': latest_row['timestamp'],
                'datetime': latest_row['datetime'],
                'close': latest_row['close'],
                'ema_7': ema_7,
                'vwma_17': vwma_17,
                'macd_line': macd_line,
                'macd_signal': macd_signal,
                'roc_8': roc_8,
                'condition_1_ema_vwma': condition_1,
                'condition_2_macd': condition_2,
                'condition_3_roc': condition_3,
                'conditions_met': conditions_met
            }
            
            current_position = self.positions[period]
            
            # Logic for opening/closing positions
            should_open = (current_position == 'CLOSED' and conditions_met == 3)
            should_close = (current_position == 'OPENED' and conditions_met <= 1)  # Any 2 conditions fail = 1 or 0 met
            
            return should_open, should_close, signal_details
            
        except (ValueError, TypeError, KeyError) as e:
            print(f"⚠️  Error processing signals for {symbol}_{period}: {e}")
            return False, False, {}

    def process_position_change(self, symbol: str, period: str, action: str, signal_details: Dict) -> bool:
        """
        Process position changes and send email notifications
        
        Args:
            symbol: Stock symbol
            period: Time period
            action: 'OPEN' or 'CLOSE'
            signal_details: Dictionary with signal information
            
        Returns:
            True if position changed, False otherwise
        """
        if action not in ['OPEN', 'CLOSE']:
            return False
        
        old_position = self.positions[period]
        new_position = 'OPENED' if action == 'OPEN' else 'CLOSED'
        
        # Only proceed if position actually changes
        if old_position == new_position:
            return False
        
        # Update position
        self.positions[period] = new_position
        
        # Format signal details for email
        dt = signal_details.get('datetime', 'Unknown')
        price = signal_details.get('close', 'Unknown')
        conditions = signal_details.get('conditions_met', 0)
        
        # P&L calculation for closing positions
        pnl_info = ""
        if action == 'OPEN':
            # Store opening price
            self.opening_prices[period] = float(price) if price != 'Unknown' else None
            pnl_info = f"Opening position at ${price}"
        elif action == 'CLOSE' and self.opening_prices[period] is not None:
            # Calculate P&L
            opening_price = self.opening_prices[period]
            closing_price = float(price) if price != 'Unknown' else None
            
            if closing_price is not None:
                pnl = closing_price - opening_price
                pnl_percent = (pnl / opening_price) * 100 if opening_price != 0 else 0
                
                pnl_symbol = "📈" if pnl >= 0 else "📉"
                pnl_info = f"""
P&L Analysis:
- Opening Price: ${opening_price:.4f}
- Closing Price: ${closing_price:.4f}
- Profit/Loss: {pnl_symbol} ${pnl:.4f} ({pnl_percent:+.2f}%)"""
                
                # Reset opening price after closing
                self.opening_prices[period] = None
            else:
                pnl_info = "P&L calculation unavailable (invalid closing price)"
        elif action == 'CLOSE':
            pnl_info = "P&L calculation unavailable (no opening price recorded)"
        
        # Create detailed signal info
        signal_info = f"""
Position Change Details:
- Symbol: {symbol}
- Timeframe: {period}
- Action: {action} POSITION
- Time: {dt}
- Price: ${price}
- Conditions Met: {conditions}/3

{pnl_info}

Technical Indicators:
- 7 EMA: {signal_details.get('ema_7', 'N/A'):.4f}
- 17 VWMA: {signal_details.get('vwma_17', 'N/A'):.4f}
- MACD Line: {signal_details.get('macd_line', 'N/A'):.6f}
- MACD Signal: {signal_details.get('macd_signal', 'N/A'):.6f}
- ROC-8: {signal_details.get('roc_8', 'N/A'):.2f}%

Condition Status:
- ✅ EMA > VWMA: {signal_details.get('condition_1_ema_vwma', False)}
- ✅ MACD > Signal: {signal_details.get('condition_2_macd', False)} 
- ✅ ROC > 0: {signal_details.get('condition_3_roc', False)}

Current Positions Status:
- 1m: {self.positions['1m']}
- 5m: {self.positions['5m']}
- 15m: {self.positions['15m']}
"""

        # Email subject with P&L for closes
        subject = f"🚨 {symbol} {period} - {action} POSITION at ${price}"
        
        # Add P&L to subject for closes when available
        if action == 'CLOSE' and 'Profit/Loss:' in pnl_info:
            try:
                # Extract P&L amount from pnl_info
                for line in pnl_info.split('\n'):
                    if 'Profit/Loss:' in line:
                        # Extract the P&L value and symbol
                        if '📈' in line:
                            pnl_match = line.split('$')[1].split(' ')[0]
                            subject = f"🚨 {symbol} {period} - {action} POSITION at ${price} 📈${pnl_match}"
                        elif '📉' in line:
                            pnl_match = line.split('$')[1].split(' ')[0]
                            subject = f"🚨 {symbol} {period} - {action} POSITION at ${price} 📉${pnl_match}"
                        break
            except:
                pass  # Keep original subject if parsing fails
        
        # Console output with P&L
        print(f"\n🚨 POSITION CHANGE: {symbol}_{period}")
        print(f"   Action: {action} POSITION")
        print(f"   Time: {dt}")
        print(f"   Price: ${price}")
        print(f"   Conditions: {conditions}/3")
        print(f"   EMA>VWMA: {signal_details.get('condition_1_ema_vwma', False)}")
        print(f"   MACD>Sig: {signal_details.get('condition_2_macd', False)}")
        print(f"   ROC>0: {signal_details.get('condition_3_roc', False)}")
        
        # Show P&L in console for closes
        if action == 'CLOSE' and 'Profit/Loss:' in pnl_info:
            pnl_lines = pnl_info.split('\n')
            for line in pnl_lines:
                if 'Profit/Loss:' in line:
                    print(f"   {line.strip()}")
                    break
        
        # Send email notification
        self.send_email_notification(subject, signal_info)
        
        return True

    def analyze_historical_positions(self, symbol: str) -> Dict:
        """
        Analyze all historical data to track position changes retrospectively
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Dictionary with analysis results
        """
        print(f"\n📊 Analyzing Historical Positions for {symbol}")
        print("=" * 60)
        
        results = {
            'total_signals': 0,
            'open_signals': 0,
            'close_signals': 0,
            'timeframes': {}
        }
        
        for period in ['1m', '5m', '15m']:
            print(f"\n🔍 Analyzing {period} timeframe...")
            
            csv_path = os.path.join(self.data_dir, f"{symbol}_{period}.csv")
            if not os.path.exists(csv_path):
                print(f"   ❌ No data file found: {csv_path}")
                continue
            
            try:
                df = pd.read_csv(csv_path)
                if df.empty:
                    print(f"   📊 No data in {csv_path}")
                    continue
                
                # Reset position for this timeframe analysis
                self.positions[period] = 'CLOSED'
                
                # Reset opening price for this timeframe
                self.opening_prices[period] = None
                
                timeframe_results = {
                    'open_signals': [],
                    'close_signals': [],
                    'total_opens': 0,
                    'total_closes': 0
                }
                
                # Analyze each row chronologically
                for i in range(len(df)):
                    row_df = df.iloc[:i+1]  # All data up to current row
                    
                    should_open, should_close, signal_details = self.check_trading_signals(symbol, period, row_df)
                    
                    if should_open:
                        success = self.process_position_change(symbol, period, 'OPEN', signal_details)
                        if success:
                            timeframe_results['open_signals'].append(signal_details)
                            timeframe_results['total_opens'] += 1
                            results['open_signals'] += 1
                            results['total_signals'] += 1
                    
                    elif should_close:
                        success = self.process_position_change(symbol, period, 'CLOSE', signal_details)
                        if success:
                            timeframe_results['close_signals'].append(signal_details)
                            timeframe_results['total_closes'] += 1
                            results['close_signals'] += 1
                            results['total_signals'] += 1
                
                results['timeframes'][period] = timeframe_results
                
                print(f"   📈 {period} Analysis Complete:")
                print(f"      Opens: {timeframe_results['total_opens']}")
                print(f"      Closes: {timeframe_results['total_closes']}")
                print(f"      Final Position: {self.positions[period]}")
                
            except Exception as e:
                print(f"   ❌ Error analyzing {period}: {e}")
        
        print(f"\n📊 Historical Analysis Summary:")
        print(f"   Total Signals: {results['total_signals']}")
        print(f"   Open Signals: {results['open_signals']}")
        print(f"   Close Signals: {results['close_signals']}")
        print(f"\n📊 Final Position States:")
        for period in ['1m', '5m', '15m']:
            print(f"   {period}: {self.positions[period]}")
        
        return results

    def check_live_position_signals(self, symbol: str) -> bool:
        """
        Check for position signals in latest data during continuous mode
        
        Args:
            symbol: Stock symbol to check
            
        Returns:
            True if any signals were processed, False otherwise
        """
        signals_found = False
        
        for period in ['1m', '5m', '15m']:
            csv_path = os.path.join(self.data_dir, f"{symbol}_{period}.csv")
            if not os.path.exists(csv_path):
                continue
            
            try:
                df = pd.read_csv(csv_path)
                if df.empty:
                    continue
                
                should_open, should_close, signal_details = self.check_trading_signals(symbol, period, df)
                
                if should_open:
                    success = self.process_position_change(symbol, period, 'OPEN', signal_details)
                    if success:
                        signals_found = True
                
                elif should_close:
                    success = self.process_position_change(symbol, period, 'CLOSE', signal_details)
                    if success:
                        signals_found = True
                        
            except Exception as e:
                print(f"⚠️  Error checking live signals for {symbol}_{period}: {e}")
        
        return signals_found


def main():
    """Main function with options for single run, continuous collection, or position analysis"""
    updater = MarketDataUpdater()
    
    print("🚀 Schwab Market Data Updater with Position Tracking")
    print("=" * 60)
    
    # Check command line arguments
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--single":
            # Single run mode (original behavior)
            symbol = sys.argv[2] if len(sys.argv) > 2 else "SPY"
            print(f"📊 Running single update for {symbol}")
            
            success = updater.update_market_data_with_aggregation(symbol)
            
            if success:
                print(f"\n🎉 Successfully updated all timeframes for {symbol}!")
                
                # Run position analysis after single update
                print(f"\n🎯 Running position analysis...")
                try:
                    results = updater.analyze_historical_positions(symbol)
                    print(f"✅ Position analysis completed - {results['total_signals']} signals found")
                except Exception as e:
                    print(f"⚠️  Position analysis failed: {e}")
            else:
                print(f"\n❌ Some issues occurred during update for {symbol}")
                
        elif sys.argv[1] == "--analyze":
            # Historical position analysis only
            symbol = sys.argv[2] if len(sys.argv) > 2 else "SPY"
            print(f"🎯 Running historical position analysis for {symbol}")
            
            try:
                results = updater.analyze_historical_positions(symbol)
                print(f"\n🎉 Position analysis completed!")
                print(f"   Total signals: {results['total_signals']}")
                print(f"   Opens: {results['open_signals']}")
                print(f"   Closes: {results['close_signals']}")
            except Exception as e:
                print(f"\n❌ Position analysis failed: {e}")
                
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python market_data_updater.py                     # Continuous mode with position tracking")
            print("  python market_data_updater.py --single [SYMBOL]   # Single run + position analysis")
            print("  python market_data_updater.py --analyze [SYMBOL]  # Historical position analysis only")
            print("  python market_data_updater.py --help             # Show this help")
            print()
            print("Position Tracking Features:")
            print("  • Opens when: 7EMA > 17VWMA AND MACD > Signal AND ROC > 0")
            print("  • Closes when: Any 2 of the 3 conditions fail")
            print("  • Email notifications for all position changes")
            print("  • Tracks 1m, 5m, and 15m timeframes independently")
            print()
            print("Email Configuration (create email_credentials.env):")
            print("  EMAIL_ALERTS_ENABLED=true")
            print("  EMAIL_SENDER=your_email@gmail.com")
            print("  EMAIL_PASSWORD=your_app_password")
            print("  EMAIL_TO=recipient1@gmail.com, recipient2@gmail.com")
            print()
            print("Gmail Setup Notes:")
            print("  • Use App Password instead of regular password")
            print("  • Go to Google Account > Security > 2-Step Verification > App passwords")
            print("  • Generate app password for 'Mail' and use as EMAIL_PASSWORD")
            print("  • Multiple recipients: separate with commas in EMAIL_TO")
            print()
            print("Default symbol: SPY")
            
        else:
            symbol = sys.argv[1]
            print(f"📊 Running continuous collection with position tracking for {symbol}")
            updater.run_continuous_data_collection(symbol)
    else:
        # Default: Continuous mode
        symbol = "SPY"
        print(f"📊 Running continuous collection with position tracking for {symbol}")
        print("   Use --single for one-time update + analysis")
        print("   Use --analyze for historical analysis only")
        print("   Use --help for usage information")
        print()
        updater.run_continuous_data_collection(symbol)


if __name__ == "__main__":
    main() 