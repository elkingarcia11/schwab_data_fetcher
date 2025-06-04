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

class MarketDataUpdater:
    def __init__(self):
        self.data_dir = "data"
        self.et_timezone = pytz.timezone('US/Eastern')
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET
        self.last_token_refresh = None
        self.token_refresh_interval = 20 * 60  # 20 minutes in seconds
        self.running = True
        
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
        Calculate and update EMA and VWMA indicators in CSV file
        
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
            expected_columns = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'ema_7', 'vwma_17']
            df = df.reindex(columns=expected_columns)
            
            if df.empty or 'close' not in df.columns or 'volume' not in df.columns:
                print(f"📊 Insufficient data for indicator calculation in {csv_path}")
                return False
            
            # Extract prices and volumes, handling any NaN values
            prices = pd.to_numeric(df['close'], errors='coerce').fillna(0).tolist()
            volumes = pd.to_numeric(df['volume'], errors='coerce').fillna(0).tolist()
            
            # Calculate indicators
            ema_7 = self.calculate_ema(prices, period=7)
            vwma_17 = self.calculate_vwma(prices, volumes, period=17)
            
            # Update the dataframe with proper formatting
            df['ema_7'] = [f"{val:.4f}" if val is not None else "" for val in ema_7]
            df['vwma_17'] = [f"{val:.4f}" if val is not None else "" for val in vwma_17]
            
            # Save back to CSV with proper formatting
            df.to_csv(csv_path, index=False)
            
            # Count how many indicators were calculated
            ema_count = sum(1 for val in ema_7 if val is not None)
            vwma_count = sum(1 for val in vwma_17 if val is not None)
            
            print(f"📈 Updated indicators for {symbol}_{period}: {ema_count} EMA values, {vwma_count} VWMA values")
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
        headers = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'ema_7', 'vwma_17']
        
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
                        ''   # vwma_17 placeholder - will be calculated
                    ]
                    writer.writerow(row)
            
            print(f"✅ Successfully appended {len(new_candles)} candles to {csv_path}")
            
            # Calculate and update indicators after adding new data
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


def main():
    """Main function with options for single run or continuous collection"""
    updater = MarketDataUpdater()
    
    print("🚀 Schwab Market Data Updater")
    print("=" * 50)
    
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
            else:
                print(f"\n❌ Some issues occurred during update for {symbol}")
                
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python market_data_updater.py                    # Continuous mode (default)")
            print("  python market_data_updater.py --single [SYMBOL]  # Single run mode")
            print("  python market_data_updater.py --help            # Show this help")
            print("\nDefault symbol: SPY")
            
        else:
            symbol = sys.argv[1]
            print(f"📊 Running continuous collection for {symbol}")
            updater.run_continuous_data_collection(symbol)
    else:
        # Default: Continuous mode
        symbol = "SPY"
        print(f"📊 Running continuous collection for {symbol}")
        print("   Use --single for one-time update")
        print("   Use --help for usage information")
        print()
        updater.run_continuous_data_collection(symbol)


if __name__ == "__main__":
    main() 