#!/usr/bin/env python3
"""
Data Fetcher Module
Handles all data retrieval, CSV operations, and timestamp management
"""

import os
import csv
import requests
import pandas as pd
from datetime import datetime, timezone, time
import pytz
from typing import Optional, Dict, List, Tuple
from schwab_auth import SchwabAuth

class DataFetcher:
    def __init__(self):
        self.data_dir = "data"
        self.et_timezone = pytz.timezone('US/Eastern')
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET
        self.schwab_auth = SchwabAuth()
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
    
    def get_csv_path(self, symbol: str, period: str, inverse: bool = False) -> str:
        """
        Get the CSV file path for a symbol and period
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            inverse: Whether to get inverse price file path
            
        Returns:
            Full path to CSV file
        """
        if inverse:
            filename = f"{symbol}_{period}_INVERSE.csv"
        else:
            filename = f"{symbol}_{period}.csv"
        return os.path.join(self.data_dir, filename)
    
    def get_latest_timestamp_from_csv(self, symbol: str, period: str, inverse: bool = False) -> Optional[int]:
        """
        Get the latest timestamp from a ticker CSV file
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            inverse: Whether to check inverse price file
            
        Returns:
            Latest timestamp in milliseconds, or None if file is empty/doesn't exist
        """
        csv_path = self.get_csv_path(symbol, period, inverse)
        
        if not os.path.exists(csv_path):
            file_type = "INVERSE" if inverse else "regular"
            print(f"📁 {file_type} CSV file {csv_path} does not exist")
            return None
        
        try:
            df = pd.read_csv(csv_path)
            
            if df.empty or 'timestamp' not in df.columns:
                file_type = "INVERSE" if inverse else "regular"
                print(f"📊 {file_type} CSV file {csv_path} is empty or missing timestamp column")
                return None
            
            # Remove any empty rows
            df = df.dropna(subset=['timestamp'])
            
            if df.empty:
                file_type = "INVERSE" if inverse else "regular"
                print(f"📊 No valid timestamps found in {file_type} {csv_path}")
                return None
            
            latest_timestamp = int(df['timestamp'].max())
            latest_datetime = datetime.fromtimestamp(latest_timestamp / 1000)
            
            file_type = "INVERSE" if inverse else "regular"
            print(f"📅 Latest timestamp in {file_type} {csv_path}: {latest_timestamp} ({latest_datetime})")
            return latest_timestamp
            
        except Exception as e:
            file_type = "INVERSE" if inverse else "regular"
            print(f"❌ Error reading {file_type} CSV file {csv_path}: {e}")
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
        headers = self.schwab_auth.get_auth_headers()
        if not headers:
            print("❌ No valid authentication available")
            return None
        
        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
        
        params = {
            'symbol': symbol,
            'periodType': 'day',
            'period': 2,
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

    def calculate_inverse_candles(self, candles: List[Dict]) -> List[Dict]:
        """
        Calculate inverse price candles (1/price) from regular candles
        
        Args:
            candles: List of regular candle data from API
            
        Returns:
            List of inverse candle data
        """
        inverse_candles = []
        
        for candle in candles:
            try:
                # Extract OHLC values
                open_price = float(candle.get('open', 0))
                high_price = float(candle.get('high', 0))
                low_price = float(candle.get('low', 0))
                close_price = float(candle.get('close', 0))
                
                # Skip candles with zero or invalid prices
                if any(price <= 0 for price in [open_price, high_price, low_price, close_price]):
                    continue
                
                # Calculate inverse prices (1/price)
                # For inverse: high becomes low, low becomes high
                inverse_open = 1.0 / open_price
                inverse_high = 1.0 / low_price   # Inverse of low becomes high
                inverse_low = 1.0 / high_price   # Inverse of high becomes low
                inverse_close = 1.0 / close_price
                
                # Create inverse candle
                inverse_candle = {
                    'datetime': candle.get('datetime'),
                    'open': inverse_open,
                    'high': inverse_high,
                    'low': inverse_low,
                    'close': inverse_close,
                    'volume': candle.get('volume', 0)  # Volume stays the same
                }
                
                inverse_candles.append(inverse_candle)
                
            except (ValueError, TypeError, ZeroDivisionError) as e:
                print(f"⚠️  Error calculating inverse for candle: {e}")
                continue
        
        print(f"🔄 Calculated {len(inverse_candles)} inverse candles from {len(candles)} regular candles")
        return inverse_candles

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

    def append_to_csv(self, symbol: str, period: str, new_candles: List[Dict], inverse: bool = False) -> bool:
        """
        Append new candle data to CSV file (without indicators - those are calculated separately)
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            new_candles: List of new candle data to append
            inverse: Whether to save to inverse price file
            
        Returns:
            True if successful, False otherwise
        """
        if not new_candles:
            file_type = "INVERSE" if inverse else "regular"
            print(f"📊 No new candles to append for {file_type} {symbol}_{period}")
            return True
        
        csv_path = self.get_csv_path(symbol, period, inverse)
        
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
                        '',  # ema_7 placeholder - calculated by indicator_calculator
                        '',  # vwma_17 placeholder - calculated by indicator_calculator
                        '',  # ema_12 placeholder - calculated by indicator_calculator  
                        '',  # ema_26 placeholder - calculated by indicator_calculator
                        '',  # macd_line placeholder - calculated by indicator_calculator
                        '',  # macd_signal placeholder - calculated by indicator_calculator
                        ''   # roc_8 placeholder - calculated by indicator_calculator
                    ]
                    writer.writerow(row)
            
            file_type = "INVERSE" if inverse else "regular"
            print(f"✅ Successfully appended {len(new_candles)} candles to {file_type} {csv_path}")
            return True
            
        except Exception as e:
            file_type = "INVERSE" if inverse else "regular"
            print(f"❌ Error writing to {file_type} CSV file {csv_path}: {e}")
            return False

    def load_csv_data(self, symbol: str, period: str, inverse: bool = False) -> Optional[pd.DataFrame]:
        """
        Load CSV data into a DataFrame
        
        Args:
            symbol: Stock symbol
            period: Time period
            inverse: Whether to load inverse price file
            
        Returns:
            DataFrame with CSV data, or None if error
        """
        csv_path = self.get_csv_path(symbol, period, inverse)
        
        if not os.path.exists(csv_path):
            file_type = "INVERSE" if inverse else "regular"
            print(f"❌ {file_type} CSV file not found: {csv_path}")
            return None
        
        try:
            df = pd.read_csv(csv_path)
            file_type = "INVERSE" if inverse else "regular"
            print(f"📊 Loaded {len(df)} rows from {file_type} {csv_path}")
            return df
        except Exception as e:
            file_type = "INVERSE" if inverse else "regular"
            print(f"❌ Error loading {file_type} CSV file {csv_path}: {e}")
            return None

    def save_csv_data(self, symbol: str, period: str, df: pd.DataFrame, inverse: bool = False) -> bool:
        """
        Save DataFrame to CSV file
        
        Args:
            symbol: Stock symbol
            period: Time period
            df: DataFrame to save
            inverse: Whether to save to inverse price file
            
        Returns:
            True if successful, False otherwise
        """
        csv_path = self.get_csv_path(symbol, period, inverse)
        
        try:
            df.to_csv(csv_path, index=False)
            file_type = "INVERSE" if inverse else "regular"
            print(f"✅ Saved {len(df)} rows to {file_type} {csv_path}")
            return True
        except Exception as e:
            file_type = "INVERSE" if inverse else "regular"
            print(f"❌ Error saving {file_type} CSV file {csv_path}: {e}")
            return False

    def fetch_new_data(self, symbol: str, period: str = '1m', target_date: datetime.date = None) -> bool:
        """
        Complete workflow to fetch new data for a symbol and period (both regular and inverse)
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (only '1m' supported for direct API fetch)
            target_date: Date to fetch data for (defaults to today)
            
        Returns:
            True if successful, False otherwise
        """
        if period != '1m':
            print(f"❌ Direct API fetch only supports 1m period, got {period}")
            return False
        
        print(f"\n📡 Fetching new data for {symbol}_{period} (regular + inverse)")
        print("=" * 60)
        
        # Step 1: Get latest timestamp from both regular and inverse CSV files
        last_timestamp_regular = self.get_latest_timestamp_from_csv(symbol, period, inverse=False)
        last_timestamp_inverse = self.get_latest_timestamp_from_csv(symbol, period, inverse=True)
        
        # Use the latest timestamp from either file to ensure sync
        last_timestamp = None
        if last_timestamp_regular and last_timestamp_inverse:
            last_timestamp = max(last_timestamp_regular, last_timestamp_inverse)
        elif last_timestamp_regular:
            last_timestamp = last_timestamp_regular
        elif last_timestamp_inverse:
            last_timestamp = last_timestamp_inverse
        
        print(f"📊 Using latest timestamp: {last_timestamp}")
        
        # Step 2: Get market hours in epoch milliseconds
        start_time_ms, end_time_ms = self.convert_et_to_epoch_ms(target_date)
        
        # Step 3: Retrieve price history from Schwab API
        candles = self.get_price_history_from_schwab(symbol, start_time_ms, end_time_ms)
        if candles is None:
            print("❌ Failed to retrieve price history")
            return False
        
        # Step 4: Filter to only new data
        new_candles = self.filter_new_data(candles, last_timestamp)
        
        if not new_candles:
            print("📊 No new data to process")
            return True
        
        # Step 5: Calculate inverse candles
        inverse_candles = self.calculate_inverse_candles(new_candles)
        
        # Step 6: Append new data to both regular and inverse CSV files
        success_regular = self.append_to_csv(symbol, period, new_candles, inverse=False)
        success_inverse = self.append_to_csv(symbol, period, inverse_candles, inverse=True)
        
        overall_success = success_regular and success_inverse
        
        if overall_success:
            print(f"✅ Data fetch completed for {symbol}_{period} (regular + inverse)")
        else:
            print(f"❌ Data fetch failed for {symbol}_{period}")
            if not success_regular:
                print("   - Regular data save failed")
            if not success_inverse:
                print("   - Inverse data save failed")
        
        return overall_success

    def fetch_data_at_frequency(self, symbol: str, frequency: str, target_date: datetime.date = None) -> bool:
        """
        Fetch market data at a specific frequency directly from Schwab API
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            frequency: Data frequency ('1m', '5m', '10m', '15m', '30m')
            target_date: Date to fetch data for (defaults to today)
            
        Returns:
            True if successful, False otherwise
        """
        # Validate frequency
        valid_frequencies = ['1m', '5m', '10m', '15m', '30m']
        if frequency not in valid_frequencies:
            print(f"❌ Invalid frequency: {frequency}. Must be one of {valid_frequencies}")
            return False
        
        # Convert frequency to API parameters
        frequency_map = {
            '1m': {'frequencyType': 'minute', 'frequency': 1},
            '5m': {'frequencyType': 'minute', 'frequency': 5},
            '10m': {'frequencyType': 'minute', 'frequency': 10},
            '15m': {'frequencyType': 'minute', 'frequency': 15},
            '30m': {'frequencyType': 'minute', 'frequency': 30}
        }
        
        freq_params = frequency_map[frequency]
        
        print(f"\n📡 Fetching {frequency} data for {symbol} (regular + inverse)")
        print("=" * 60)
        
        # Get latest timestamp from existing file
        last_timestamp = self.get_latest_timestamp_from_csv(symbol, frequency)
        print(f"📊 Using latest timestamp: {datetime.fromtimestamp(last_timestamp / 1000) if last_timestamp else 'None'}")
        
        # Get market hours in epoch milliseconds
        start_time_ms, end_time_ms = self.convert_et_to_epoch_ms(target_date)
        
        # If we have existing data, fetch only from the last timestamp
        if last_timestamp:
            start_time_ms = last_timestamp + (60 * 1000)  # Start 1 minute after last data
        
        # Retrieve price history from Schwab API
        headers = self.schwab_auth.get_auth_headers()
        if not headers:
            print("❌ No valid authentication available")
            return False
        
        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
        
        params = {
            'symbol': symbol,
            'periodType': 'day',
            'period': 2,
            'frequencyType': freq_params['frequencyType'],
            'frequency': freq_params['frequency'],
            'startDate': start_time_ms,
            'endDate': end_time_ms,
            'needExtendedHoursData': 'false',
            'needPreviousClose': 'false'
        }
        
        print(f"📡 Fetching {frequency} price history for {symbol} from Schwab API...")
        print(f"   URL: {url}")
        print(f"   Params: {params}")
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'candles' in data and data['candles']:
                    candles = data['candles']
                    print(f"✅ Retrieved {len(candles)} {frequency} candles from Schwab API")
                    
                    # Filter out any incomplete candles and duplicates
                    new_candles = self.filter_new_data_for_frequency(candles, last_timestamp, frequency)
                    
                    if new_candles:
                        # Calculate inverse data
                        inverse_candles = self.calculate_inverse_candles(new_candles)
                        print(f"🔄 Calculated {len(inverse_candles)} inverse candles from {len(new_candles)} regular candles")
                        
                        # Save regular data
                        regular_success = self.append_to_csv(symbol, frequency, new_candles, inverse=False)
                        
                        # Save inverse data
                        inverse_success = self.append_to_csv(symbol, frequency, inverse_candles, inverse=True)
                        
                        if regular_success and inverse_success:
                            print(f"✅ Data fetch completed for {symbol}_{frequency} (regular + inverse)")
                            return True
                        else:
                            print(f"❌ Failed to save data for {symbol}_{frequency}")
                            return False
                    else:
                        print(f"📊 No new {frequency} data to save")
                        return True
                else:
                    print(f"📊 No {frequency} candle data found in API response")
                    return True
            else:
                print(f"❌ API request failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error fetching {frequency} price history: {e}")
            return False

    def filter_new_data_for_frequency(self, candles: List[Dict], last_timestamp_ms: Optional[int], frequency: str) -> List[Dict]:
        """
        Filter candles for specific frequency to only include new, complete data
        
        Args:
            candles: List of candle data from API
            last_timestamp_ms: Last timestamp in CSV (in milliseconds)
            frequency: Data frequency (e.g., '5m', '15m')
            
        Returns:
            Filtered list of new, complete candles
        """
        if not candles:
            return []
        
        # Calculate frequency duration in milliseconds
        frequency_minutes = int(frequency.replace('m', ''))
        frequency_ms = frequency_minutes * 60 * 1000
        
        # Get current time and calculate the current period boundary
        current_time = datetime.now(self.et_timezone)
        current_period_start = self.get_period_boundary(current_time, frequency_minutes)
        current_period_start_ms = int(current_period_start.timestamp() * 1000)
        
        print(f"🕐 Current time: {current_time.strftime('%H:%M:%S')} ET")
        print(f"🕐 Current {frequency} period starts: {current_period_start.strftime('%H:%M:%S')} ({current_period_start_ms})")
        
        # Filter out the current forming period
        completed_candles = []
        for candle in candles:
            candle_timestamp = candle.get('datetime')
            if candle_timestamp and candle_timestamp < current_period_start_ms:
                completed_candles.append(candle)
        
        print(f"🔍 Filtered out current forming {frequency} period: {len(candles)} → {len(completed_candles)} completed candles")
        
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
        
        print(f"🔍 Filtered {len(new_candles)} new completed {frequency} candles from {len(completed_candles)} total completed candles")
        print(f"   Last recorded: {datetime.fromtimestamp(last_timestamp_ms / 1000) if last_timestamp_ms else 'None'}")
        
        if new_candles:
            first_new = datetime.fromtimestamp(new_candles[0]['datetime'] / 1000)
            last_new = datetime.fromtimestamp(new_candles[-1]['datetime'] / 1000)
            print(f"   New data range: {first_new} to {last_new}")
        else:
            print("   No new completed candles to save")
        
        return new_candles

    def get_period_boundary(self, dt: datetime, period_minutes: int) -> datetime:
        """
        Get the period boundary for a given datetime and period
        
        Args:
            dt: Datetime to get boundary for
            period_minutes: Period in minutes (5, 10, 15, 30)
            
        Returns:
            Datetime of the period boundary
        """
        # Round down to the nearest period boundary
        minute_boundary = (dt.minute // period_minutes) * period_minutes
        return dt.replace(minute=minute_boundary, second=0, microsecond=0) 