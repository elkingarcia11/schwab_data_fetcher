#!/usr/bin/env python3
"""
Data Aggregator Module  
Handles aggregation from 1-minute data to 5-minute and 15-minute timeframes
"""

import pandas as pd
from datetime import datetime
import pytz
from typing import Optional, List, Dict
from data_fetcher import DataFetcher

class DataAggregator:
    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.et_timezone = pytz.timezone('US/Eastern')
    
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

    def aggregate_candles_to_period(self, symbol: str, target_period: str, inverse: bool = False) -> bool:
        """
        Aggregate 1-minute candles to create 5-minute or 15-minute candles
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            target_period: Target period ('5m' or '15m')
            inverse: Whether to aggregate inverse price data
            
        Returns:
            True if successful, False otherwise
        """
        if target_period not in ['5m', '15m']:
            print(f"❌ Unsupported aggregation period: {target_period}")
            return False
        
        period_minutes = 5 if target_period == '5m' else 15
        file_type = "INVERSE" if inverse else "regular"
        
        print(f"🔄 Aggregating 1m {file_type} data to {target_period} for {symbol}...")
        
        # Load 1-minute data (regular or inverse)
        df_1m = self.data_fetcher.load_csv_data(symbol, '1m', inverse=inverse)
        if df_1m is None or df_1m.empty:
            print(f"❌ No 1-minute {file_type} data available for aggregation")
            return False
        
        # Get last timestamp from target period file (regular or inverse)
        last_aggregated_timestamp = self.data_fetcher.get_latest_timestamp_from_csv(symbol, target_period, inverse=inverse)
        
        # Filter 1m data to only process data that hasn't been aggregated yet
        if last_aggregated_timestamp:
            # Only process 1m data that comes AFTER the last complete aggregated period
            # We need to add the period duration to ensure we don't re-aggregate existing periods
            next_period_start = last_aggregated_timestamp + (period_minutes * 60 * 1000)
            df_1m = df_1m[df_1m['timestamp'] >= next_period_start]
            print(f"📊 Filtering 1m {file_type} data from {datetime.fromtimestamp(next_period_start / 1000)} onwards")
        else:
            print(f"📊 No existing {target_period} {file_type} data, aggregating all 1m data")
            
        if df_1m.empty:
            print(f"📊 No new 1m {file_type} data to aggregate for {target_period}")
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
                    print(f"⏳ Skipping incomplete {target_period} {file_type} period: {datetime.fromtimestamp(boundary_ts / 1000)}")
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
            print(f"✅ Aggregated {target_period} {file_type} period: {datetime.fromtimestamp(boundary_ts / 1000)} ({len(group)} 1m candles)")
        
        if aggregated_candles:
            # Sort by timestamp
            aggregated_candles.sort(key=lambda x: x['datetime'])
            
            # Append to target period CSV (regular or inverse)
            success = self.data_fetcher.append_to_csv(symbol, target_period, aggregated_candles, inverse=inverse)
            
            if success:
                print(f"✅ Aggregated {len(aggregated_candles)} new {target_period} {file_type} candles for {symbol}")
            else:
                print(f"❌ Failed to save {target_period} {file_type} candles for {symbol}")
                
            return success
        else:
            print(f"📊 No complete {target_period} {file_type} periods to aggregate")
            return True

    def aggregate_all_timeframes(self, symbol: str, inverse: bool = False) -> bool:
        """
        Aggregate data for all timeframes (5m and 15m) from 1m data
        
        Args:
            symbol: Stock symbol
            inverse: Whether to aggregate inverse price data
            
        Returns:
            True if all aggregations successful, False otherwise
        """
        file_type = "INVERSE" if inverse else "regular"
        print(f"\n🔄 Starting {file_type} aggregation for all timeframes: {symbol}")
        print("=" * 70)
        
        # Aggregate to 5-minute data
        print(f"\n📊 Step 1: Aggregating to 5-minute {file_type} data...")
        success_5m = self.aggregate_candles_to_period(symbol, '5m', inverse=inverse)
        
        # Aggregate to 15-minute data
        print(f"\n📊 Step 2: Aggregating to 15-minute {file_type} data...")
        success_15m = self.aggregate_candles_to_period(symbol, '15m', inverse=inverse)
        
        # Summary
        print(f"\n📈 {file_type.title()} Aggregation Summary for {symbol}:")
        print(f"   5m data: {'✅ Success' if success_5m else '❌ Failed'}")
        print(f"   15m data: {'✅ Success' if success_15m else '❌ Failed'}")
        
        overall_success = success_5m and success_15m
        
        if overall_success:
            print(f"🎉 All {file_type} aggregations successful for {symbol}!")
        else:
            print(f"⚠️  Some {file_type} aggregations failed for {symbol}")
        
        return overall_success

    def aggregate_both_regular_and_inverse(self, symbol: str) -> bool:
        """
        Aggregate both regular and inverse data for all timeframes
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if all aggregations successful, False otherwise
        """
        print(f"\n🔄 Starting complete aggregation (regular + inverse) for {symbol}")
        print("=" * 80)
        
        # Aggregate regular data
        success_regular = self.aggregate_all_timeframes(symbol, inverse=False)
        
        # Aggregate inverse data
        success_inverse = self.aggregate_all_timeframes(symbol, inverse=True)
        
        # Overall summary
        print(f"\n📈 Complete Aggregation Summary for {symbol}:")
        print(f"   Regular data: {'✅ Success' if success_regular else '❌ Failed'}")
        print(f"   Inverse data: {'✅ Success' if success_inverse else '❌ Failed'}")
        
        overall_success = success_regular and success_inverse
        
        if overall_success:
            print(f"🎉 All aggregations (regular + inverse) successful for {symbol}!")
        else:
            print(f"⚠️  Some aggregations failed for {symbol}")
        
        return overall_success

    def get_aggregation_stats(self, symbol: str, include_inverse: bool = True) -> Dict[str, int]:
        """
        Get statistics about aggregated data
        
        Args:
            symbol: Stock symbol
            include_inverse: Whether to include inverse data stats
            
        Returns:
            Dictionary with row counts for each timeframe
        """
        stats = {}
        
        periods = ['1m', '5m', '15m']
        
        for period in periods:
            # Regular data
            df_regular = self.data_fetcher.load_csv_data(symbol, period, inverse=False)
            stats[period] = len(df_regular) if df_regular is not None else 0
            
            # Inverse data
            if include_inverse:
                df_inverse = self.data_fetcher.load_csv_data(symbol, period, inverse=True)
                stats[f"{period}_INVERSE"] = len(df_inverse) if df_inverse is not None else 0
        
        return stats

    def validate_aggregation_integrity(self, symbol: str, include_inverse: bool = True) -> bool:
        """
        Validate that aggregation maintains data integrity for both regular and inverse data
        
        Args:
            symbol: Stock symbol
            include_inverse: Whether to validate inverse data too
            
        Returns:
            True if aggregation integrity is valid, False otherwise
        """
        print(f"\n🔍 Validating aggregation integrity for {symbol}...")
        
        data_types = ['regular']
        if include_inverse:
            data_types.append('inverse')
        
        overall_valid = True
        
        for data_type in data_types:
            is_inverse = (data_type == 'inverse')
            print(f"\n🔍 Checking {data_type} data integrity...")
            
            # Load all timeframe data
            df_1m = self.data_fetcher.load_csv_data(symbol, '1m', inverse=is_inverse)
            df_5m = self.data_fetcher.load_csv_data(symbol, '5m', inverse=is_inverse)
            df_15m = self.data_fetcher.load_csv_data(symbol, '15m', inverse=is_inverse)
            
            if df_1m is None or df_1m.empty:
                print(f"❌ No 1m {data_type} data to validate against")
                overall_valid = False
                continue
            
            issues = []
            
            # Check 5m aggregation
            if df_5m is not None and not df_5m.empty:
                # Validate that 5m boundaries align correctly
                for _, row in df_5m.iterrows():
                    timestamp = int(row['timestamp'])
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    if dt.minute % 5 != 0:
                        issues.append(f"5m {data_type} timestamp not on 5-minute boundary: {dt}")
            
            # Check 15m aggregation  
            if df_15m is not None and not df_15m.empty:
                # Validate that 15m boundaries align correctly
                for _, row in df_15m.iterrows():
                    timestamp = int(row['timestamp'])
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    if dt.minute % 15 != 0:
                        issues.append(f"15m {data_type} timestamp not on 15-minute boundary: {dt}")
            
            if issues:
                print(f"❌ {data_type.title()} aggregation integrity issues found:")
                for issue in issues:
                    print(f"   - {issue}")
                overall_valid = False
            else:
                print(f"✅ {data_type.title()} aggregation integrity validated successfully")
        
        return overall_valid 