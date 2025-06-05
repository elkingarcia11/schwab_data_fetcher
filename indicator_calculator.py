#!/usr/bin/env python3
"""
Indicator Calculator Module
Handles all technical indicator calculations (EMA, VWMA, MACD, ROC)
"""

import pandas as pd
from typing import List, Tuple, Optional
from data_fetcher import DataFetcher

class IndicatorCalculator:
    def __init__(self):
        self.data_fetcher = DataFetcher()
    
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

    def calculate_all_indicators(self, symbol: str, period: str, inverse: bool = False) -> bool:
        """
        Calculate and update all technical indicators in CSV file
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            period: Time period (e.g., '1m', '5m', '15m')
            inverse: Whether to calculate indicators for inverse price data
            
        Returns:
            True if successful, False otherwise
        """
        file_type = "INVERSE" if inverse else "regular"
        print(f"📈 Calculating {file_type} indicators for {symbol}_{period}...")
        
        # Load the CSV data (regular or inverse)
        df = self.data_fetcher.load_csv_data(symbol, period, inverse=inverse)
        if df is None or df.empty:
            print(f"❌ No {file_type} data available for indicator calculation in {symbol}_{period}")
            return False
        
        # Clean up any extra columns that might have been created
        expected_columns = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 
                          'ema_7', 'vwma_17', 'ema_12', 'ema_26', 'macd_line', 'macd_signal', 'roc_8']
        df = df.reindex(columns=expected_columns)
        
        if 'close' not in df.columns or 'volume' not in df.columns:
            print(f"📊 Insufficient {file_type} data for indicator calculation in {symbol}_{period}")
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
        
        # Update the dataframe with full precision (no rounding for calculations)
        df['ema_7'] = ema_7
        df['ema_12'] = ema_12
        df['ema_26'] = ema_26
        df['vwma_17'] = vwma_17
        df['macd_line'] = macd_line
        df['macd_signal'] = macd_signal
        df['roc_8'] = roc_8
        
        # Save back to CSV (regular or inverse)
        success = self.data_fetcher.save_csv_data(symbol, period, df, inverse=inverse)
        
        if success:
            # Count how many indicators were calculated
            ema_7_count = sum(1 for val in ema_7 if val is not None)
            ema_12_count = sum(1 for val in ema_12 if val is not None)
            ema_26_count = sum(1 for val in ema_26 if val is not None)
            vwma_count = sum(1 for val in vwma_17 if val is not None)
            macd_count = sum(1 for val in macd_line if val is not None)
            signal_count = sum(1 for val in macd_signal if val is not None)
            roc_count = sum(1 for val in roc_8 if val is not None)
            
            print(f"📈 Updated {file_type} indicators for {symbol}_{period}:")
            print(f"   EMA7: {ema_7_count}, EMA12: {ema_12_count}, EMA26: {ema_26_count}")
            print(f"   VWMA17: {vwma_count}, MACD: {macd_count}, Signal: {signal_count}, ROC8: {roc_count}")
        
        return success

    def calculate_indicators_for_all_timeframes(self, symbol: str, inverse: bool = False) -> bool:
        """
        Calculate indicators for all timeframes (1m, 5m, 15m)
        
        Args:
            symbol: Stock symbol
            inverse: Whether to calculate indicators for inverse price data
            
        Returns:
            True if all successful, False otherwise
        """
        file_type = "INVERSE" if inverse else "regular"
        print(f"\n📈 Calculating {file_type} indicators for all timeframes: {symbol}")
        print("=" * 70)
        
        success_1m = self.calculate_all_indicators(symbol, '1m', inverse=inverse)
        success_5m = self.calculate_all_indicators(symbol, '5m', inverse=inverse)
        success_15m = self.calculate_all_indicators(symbol, '15m', inverse=inverse)
        
        print(f"\n📈 {file_type.title()} Indicator Calculation Summary for {symbol}:")
        print(f"   1m indicators: {'✅ Success' if success_1m else '❌ Failed'}")
        print(f"   5m indicators: {'✅ Success' if success_5m else '❌ Failed'}")
        print(f"   15m indicators: {'✅ Success' if success_15m else '❌ Failed'}")
        
        overall_success = success_1m and success_5m and success_15m
        
        if overall_success:
            print(f"🎉 All {file_type} indicator calculations successful for {symbol}!")
        else:
            print(f"⚠️  Some {file_type} indicator calculations failed for {symbol}")
        
        return overall_success

    def calculate_indicators_for_both_regular_and_inverse(self, symbol: str) -> bool:
        """
        Calculate indicators for both regular and inverse data across all timeframes
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if all calculations successful, False otherwise
        """
        print(f"\n📈 Calculating indicators for all timeframes (regular + inverse): {symbol}")
        print("=" * 80)
        
        # Calculate indicators for regular data
        success_regular = self.calculate_indicators_for_all_timeframes(symbol, inverse=False)
        
        # Calculate indicators for inverse data
        success_inverse = self.calculate_indicators_for_all_timeframes(symbol, inverse=True)
        
        # Overall summary
        print(f"\n📈 Complete Indicator Calculation Summary for {symbol}:")
        print(f"   Regular indicators: {'✅ Success' if success_regular else '❌ Failed'}")
        print(f"   Inverse indicators: {'✅ Success' if success_inverse else '❌ Failed'}")
        
        overall_success = success_regular and success_inverse
        
        if overall_success:
            print(f"🎉 All indicator calculations (regular + inverse) successful for {symbol}!")
        else:
            print(f"⚠️  Some indicator calculations failed for {symbol}")
        
        return overall_success

    def get_latest_indicators(self, symbol: str, period: str, inverse: bool = False) -> Optional[dict]:
        """
        Get the latest indicator values for a symbol and period
        
        Args:
            symbol: Stock symbol
            period: Time period
            inverse: Whether to get indicators from inverse price data
            
        Returns:
            Dictionary with latest indicator values, or None if error
        """
        df = self.data_fetcher.load_csv_data(symbol, period, inverse=inverse)
        if df is None or df.empty:
            return None
        
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
            return None
        
        try:
            file_type = "INVERSE" if inverse else "regular"
            result = {
                'timestamp': latest_row['timestamp'],
                'datetime': latest_row['datetime'],
                'close': float(latest_row['close']),
                'ema_7': float(latest_row['ema_7']),
                'ema_12': float(latest_row['ema_12']),
                'ema_26': float(latest_row['ema_26']),
                'vwma_17': float(latest_row['vwma_17']),
                'macd_line': float(latest_row['macd_line']),
                'macd_signal': float(latest_row['macd_signal']),
                'roc_8': float(latest_row['roc_8']),
                'data_type': file_type
            }
            return result
        except (ValueError, TypeError, KeyError) as e:
            file_type = "INVERSE" if inverse else "regular"
            print(f"⚠️  Error parsing {file_type} indicators for {symbol}_{period}: {e}")
            return None

    def get_latest_indicators_for_both_types(self, symbol: str, period: str) -> dict:
        """
        Get the latest indicator values for both regular and inverse data
        
        Args:
            symbol: Stock symbol
            period: Time period
            
        Returns:
            Dictionary with both regular and inverse indicator values
        """
        regular_indicators = self.get_latest_indicators(symbol, period, inverse=False)
        inverse_indicators = self.get_latest_indicators(symbol, period, inverse=True)
        
        return {
            'regular': regular_indicators,
            'inverse': inverse_indicators
        }

    def validate_indicator_integrity(self, symbol: str, period: str, inverse: bool = False) -> bool:
        """
        Validate that indicators are calculated correctly
        
        Args:
            symbol: Stock symbol
            period: Time period
            inverse: Whether to validate inverse price data indicators
            
        Returns:
            True if indicators are valid, False otherwise
        """
        file_type = "INVERSE" if inverse else "regular"
        df = self.data_fetcher.load_csv_data(symbol, period, inverse=inverse)
        if df is None or df.empty:
            print(f"❌ No {file_type} data to validate for {symbol}_{period}")
            return False
        
        issues = []
        
        # Check that we have sufficient data for each indicator
        total_rows = len(df)
        
        # EMA 7 should start after 7 periods
        ema_7_values = [val for val in df['ema_7'] if val != '' and pd.notna(val)]
        if len(ema_7_values) > 0 and total_rows >= 7:
            expected_min = total_rows - 7 + 1
            if len(ema_7_values) < expected_min:
                issues.append(f"EMA 7 has fewer values than expected: {len(ema_7_values)} < {expected_min}")
        
        # MACD should start after 26 periods (needs 26 EMA)
        macd_values = [val for val in df['macd_line'] if val != '' and pd.notna(val)]
        if len(macd_values) > 0 and total_rows >= 26:
            expected_min = total_rows - 26 + 1
            if len(macd_values) < expected_min:
                issues.append(f"MACD has fewer values than expected: {len(macd_values)} < {expected_min}")
        
        # ROC 8 should start after 8 periods
        roc_values = [val for val in df['roc_8'] if val != '' and pd.notna(val)]
        if len(roc_values) > 0 and total_rows >= 8:
            expected_min = total_rows - 8 + 1
            if len(roc_values) < expected_min:
                issues.append(f"ROC 8 has fewer values than expected: {len(roc_values)} < {expected_min}")
        
        if issues:
            print(f"❌ {file_type.title()} indicator validation issues for {symbol}_{period}:")
            for issue in issues:
                print(f"   - {issue}")
            return False
        else:
            print(f"✅ {file_type.title()} indicator validation passed for {symbol}_{period}")
            return True

    def validate_indicator_integrity_for_both_types(self, symbol: str, period: str) -> bool:
        """
        Validate indicators for both regular and inverse data
        
        Args:
            symbol: Stock symbol
            period: Time period
            
        Returns:
            True if both validations pass, False otherwise
        """
        print(f"\n🔍 Validating indicator integrity for both data types: {symbol}_{period}")
        print("=" * 70)
        
        regular_valid = self.validate_indicator_integrity(symbol, period, inverse=False)
        inverse_valid = self.validate_indicator_integrity(symbol, period, inverse=True)
        
        overall_valid = regular_valid and inverse_valid
        
        print(f"\n🔍 Indicator Validation Summary for {symbol}_{period}:")
        print(f"   Regular data: {'✅ Valid' if regular_valid else '❌ Invalid'}")
        print(f"   Inverse data: {'✅ Valid' if inverse_valid else '❌ Invalid'}")
        print(f"   Overall: {'✅ All valid' if overall_valid else '❌ Some invalid'}")
        
        return overall_valid 