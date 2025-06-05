#!/usr/bin/env python3
"""
Scheduled Market Data Coordinator
Designed for cron-based execution at specific intervals
Handles: data fetching → indicator calculation → position analysis
"""

import sys
import argparse
from datetime import datetime, time, timedelta
import pytz
from typing import Optional

# Import all our modular components
from data_fetcher import DataFetcher
from indicator_calculator import IndicatorCalculator
from position_tracker import PositionTracker
from email_notifier import EmailNotifier
from schwab_auth import SchwabAuth

class ScheduledCoordinator:
    def __init__(self):
        # Initialize all modular components
        self.data_fetcher = DataFetcher()
        self.indicator_calculator = IndicatorCalculator()
        self.position_tracker = PositionTracker()
        self.email_notifier = EmailNotifier()
        self.schwab_auth = SchwabAuth()
        
        # Market hours and timezone
        self.et_timezone = pytz.timezone('US/Eastern')
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET

    def is_market_hours(self) -> bool:
        """Check if current time is within market hours"""
        current_time = datetime.now(self.et_timezone).time()
        return self.market_open <= current_time <= self.market_close

    def is_market_day(self) -> bool:
        """Check if today is a market day (weekday)"""
        current_date = datetime.now(self.et_timezone)
        return current_date.weekday() < 5  # Monday = 0, Friday = 4

    def run_scheduled_execution(self, symbol: str, frequency: str) -> bool:
        """
        Execute scheduled data collection, indicator calculation, and position analysis
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            frequency: Data frequency ('5m', '10m', '15m', '30m')
            
        Returns:
            True if successful, False otherwise
        """
        current_time = datetime.now(self.et_timezone)
        print(f"\n🕒 Scheduled Execution: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ET")
        print(f"📊 Symbol: {symbol} | Frequency: {frequency}")
        print("=" * 60)
        
        # Check if it's a valid market day and time
        if not self.is_market_day():
            print("📅 Not a market day (weekend). Skipping execution.")
            return True
        
        if not self.is_market_hours():
            print("🕒 Outside market hours. Skipping execution.")
            return True
        
        # Check authentication
        if not self.schwab_auth.is_authenticated():
            print("❌ Authentication failed. Skipping execution.")
            return False
        
        overall_success = True
        
        try:
            # Step 1: Fetch data at specified frequency
            print(f"\n📡 Step 1: Fetching {frequency} data...")
            fetch_success = self.data_fetcher.fetch_data_at_frequency(symbol, frequency)
            
            if not fetch_success:
                print(f"❌ Failed to fetch {frequency} data")
                overall_success = False
            else:
                print(f"✅ {frequency} data fetch completed")
            
            # Step 2: Calculate indicators for the frequency
            print(f"\n📈 Step 2: Calculating {frequency} indicators...")
            
            # Calculate indicators for both regular and inverse data
            regular_indicators = self.indicator_calculator.calculate_all_indicators(symbol, frequency, inverse=False)
            inverse_indicators = self.indicator_calculator.calculate_all_indicators(symbol, frequency, inverse=True)
            
            indicators_success = regular_indicators and inverse_indicators
            
            if not indicators_success:
                print(f"❌ Failed to calculate {frequency} indicators")
                overall_success = False
            else:
                print(f"✅ {frequency} indicator calculation completed")
            
            # Step 3: Analyze position signals for this frequency
            print(f"\n🎯 Step 3: Analyzing {frequency} position signals...")
            
            # Check for position signals on this specific timeframe
            period_signals = self.position_tracker.check_position_signals(symbol, frequency)
            
            signals_found = False
            
            # Process LONG signals
            long_signal = period_signals['LONG']
            if long_signal['action']:
                signals_found = True
                self.position_tracker._send_position_notification(symbol, frequency, 'LONG', long_signal)
                print(f"🚨 LONG {long_signal['action']} signal detected for {symbol}_{frequency}")
            
            # Process SHORT signals
            short_signal = period_signals['SHORT']
            if short_signal['action']:
                signals_found = True
                self.position_tracker._send_position_notification(symbol, frequency, 'SHORT', short_signal)
                print(f"🚨 SHORT {short_signal['action']} signal detected for {symbol}_{frequency}")
            
            if not signals_found:
                print(f"📊 No position signals for {symbol}_{frequency}")
            
            # Show current position status for this timeframe
            positions = self.position_tracker.get_position_status()
            print(f"📊 Current {frequency} Position: {positions.get(frequency, 'N/A')}")
            
            # Step 4: Summary
            print(f"\n📈 Scheduled Execution Summary:")
            print(f"   Data Fetch ({frequency}): {'✅ Success' if fetch_success else '❌ Failed'}")
            print(f"   Indicators ({frequency}): {'✅ Success' if indicators_success else '❌ Failed'}")
            print(f"   Position Signals: {'🚨 Found' if signals_found else '📊 None'}")
            print(f"   Overall: {'✅ Success' if overall_success else '❌ Partial failure'}")
            
        except Exception as e:
            print(f"❌ Error during scheduled execution: {e}")
            overall_success = False
        
        return overall_success

    def run_bootstrap(self, symbol: str, frequency: str) -> bool:
        """
        Run initial bootstrap to fill historical data from previous trading day
        and analyze historical positions to continue where we left off
        
        Args:
            symbol: Stock symbol
            frequency: Data frequency
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n🔄 Bootstrap Mode: Historical data + position analysis for {symbol}_{frequency}")
        print("=" * 75)
        
        # Step 1: Fetch bootstrap data from previous trading day 9:30AM ET
        print(f"📡 Step 1: Fetching bootstrap data...")
        fetch_success = self.data_fetcher.fetch_bootstrap_data(symbol, frequency)
        
        if not fetch_success:
            print(f"❌ Bootstrap data fetch failed for {symbol}_{frequency}")
            return False
        
        # Step 2: Calculate indicators for both regular and inverse data
        print(f"\n📈 Step 2: Calculating indicators...")
        regular_indicators = self.indicator_calculator.calculate_all_indicators(symbol, frequency, inverse=False)
        inverse_indicators = self.indicator_calculator.calculate_all_indicators(symbol, frequency, inverse=True)
        
        indicators_success = regular_indicators and inverse_indicators
        
        if not indicators_success:
            print(f"❌ Failed to calculate indicators for {symbol}_{frequency}")
            return False
        
        # Step 3: Analyze historical positions to continue where we left off
        print(f"\n🎯 Step 3: Analyzing historical positions (emails suppressed)...")
        historical_analysis = self.position_tracker.analyze_historical_positions(symbol, suppress_emails=True)
        
        # Step 4: Summary
        print(f"\n🔄 Bootstrap Summary for {symbol}_{frequency}:")
        print(f"   Data Fetch: {'✅ Success' if fetch_success else '❌ Failed'}")
        print(f"   Indicators: {'✅ Success' if indicators_success else '❌ Failed'}")
        print(f"   Historical Analysis: {'✅ Complete' if historical_analysis else '❌ Failed'}")
        print(f"   Total Historical Signals: {historical_analysis.get('total_signals', 0)}")
        print(f"   Position States Ready: ✅ Ready for live trading")
        
        return True

    def run_analysis_only(self, symbol: str, frequency: str) -> bool:
        """
        Run position analysis only for a specific frequency
        
        Args:
            symbol: Stock symbol
            frequency: Data frequency
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n🎯 Analysis Mode: {symbol}_{frequency}")
        print("=" * 40)
        
        try:
            # Check for position signals
            period_signals = self.position_tracker.check_position_signals(symbol, frequency)
            
            # Process signals
            signals_found = False
            
            long_signal = period_signals['LONG']
            if long_signal['action']:
                signals_found = True
                self.position_tracker._send_position_notification(symbol, frequency, 'LONG', long_signal)
                print(f"🚨 LONG {long_signal['action']} signal: ${long_signal['price']}")
            
            short_signal = period_signals['SHORT']
            if short_signal['action']:
                signals_found = True
                self.position_tracker._send_position_notification(symbol, frequency, 'SHORT', short_signal)
                print(f"🚨 SHORT {short_signal['action']} signal: ${short_signal['price']}")
            
            if not signals_found:
                print(f"📊 No position signals for {symbol}_{frequency}")
            
            # Show position status
            positions = self.position_tracker.get_position_status()
            print(f"📊 Current {frequency} Position: {positions.get(frequency, 'N/A')}")
            
            return True
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return False


def main():
    """Main function for scheduled execution"""
    parser = argparse.ArgumentParser(description='Scheduled Market Data Coordinator')
    parser.add_argument('symbol', help='Stock symbol (e.g., SPY)')
    parser.add_argument('frequency', choices=['5m', '10m', '15m', '30m'], 
                       help='Data frequency')
    parser.add_argument('--mode', choices=['scheduled', 'bootstrap', 'analysis'], 
                       default='scheduled', help='Execution mode (scheduled is recommended for cron jobs)')
    
    args = parser.parse_args()
    
    coordinator = ScheduledCoordinator()
    
    print(f"🚀 Scheduled Market Data Coordinator")
    print(f"📊 Mode: {args.mode.upper()}")
    print(f"📊 Symbol: {args.symbol}")
    print(f"📊 Frequency: {args.frequency}")
    
    # For cron jobs, always use scheduled mode (complete workflow)
    if args.mode == 'scheduled':
        success = coordinator.run_scheduled_execution(args.symbol, args.frequency)
    elif args.mode == 'bootstrap':
        success = coordinator.run_bootstrap(args.symbol, args.frequency)
    elif args.mode == 'analysis':
        success = coordinator.run_analysis_only(args.symbol, args.frequency)
    
    if success:
        print(f"\n✅ {args.mode.title()} execution completed successfully")
        sys.exit(0)
    else:
        print(f"\n❌ {args.mode.title()} execution failed")
        sys.exit(1)


if __name__ == "__main__":
    main() 