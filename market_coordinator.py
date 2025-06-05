#!/usr/bin/env python3
"""
Market Data Coordinator
Main orchestrator that coordinates all modular components:
- Data fetching (data_fetcher.py)
- Data aggregation (data_aggregator.py)  
- Indicator calculation (indicator_calculator.py)
- Position tracking (position_tracker.py)
- Email notifications (email_notifier.py)
- Authentication (schwab_auth.py)
"""

import signal
import sys
import time as time_module
from datetime import datetime, time, timedelta
import pytz
from typing import Optional

# Import all our modular components
from data_fetcher import DataFetcher
from data_aggregator import DataAggregator
from indicator_calculator import IndicatorCalculator
from position_tracker import PositionTracker
from email_notifier import EmailNotifier
from schwab_auth import SchwabAuth

class MarketCoordinator:
    def __init__(self):
        # Initialize all modular components
        self.data_fetcher = DataFetcher()
        self.data_aggregator = DataAggregator()
        self.indicator_calculator = IndicatorCalculator()
        self.position_tracker = PositionTracker()
        self.email_notifier = EmailNotifier()
        self.schwab_auth = SchwabAuth()
        
        # Market hours and timezone
        self.et_timezone = pytz.timezone('US/Eastern')
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET
        
        # Control flag for continuous operation
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False

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
            next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            target_time = next_minute + timedelta(seconds=offset_seconds)
            
            # If we've already passed this minute's target time, wait for next minute
            if now >= target_time:
                next_minute = next_minute + timedelta(minutes=1)
                target_time = next_minute + timedelta(seconds=offset_seconds)
            
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

    def complete_market_data_update(self, symbol: str, target_date: datetime.date = None) -> bool:
        """
        Complete workflow: fetch 1m data (regular + inverse), aggregate to 5m/15m, calculate indicators
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            target_date: Date to fetch data for (defaults to today)
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n🚀 Starting complete market data update for {symbol} (regular + inverse)")
        print("=" * 80)
        
        # Step 1: Fetch new 1-minute data (both regular and inverse)
        print("📊 Step 1: Fetching 1-minute data (regular + inverse)...")
        fetch_success = self.data_fetcher.fetch_new_data(symbol, '1m', target_date)
        
        if not fetch_success:
            print("❌ Failed to fetch 1-minute data, skipping remaining steps")
            return False
        
        # Step 2: Calculate indicators for 1m data (both regular and inverse)
        print("\n📈 Step 2: Calculating indicators for 1-minute data (regular + inverse)...")
        indicators_1m_success = self.indicator_calculator.calculate_indicators_for_both_regular_and_inverse(symbol)
        
        # Step 3: Aggregate to higher timeframes (both regular and inverse)
        print("\n🔄 Step 3: Aggregating to higher timeframes (regular + inverse)...")
        aggregation_success = self.data_aggregator.aggregate_both_regular_and_inverse(symbol)
        
        # Step 4: Calculate indicators for aggregated timeframes (both regular and inverse)
        if aggregation_success:
            print("\n📈 Step 4: Calculating indicators for aggregated timeframes (regular + inverse)...")
            
            # Calculate indicators for 5m data (both types)
            indicators_5m_regular = self.indicator_calculator.calculate_all_indicators(symbol, '5m', inverse=False)
            indicators_5m_inverse = self.indicator_calculator.calculate_all_indicators(symbol, '5m', inverse=True)
            
            # Calculate indicators for 15m data (both types)
            indicators_15m_regular = self.indicator_calculator.calculate_all_indicators(symbol, '15m', inverse=False)
            indicators_15m_inverse = self.indicator_calculator.calculate_all_indicators(symbol, '15m', inverse=True)
            
            indicators_success = (indicators_5m_regular and indicators_5m_inverse and 
                                indicators_15m_regular and indicators_15m_inverse)
        else:
            indicators_success = False
        
        # Summary
        print(f"\n📈 Complete Update Summary for {symbol}:")
        print(f"   1m data fetch (regular + inverse): {'✅ Success' if fetch_success else '❌ Failed'}")
        print(f"   1m indicators (regular + inverse): {'✅ Success' if indicators_1m_success else '❌ Failed'}")
        print(f"   Aggregation (regular + inverse): {'✅ Success' if aggregation_success else '❌ Failed'}")
        print(f"   5m/15m indicators (regular + inverse): {'✅ Success' if indicators_success else '❌ Failed'}")
        
        overall_success = fetch_success and indicators_1m_success and aggregation_success and indicators_success
        
        if overall_success:
            print(f"🎉 Complete market data update successful for {symbol} (regular + inverse)!")
        else:
            print(f"⚠️  Partial success - some steps may have failed")
        
        return overall_success

    def run_continuous_data_collection(self, symbol: str = "SPY"):
        """
        Run continuous market data collection during market hours with position tracking
        
        Args:
            symbol: Stock symbol to collect data for
        """
        print("🚀 Starting Continuous Market Data Collection with Position Tracking")
        print("=" * 70)
        print(f"📊 Symbol: {symbol}")
        print(f"🔄 Data Types: Regular + Inverse prices (1/price)")
        print(f"🕒 Market Hours: {self.market_open} - {self.market_close} ET")
        print(f"🔄 Token Refresh: Every {self.schwab_auth.token_refresh_interval // 60} minutes")
        print(f"⏰ API Calls: 5 seconds after each minute")
        print(f"🎯 Position Tracking: ENABLED for regular data (all timeframes)")
        print(f"📦 Modular Architecture: All components isolated")
        print("=" * 70)
        
        if not self.is_market_day():
            print("📅 Not a market day (weekend). Exiting.")
            return
        
        # Initial authentication check
        if not self.schwab_auth.is_authenticated():
            print("❌ Failed to get initial authentication. Exiting.")
            return
        
        # Bootstrap Phase: Complete update to fill any missing data
        print("\n🔄 BOOTSTRAP PHASE: Filling any missing data (regular + inverse)...")
        print("-" * 80)
        bootstrap_success = self.complete_market_data_update(symbol)
        
        if bootstrap_success:
            print("✅ Bootstrap phase completed successfully")
            
            # Analyze historical positions after bootstrap (using regular data for trading signals)
            print("\n🎯 POSITION ANALYSIS: Analyzing historical data (regular prices)...")
            print("-" * 80)
            try:
                historical_results = self.position_tracker.analyze_historical_positions(symbol)
                print(f"✅ Historical position analysis completed")
                print(f"   Found {historical_results['total_signals']} total position signals")
            except Exception as e:
                print(f"⚠️  Historical analysis had issues: {e}")
        else:
            print("⚠️  Bootstrap phase had some issues, but continuing...")
        
        # Start incremental updates
        print(f"\n🔄 INCREMENTAL PHASE: Starting real-time updates...")
        print("-" * 80)
        
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
                
                # Run the complete market data update workflow (regular + inverse)
                success = self.complete_market_data_update(symbol)
                
                if success:
                    print(f"✅ Incremental cycle #{iteration_count} completed successfully")
                    
                    # Check for live position signals after data update (using regular data)
                    print(f"🎯 Checking live position signals (regular data)...")
                    signals_found = self.position_tracker.check_live_position_signals(symbol)
                    
                    if signals_found:
                        print(f"🚨 New position signals detected and processed!")
                    else:
                        print(f"📊 No new position signals at this time")
                        
                    # Show current position status (ONLY 5m and 15m)
                    positions = self.position_tracker.get_position_status()
                    print(f"📊 Current Positions: 5m:{positions['5m']} | 15m:{positions['15m']}")
                    
                    # Show data counts for monitoring (include 1m for aggregation pipeline)
                    stats = self.data_aggregator.get_aggregation_stats(symbol, include_inverse=True)
                    print(f"📊 Data Counts: 1m:{stats.get('1m', 0)}/{stats.get('1m_INVERSE', 0)} | " +
                          f"5m:{stats.get('5m', 0)}/{stats.get('5m_INVERSE', 0)} | " +
                          f"15m:{stats.get('15m', 0)}/{stats.get('15m_INVERSE', 0)} (reg/inv)")
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
        
        positions = self.position_tracker.get_position_status()
        stats = self.data_aggregator.get_aggregation_stats(symbol, include_inverse=True)
        
        print("\n🏁 Continuous data collection stopped")
        print(f"📊 Bootstrap: ✅ Completed")
        print(f"📊 Incremental cycles: {iteration_count}")
        print(f"🎯 Final Positions: 5m:{positions['5m']} | 15m:{positions['15m']}")
        print(f"📊 Final Data Counts: 1m:{stats.get('1m', 0)}/{stats.get('1m_INVERSE', 0)} | " +
              f"5m:{stats.get('5m', 0)}/{stats.get('5m_INVERSE', 0)} | " +
              f"15m:{stats.get('15m', 0)}/{stats.get('15m_INVERSE', 0)} (reg/inv)")
        print(f"📊 Total data collection session complete (regular + inverse)")

    def run_single_update(self, symbol: str) -> bool:
        """
        Run single complete update with position analysis
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if successful, False otherwise
        """
        print(f"📊 Running single update for {symbol} (regular + inverse)")
        
        success = self.complete_market_data_update(symbol)
        
        if success:
            print(f"\n🎉 Successfully updated all timeframes for {symbol} (regular + inverse)!")
            
            # Show data statistics
            stats = self.data_aggregator.get_aggregation_stats(symbol, include_inverse=True)
            print(f"\n📊 Data Statistics:")
            print(f"   Regular: 1m:{stats.get('1m', 0)} | 5m:{stats.get('5m', 0)} | 15m:{stats.get('15m', 0)}")
            print(f"   Inverse: 1m:{stats.get('1m_INVERSE', 0)} | 5m:{stats.get('5m_INVERSE', 0)} | 15m:{stats.get('15m_INVERSE', 0)}")
            
            # Run position analysis after single update (using regular data)
            print(f"\n🎯 Running position analysis (regular data)...")
            try:
                results = self.position_tracker.analyze_historical_positions(symbol)
                print(f"✅ Position analysis completed - {results['total_signals']} signals found")
                return True
            except Exception as e:
                print(f"⚠️  Position analysis failed: {e}")
                return False
        else:
            print(f"\n❌ Some issues occurred during update for {symbol}")
            return False

    def run_analysis_only(self, symbol: str) -> bool:
        """
        Run historical position analysis only (no data fetching)
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if successful, False otherwise
        """
        print(f"🎯 Running historical position analysis for {symbol} (regular data)")
        
        try:
            results = self.position_tracker.analyze_historical_positions(symbol)
            print(f"\n🎉 Position analysis completed!")
            print(f"   Total signals: {results['total_signals']}")
            print(f"   Opens: {results['open_signals']}")
            print(f"   Closes: {results['close_signals']}")
            
            # Show data statistics for both types
            stats = self.data_aggregator.get_aggregation_stats(symbol, include_inverse=True)
            print(f"\n📊 Available Data Statistics:")
            print(f"   Regular: 1m:{stats.get('1m', 0)} | 5m:{stats.get('5m', 0)} | 15m:{stats.get('15m', 0)}")
            print(f"   Inverse: 1m:{stats.get('1m_INVERSE', 0)} | 5m:{stats.get('5m_INVERSE', 0)} | 15m:{stats.get('15m_INVERSE', 0)}")
            
            return True
        except Exception as e:
            print(f"\n❌ Position analysis failed: {e}")
            return False

    def test_all_modules(self, symbol: str = "SPY") -> bool:
        """
        Test all modular components independently (including inverse data handling)
        
        Args:
            symbol: Stock symbol to test with
            
        Returns:
            True if all tests pass, False otherwise
        """
        print(f"🧪 Testing all modules with {symbol} (regular + inverse)")
        print("=" * 60)
        
        all_tests_passed = True
        
        # Test 1: Authentication
        print("🔐 Testing authentication module...")
        auth_valid = self.schwab_auth.validate_credentials()
        if not auth_valid:
            all_tests_passed = False
        
        # Test 2: Email notifier
        print("\n📧 Testing email module...")
        email_test = self.email_notifier.test_configuration()
        
        # Test 3: Data fetcher (check if data exists for both types)
        print(f"\n📊 Testing data fetcher module...")
        df_regular = self.data_fetcher.load_csv_data(symbol, '1m', inverse=False)
        df_inverse = self.data_fetcher.load_csv_data(symbol, '1m', inverse=True)
        
        if df_regular is not None and not df_regular.empty:
            print(f"✅ Data fetcher (regular): Found {len(df_regular)} rows of 1m data")
        else:
            print(f"⚠️  Data fetcher (regular): No existing data found for {symbol}")
            
        if df_inverse is not None and not df_inverse.empty:
            print(f"✅ Data fetcher (inverse): Found {len(df_inverse)} rows of 1m inverse data")
        else:
            print(f"⚠️  Data fetcher (inverse): No existing inverse data found for {symbol}")
        
        # Test 4: Indicator calculator (both types)
        print(f"\n📈 Testing indicator calculator...")
        if df_regular is not None and len(df_regular) > 26:
            indicator_test_regular = self.indicator_calculator.validate_indicator_integrity(symbol, '1m', inverse=False)
            if not indicator_test_regular:
                all_tests_passed = False
        else:
            print(f"⚠️  Not enough regular data for indicator validation")
            
        if df_inverse is not None and len(df_inverse) > 26:
            indicator_test_inverse = self.indicator_calculator.validate_indicator_integrity(symbol, '1m', inverse=True)
            if not indicator_test_inverse:
                all_tests_passed = False
        else:
            print(f"⚠️  Not enough inverse data for indicator validation")
        
        # Test 5: Position tracker (uses regular data)
        print(f"\n🎯 Testing position tracker...")
        if df_regular is not None and len(df_regular) > 26:
            position_test = self.position_tracker.validate_position_logic(symbol, '1m')
            if not position_test:
                all_tests_passed = False
        else:
            print(f"⚠️  Not enough regular data for position logic validation")
        
        # Test 6: Data aggregator integrity (both types)
        print(f"\n🔄 Testing data aggregator...")
        aggregation_test = self.data_aggregator.validate_aggregation_integrity(symbol, include_inverse=True)
        if not aggregation_test:
            all_tests_passed = False
        
        # Test 7: Show data statistics
        print(f"\n📊 Data availability summary...")
        stats = self.data_aggregator.get_aggregation_stats(symbol, include_inverse=True)
        print(f"   Regular: 1m:{stats.get('1m', 0)} | 5m:{stats.get('5m', 0)} | 15m:{stats.get('15m', 0)}")
        print(f"   Inverse: 1m:{stats.get('1m_INVERSE', 0)} | 5m:{stats.get('5m_INVERSE', 0)} | 15m:{stats.get('15m_INVERSE', 0)}")
        
        print(f"\n🧪 Module Testing Summary:")
        print(f"   Authentication: {'✅ Pass' if auth_valid else '❌ Fail'}")
        print(f"   Email: {'✅ Pass' if email_test else '⚠️  Not configured'}")
        print(f"   Data availability (regular): {'✅ Available' if df_regular is not None else '⚠️  No data'}")
        print(f"   Data availability (inverse): {'✅ Available' if df_inverse is not None else '⚠️  No data'}")
        print(f"   Aggregation integrity: {'✅ Pass' if aggregation_test else '❌ Fail'}")
        print(f"   Overall: {'✅ All tests passed' if all_tests_passed else '❌ Some tests failed'}")
        
        return all_tests_passed


def main():
    """Main function with options for single run, continuous collection, analysis, or testing"""
    coordinator = MarketCoordinator()
    
    print("🚀 Schwab Market Data Coordinator (Modular Architecture + Inverse Prices)")
    print("=" * 80)
    
    # Check command line arguments
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--single":
            # Single run mode
            symbol = sys.argv[2] if len(sys.argv) > 2 else "SPY"
            coordinator.run_single_update(symbol)
                
        elif sys.argv[1] == "--analyze":
            # Historical position analysis only
            symbol = sys.argv[2] if len(sys.argv) > 2 else "SPY"
            coordinator.run_analysis_only(symbol)
                
        elif sys.argv[1] == "--test":
            # Test all modules
            symbol = sys.argv[2] if len(sys.argv) > 2 else "SPY"
            coordinator.test_all_modules(symbol)
                
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python market_coordinator.py                     # Continuous mode with position tracking")
            print("  python market_coordinator.py --single [SYMBOL]   # Single run + position analysis")
            print("  python market_coordinator.py --analyze [SYMBOL]  # Historical position analysis only")
            print("  python market_coordinator.py --test [SYMBOL]     # Test all modules")
            print("  python market_coordinator.py --help             # Show this help")
            print()
            print("Modular Components:")
            print("  • data_fetcher.py - API calls and CSV operations (regular + inverse)")
            print("  • data_aggregator.py - 1m→5m→15m aggregation (regular + inverse)")  
            print("  • indicator_calculator.py - Technical indicators (regular + inverse)")
            print("  • position_tracker.py - Signal detection & P&L (regular data)")
            print("  • email_notifier.py - Email notifications")
            print("  • schwab_auth.py - Authentication & tokens")
            print()
            print("Inverse Price Tracking:")
            print("  • Creates parallel *_INVERSE.csv files with 1/price OHLC data")
            print("  • Aggregates inverse data: 1m_INVERSE → 5m_INVERSE → 15m_INVERSE")
            print("  • Calculates same technical indicators for inverse data")
            print("  • Position tracking uses regular data (not inverse)")
            print()
            print("Position Tracking Features:")
            print("  • LONG positions: Opens when 7EMA > 17VWMA AND MACD > Signal AND ROC > 0 (regular data)")
            print("  • SHORT positions: Opens when 7EMA > 17VWMA AND MACD > Signal AND ROC > 0 (inverse data)")
            print("  • Closes when: Any 2 of the 3 conditions fail for respective position type")
            print("  • Email notifications for all LONG and SHORT position changes")
            print("  • Tracks 5m and 15m timeframes only (1m used for aggregation only)")
            print("  • P&L tracking: LONG profits from price increases, SHORT profits from price decreases")
            print()
            print("Position Display Format: L=LONG/S=SHORT (e.g., '5m:L:O/S:C' = LONG Open, SHORT Closed)")
            print()
            print("Default symbol: SPY")
            
        else:
            symbol = sys.argv[1]
            print(f"📊 Running continuous collection with position tracking for {symbol}")
            coordinator.run_continuous_data_collection(symbol)
    else:
        # Default: Continuous mode
        symbol = "SPY"
        print(f"📊 Running continuous collection with position tracking for {symbol}")
        print("   Use --single for one-time update + analysis")
        print("   Use --analyze for historical analysis only") 
        print("   Use --test for module testing")
        print("   Use --help for usage information")
        print()
        coordinator.run_continuous_data_collection(symbol)


if __name__ == "__main__":
    main() 