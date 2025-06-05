#!/usr/bin/env python3
"""
Continuous Market Trading System
Runs continuously with health checks every 4 minutes
Executes trading logic for multiple symbols across 5m, 10m, 15m, 30m timeframes
Starting at 9:30AM ET during market hours
"""

import sys
import time
import argparse
import threading
import signal
from datetime import datetime, time as dt_time, timedelta
import pytz
from typing import List, Dict
import logging

# Import all our modular components
from scheduled_coordinator import ScheduledCoordinator
from schwab_auth import SchwabAuth

class ContinuousTrader:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.frequencies = ['5m', '10m', '15m', '30m']
        self.coordinator = ScheduledCoordinator()
        self.schwab_auth = SchwabAuth()
        
        # Market hours and timezone
        self.et_timezone = pytz.timezone('US/Eastern')
        self.market_open = dt_time(9, 30)  # 9:30 AM ET
        self.market_close = dt_time(16, 0)  # 4:00 PM ET
        
        # Timing configuration
        self.health_check_interval = 240  # 4 minutes in seconds
        
        # Frequency intervals in seconds (aligned to start at 9:30AM)
        self.frequency_intervals = {
            '5m': 300,   # 5 minutes
            '10m': 600,  # 10 minutes
            '15m': 900,  # 15 minutes
            '30m': 1800  # 30 minutes
        }
        
        # Thread management
        self.running = True
        self.threads = {}
        
        # Logging setup
        self.setup_logging()
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('logs/continuous_trader.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.running = False
    
    def is_market_hours(self) -> bool:
        """Check if current time is within market hours"""
        current_time = datetime.now(self.et_timezone).time()
        return self.market_open <= current_time <= self.market_close
    
    def is_market_day(self) -> bool:
        """Check if today is a market day (weekday)"""
        current_date = datetime.now(self.et_timezone)
        return current_date.weekday() < 5  # Monday = 0, Friday = 4
    
    def wait_for_market_open(self):
        """Wait until market opens at 9:30AM ET"""
        while self.running:
            current_time = datetime.now(self.et_timezone)
            
            if not self.is_market_day():
                # If it's weekend, wait until next weekday
                self.logger.info("Weekend detected. Waiting until next trading day...")
                time.sleep(3600)  # Check every hour
                continue
            
            if not self.is_market_hours():
                market_open_today = self.et_timezone.localize(
                    datetime.combine(current_time.date(), self.market_open)
                )
                
                if current_time < market_open_today:
                    wait_seconds = (market_open_today - current_time).total_seconds()
                    self.logger.info(f"Market opens in {wait_seconds/3600:.2f} hours. Waiting...")
                    time.sleep(min(wait_seconds, 300))  # Check every 5 minutes or until market open
                    continue
                else:
                    # Market is closed for the day
                    self.logger.info("Market is closed for today. Waiting until next trading day...")
                    time.sleep(3600)  # Check every hour
                    continue
            
            # Market is open
            break
    
    def run_bootstrap_for_all(self) -> bool:
        """Run bootstrap for all symbols and frequencies"""
        self.logger.info("🔄 STARTING BOOTSTRAP PROCESS")
        self.logger.info("=" * 80)
        
        bootstrap_success = True
        
        for symbol in self.symbols:
            self.logger.info(f"\n📊 Bootstrapping {symbol}...")
            
            for frequency in self.frequencies:
                self.logger.info(f"  🔄 Bootstrap {symbol}_{frequency}...")
                
                try:
                    success = self.coordinator.run_bootstrap(symbol, frequency)
                    if not success:
                        self.logger.error(f"❌ Bootstrap failed for {symbol}_{frequency}")
                        bootstrap_success = False
                    else:
                        self.logger.info(f"✅ Bootstrap completed for {symbol}_{frequency}")
                except Exception as e:
                    self.logger.error(f"❌ Bootstrap error for {symbol}_{frequency}: {e}")
                    bootstrap_success = False
        
        if bootstrap_success:
            self.logger.info("✅ BOOTSTRAP PROCESS COMPLETED SUCCESSFULLY")
        else:
            self.logger.warning("⚠️  BOOTSTRAP PROCESS COMPLETED WITH SOME FAILURES")
        
        return bootstrap_success
    
    def calculate_next_run_time(self, frequency: str) -> datetime:
        """Calculate the next run time for a frequency based on market opening with 5-second offset"""
        current_time = datetime.now(self.et_timezone)
        interval_seconds = self.frequency_intervals[frequency]
        offset_seconds = 5  # 5-second offset to give API time to reflect latest data
        
        # Calculate market open time for today with offset
        market_open_today = self.et_timezone.localize(
            datetime.combine(current_time.date(), self.market_open)
        ) + timedelta(seconds=offset_seconds)
        
        # If we're before market open, start from market open + offset
        if current_time < market_open_today:
            return market_open_today
        
        # Calculate elapsed time since market open (with offset)
        elapsed_seconds = (current_time - market_open_today).total_seconds()
        
        # Calculate next interval boundary with offset
        intervals_passed = int(elapsed_seconds // interval_seconds)
        next_interval_start = market_open_today + timedelta(seconds=(intervals_passed + 1) * interval_seconds)
        
        return next_interval_start
    
    def frequency_worker(self, frequency: str):
        """Worker thread for a specific frequency"""
        self.logger.info(f"🚀 Starting {frequency} worker thread")
        
        while self.running:
            # Check if we're in market hours and it's a market day
            if not self.is_market_day() or not self.is_market_hours():
                self.logger.info(f"📅 {frequency} worker: Outside market hours, sleeping...")
                time.sleep(60)  # Check every minute
                continue
            
            # Calculate next run time
            next_run = self.calculate_next_run_time(frequency)
            current_time = datetime.now(self.et_timezone)
            
            # Wait until next run time
            if current_time < next_run:
                wait_seconds = (next_run - current_time).total_seconds()
                if wait_seconds > 0:
                    self.logger.info(f"⏰ {frequency} worker: Next run at {next_run.strftime('%H:%M:%S')}, waiting {wait_seconds:.0f}s")
                    time.sleep(min(wait_seconds, 60))  # Check every minute or until run time
                    continue
            
            # Execute trading logic for all symbols at this frequency
            self.logger.info(f"🎯 EXECUTING {frequency.upper()} TRADING CYCLE")
            self.logger.info("-" * 50)
            
            for symbol in self.symbols:
                if not self.running:
                    break
                
                try:
                    self.logger.info(f"📊 Processing {symbol}_{frequency}...")
                    success = self.coordinator.run_scheduled_execution(symbol, frequency)
                    
                    if success:
                        self.logger.info(f"✅ {symbol}_{frequency} execution completed")
                    else:
                        self.logger.error(f"❌ {symbol}_{frequency} execution failed")
                        
                except Exception as e:
                    self.logger.error(f"❌ Error processing {symbol}_{frequency}: {e}")
            
            self.logger.info(f"🏁 {frequency.upper()} cycle completed")
            time.sleep(5)  # Small delay before next cycle
    
    def health_check_worker(self):
        """Health check worker that runs every 4 minutes"""
        self.logger.info("🏥 Starting health check worker")
        
        while self.running:
            time.sleep(self.health_check_interval)
            
            if not self.running:
                break
            
            current_time = datetime.now(self.et_timezone)
            self.logger.info(f"\n🏥 HEALTH CHECK: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ET")
            self.logger.info("=" * 60)
            
            # Check market status
            market_day = self.is_market_day()
            market_hours = self.is_market_hours()
            
            self.logger.info(f"📅 Market Day: {'✅ Yes' if market_day else '❌ No (Weekend)'}")
            self.logger.info(f"🕒 Market Hours: {'✅ Yes' if market_hours else '❌ No'}")
            
            # Check authentication
            auth_valid = self.schwab_auth.is_authenticated()
            self.logger.info(f"🔐 Authentication: {'✅ Valid' if auth_valid else '❌ Invalid'}")
            
            if not auth_valid:
                self.logger.warning("⚠️  Authentication expired - attempting refresh...")
                self.schwab_auth.refresh_access_token()
            
            # Check thread status
            active_threads = sum(1 for thread in self.threads.values() if thread.is_alive())
            expected_threads = len(self.frequencies)
            
            self.logger.info(f"🧵 Worker Threads: {active_threads}/{expected_threads} active")
            
            if market_day and market_hours and active_threads < expected_threads:
                self.logger.warning("⚠️  Some worker threads are down - system may need restart")
            
            # Display position summary
            try:
                positions = self.coordinator.position_tracker.get_position_status()
                self.logger.info("📊 Current Positions:")
                for period, status in positions.items():
                    self.logger.info(f"   {period}: {status}")
            except Exception as e:
                self.logger.error(f"❌ Error getting position status: {e}")
            
            self.logger.info("🏥 Health check completed")
    
    def start(self):
        """Start the continuous trading system"""
        self.logger.info("🚀 CONTINUOUS TRADING SYSTEM STARTING")
        self.logger.info("=" * 80)
        self.logger.info(f"📊 Symbols: {', '.join(self.symbols)}")
        self.logger.info(f"⏰ Frequencies: {', '.join(self.frequencies)}")
        self.logger.info(f"🏥 Health Check Interval: {self.health_check_interval} seconds")
        
        # Wait for market to open if necessary
        self.wait_for_market_open()
        
        if not self.running:
            self.logger.info("System shutdown requested during market wait")
            return
        
        # Run bootstrap process
        self.logger.info("\n🔄 PHASE 1: BOOTSTRAP")
        bootstrap_success = self.run_bootstrap_for_all()
        
        if not bootstrap_success:
            self.logger.error("❌ Bootstrap failed. Continuing anyway...")
        
        # Start frequency worker threads
        self.logger.info("\n🎯 PHASE 2: STARTING WORKER THREADS")
        for frequency in self.frequencies:
            thread = threading.Thread(
                target=self.frequency_worker,
                args=(frequency,),
                name=f"Worker-{frequency}",
                daemon=True
            )
            thread.start()
            self.threads[frequency] = thread
            self.logger.info(f"✅ Started {frequency} worker thread")
        
        # Start health check thread
        health_thread = threading.Thread(
            target=self.health_check_worker,
            name="HealthCheck",
            daemon=True
        )
        health_thread.start()
        self.threads['health'] = health_thread
        self.logger.info("✅ Started health check thread")
        
        self.logger.info("\n🎯 PHASE 3: CONTINUOUS OPERATION")
        self.logger.info("System is now running continuously...")
        self.logger.info("Press Ctrl+C to stop gracefully")
        
        # Main loop - keep the program alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        
        # Shutdown
        self.logger.info("\n🛑 SHUTTING DOWN CONTINUOUS TRADING SYSTEM")
        self.running = False
        
        # Wait for threads to finish
        for name, thread in self.threads.items():
            if thread.is_alive():
                self.logger.info(f"Waiting for {name} thread to finish...")
                thread.join(timeout=10)
        
        self.logger.info("✅ Continuous trading system shutdown complete")


def main():
    """Main function for continuous trading"""
    parser = argparse.ArgumentParser(description='Continuous Market Trading System')
    parser.add_argument('symbols', help='Comma-separated list of stock symbols (e.g., SPY,META,AMZN)')
    parser.add_argument('--health-interval', type=int, default=240, 
                       help='Health check interval in seconds (default: 240 = 4 minutes)')
    
    args = parser.parse_args()
    
    # Parse symbols
    symbols = [symbol.strip().upper() for symbol in args.symbols.split(',')]
    
    if not symbols:
        print("❌ No symbols provided")
        sys.exit(1)
    
    # Create and start the continuous trader
    trader = ContinuousTrader(symbols)
    trader.health_check_interval = args.health_interval
    
    try:
        trader.start()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 