#!/usr/bin/env python3
"""
Status Check Module
Provides system status and health checks for the Schwab Market Data system
"""

import os
import pandas as pd
from datetime import datetime
import pytz
from typing import Dict, List, Optional

# Import our modular components
from data_fetcher import DataFetcher
from indicator_calculator import IndicatorCalculator
from position_tracker import PositionTracker
from email_notifier import EmailNotifier
from schwab_auth import SchwabAuth

class StatusChecker:
    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.indicator_calculator = IndicatorCalculator()
        self.position_tracker = PositionTracker()
        self.email_notifier = EmailNotifier()
        self.schwab_auth = SchwabAuth()
        self.et_timezone = pytz.timezone('US/Eastern')
        
    def check_authentication_status(self) -> bool:
        """Check if Schwab authentication is working"""
        print("🔐 Checking Schwab API authentication...")
        
        is_authenticated = self.schwab_auth.is_authenticated()
        
        if is_authenticated:
            print("✅ Schwab API authentication: Valid")
            return True
        else:
            print("❌ Schwab API authentication: Failed")
            return False
    
    def check_email_configuration(self) -> bool:
        """Check if email notifications are configured"""
        print("📧 Checking email notification configuration...")
        
        try:
            # Try to initialize email notifier
            if hasattr(self.email_notifier, 'smtp_server') and self.email_notifier.smtp_server:
                print("✅ Email notifications: Configured")
                return True
            else:
                print("❌ Email notifications: Not configured")
                return False
        except Exception as e:
            print(f"❌ Email notifications: Error - {e}")
            return False
    
    def check_data_files(self, symbols: List[str] = None, periods: List[str] = None) -> Dict:
        """Check status of data files"""
        if symbols is None:
            symbols = ['SPY', 'META', 'AMZN']
        if periods is None:
            periods = ['5m', '10m', '15m', '30m']
            
        print(f"📊 Checking data files for {len(symbols)} symbols × {len(periods)} timeframes...")
        
        results = {}
        total_files = 0
        existing_files = 0
        
        for symbol in symbols:
            results[symbol] = {}
            for period in periods:
                # Check regular data file
                regular_path = self.data_fetcher.get_csv_path(symbol, period, inverse=False)
                regular_exists = os.path.exists(regular_path)
                
                # Check inverse data file  
                inverse_path = self.data_fetcher.get_csv_path(symbol, period, inverse=True)
                inverse_exists = os.path.exists(inverse_path)
                
                # Get file info if exists
                regular_info = None
                inverse_info = None
                
                if regular_exists:
                    df = pd.read_csv(regular_path)
                    regular_info = {
                        'rows': len(df),
                        'latest_timestamp': df['timestamp'].max() if 'timestamp' in df.columns and not df.empty else None
                    }
                    existing_files += 1
                
                if inverse_exists:
                    df = pd.read_csv(inverse_path)
                    inverse_info = {
                        'rows': len(df),
                        'latest_timestamp': df['timestamp'].max() if 'timestamp' in df.columns and not df.empty else None
                    }
                    existing_files += 1
                
                results[symbol][period] = {
                    'regular': {'exists': regular_exists, 'info': regular_info},
                    'inverse': {'exists': inverse_exists, 'info': inverse_info}
                }
                
                total_files += 2  # regular + inverse
        
        print(f"📊 Data files: {existing_files}/{total_files} files exist")
        return results
    
    def get_system_summary(self, symbols: List[str] = None) -> Dict:
        """Get complete system status summary"""
        if symbols is None:
            symbols = ['SPY', 'META', 'AMZN']
            
        print("🔍 Generating system status summary...")
        print("=" * 60)
        
        # Check all components
        auth_status = self.check_authentication_status()
        email_status = self.check_email_configuration()
        data_status = self.check_data_files(symbols)
        
        # Count data availability
        total_streams = 0
        active_streams = 0
        
        for symbol in symbols:
            for period in ['5m', '10m', '15m', '30m']:
                total_streams += 1
                if (symbol in data_status and 
                    period in data_status[symbol] and
                    data_status[symbol][period]['regular']['exists'] and
                    data_status[symbol][period]['inverse']['exists']):
                    active_streams += 1
        
        summary = {
            'authentication': auth_status,
            'email_notifications': email_status,
            'data_streams': f"{active_streams}/{total_streams}",
            'symbols_monitored': symbols,
            'timeframes': ['5m', '10m', '15m', '30m'],
            'system_ready': auth_status and email_status and (active_streams > 0),
            'timestamp': datetime.now(self.et_timezone).strftime('%Y-%m-%d %H:%M:%S ET')
        }
        
        # Display summary
        print(f"\n📈 System Status Summary:")
        print(f"   Authentication: {'✅ Ready' if summary['authentication'] else '❌ Failed'}")
        print(f"   Email Notifications: {'✅ Ready' if summary['email_notifications'] else '❌ Not configured'}")
        print(f"   Data Streams: {summary['data_streams']} active")
        print(f"   Symbols: {', '.join(summary['symbols_monitored'])}")
        print(f"   Timeframes: {', '.join(summary['timeframes'])}")
        print(f"   Overall Status: {'🚀 READY' if summary['system_ready'] else '⚠️ NEEDS ATTENTION'}")
        print(f"   Last Check: {summary['timestamp']}")
        
        return summary

def main():
    """Main function for status checking"""
    checker = StatusChecker()
    
    print("🚀 Schwab Market Data System - Status Check")
    print("=" * 50)
    
    # Get system summary
    summary = checker.get_system_summary()
    
    if summary['system_ready']:
        print(f"\n✅ System is ready for scheduled execution!")
        print(f"\n📋 Next Steps:")
        print(f"   Set up cron jobs: Edit cron_jobs.md")
        print(f"   Test execution:   python scheduled_coordinator.py SPY 5m")
        print(f"   Monitor logs:     tail -f /var/log/schwab_*.log")
    else:
        print(f"\n⚠️  System needs attention before deployment")
        print(f"\n📋 Troubleshooting:")
        if not summary['authentication']:
            print(f"   - Check Schwab API credentials")
        if not summary['email_notifications']:
            print(f"   - Configure email settings in email_credentials.env")
        if summary['data_streams'] == '0/12':
            print(f"   - Run bootstrap: python scheduled_coordinator.py SPY 5m --mode bootstrap")

if __name__ == "__main__":
    main() 