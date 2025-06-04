#!/usr/bin/env python3
"""
Quick status check for Schwab Market Data Updater
Checks token validity, data freshness, and system readiness
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone
import pytz
from market_data_updater import MarketDataUpdater

def check_status():
    """Check system status and data freshness"""
    print("🔍 Schwab Market Data System Status Check")
    print("=" * 50)
    
    updater = MarketDataUpdater()
    
    # Check credentials
    print("1️⃣ Checking credentials...")
    app_key, app_secret = updater.load_credentials()
    if app_key and app_secret:
        print("   ✅ Credentials loaded successfully")
    else:
        print("   ❌ Missing or invalid credentials")
        return
    
    # Check refresh token
    print("\n2️⃣ Checking refresh token...")
    if os.path.exists('schwab_refresh_token.txt'):
        print("   ✅ Refresh token file exists")
    else:
        print("   ❌ Refresh token file missing")
        return
    
    # Check access token
    print("\n3️⃣ Checking access token...")
    if updater.is_token_valid():
        print("   ✅ Access token is valid")
    else:
        print("   ⚠️  Access token expired or missing, will refresh on start")
    
    # Check market status
    print("\n4️⃣ Checking market status...")
    current_time = datetime.now(updater.et_timezone)
    is_market_day = updater.is_market_day()
    is_market_hours = updater.is_market_hours()
    
    print(f"   Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ET")
    print(f"   Market day: {'✅ Yes' if is_market_day else '❌ No (Weekend)'}")
    print(f"   Market hours: {'✅ Yes' if is_market_hours else '❌ No'}")
    
    # Check data freshness
    print("\n5️⃣ Checking data freshness...")
    symbols = ['SPY']  # Can be expanded
    
    for symbol in symbols:
        print(f"\n   📊 {symbol} Data Status:")
        
        for period in ['1m', '5m', '15m']:
            csv_path = os.path.join(updater.data_dir, f"{symbol}_{period}.csv")
            
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                    if not df.empty and 'timestamp' in df.columns:
                        latest_timestamp = int(df['timestamp'].max())
                        latest_datetime = datetime.fromtimestamp(latest_timestamp / 1000)
                        age_minutes = (current_time.timestamp() * 1000 - latest_timestamp) / (1000 * 60)
                        
                        if age_minutes < 60:
                            status = f"✅ Fresh ({age_minutes:.0f}min ago)"
                        elif age_minutes < 24 * 60:
                            status = f"⚠️  Stale ({age_minutes/60:.1f}h ago)"
                        else:
                            status = f"❌ Old ({age_minutes/(60*24):.1f}d ago)"
                        
                        # Check indicator columns
                        expected_indicators = ['ema_7', 'vwma_17', 'ema_12', 'ema_26', 'macd_line', 'macd_signal', 'roc_8']
                        available_indicators = [col for col in expected_indicators if col in df.columns]
                        
                        print(f"      {period}: {status} - Latest: {latest_datetime.strftime('%m/%d %H:%M')}")
                        print(f"           Indicators: {len(available_indicators)}/7 ({', '.join(available_indicators)})")
                    else:
                        print(f"      {period}: ❌ Empty or invalid data")
                except Exception as e:
                    print(f"      {period}: ❌ Error reading file: {e}")
            else:
                print(f"      {period}: ❌ File not found")
    
    # System recommendations
    print("\n6️⃣ System Recommendations:")
    
    if not is_market_day:
        print("   📅 Weekend detected - system will not collect data")
    elif not is_market_hours:
        if current_time.time() < updater.market_open:
            print("   🌅 Pre-market - system will wait for 9:30 AM ET")
        else:
            print("   🌙 Post-market - system stopped collecting data at 4:00 PM ET")
    else:
        print("   🚀 Market is open - system should be collecting data")
    
    print("\n📋 Quick Commands:")
    print("   Start continuous: python market_data_updater.py")
    print("   Single update:    python market_data_updater.py --single")
    print("   Help:             python market_data_updater.py --help")


if __name__ == "__main__":
    try:
        check_status()
    except KeyboardInterrupt:
        print("\n🛑 Status check interrupted")
    except Exception as e:
        print(f"\n❌ Error during status check: {e}") 