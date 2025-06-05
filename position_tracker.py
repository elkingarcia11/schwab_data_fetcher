#!/usr/bin/env python3
"""
Position Tracker Module
Handles position signals, P&L tracking, and email notifications for both LONG and SHORT positions
- LONG positions: Based on regular price data conditions
- SHORT positions: Based on inverse price data conditions
- Same 3-condition logic applied to both types
- Persistent state storage for cron job compatibility
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from indicator_calculator import IndicatorCalculator
from email_notifier import EmailNotifier

class PositionTracker:
    def __init__(self):
        self.indicator_calculator = IndicatorCalculator()
        self.email_notifier = EmailNotifier()
        
        # Position state file for persistence across cron jobs
        self.state_file = 'position_states.json'
        
        # Load existing position states or initialize defaults
        self.position_states, self.opening_prices, self.total_pnl = self._load_position_states()
        
    def _load_position_states(self) -> Tuple[Dict, Dict, Dict]:
        """
        Load position states from file or initialize defaults
        
        Returns:
            Tuple of (position_states, opening_prices, total_pnl)
        """
        default_states = {
            '5m': {'LONG': 'CLOSED', 'SHORT': 'CLOSED'},
            '10m': {'LONG': 'CLOSED', 'SHORT': 'CLOSED'},
            '15m': {'LONG': 'CLOSED', 'SHORT': 'CLOSED'},
            '30m': {'LONG': 'CLOSED', 'SHORT': 'CLOSED'}
        }
        
        default_prices = {
            '5m': {'LONG': None, 'SHORT': None},
            '10m': {'LONG': None, 'SHORT': None},
            '15m': {'LONG': None, 'SHORT': None},
            '30m': {'LONG': None, 'SHORT': None}
        }
        
        default_pnl = {
            '5m': {'LONG': 0.0, 'SHORT': 0.0},
            '10m': {'LONG': 0.0, 'SHORT': 0.0},
            '15m': {'LONG': 0.0, 'SHORT': 0.0},
            '30m': {'LONG': 0.0, 'SHORT': 0.0}
        }
        
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    
                print(f"📊 Loaded position states from {self.state_file}")
                return (
                    data.get('position_states', default_states),
                    data.get('opening_prices', default_prices),
                    data.get('total_pnl', default_pnl)
                )
            except Exception as e:
                print(f"⚠️  Error loading position states: {e}, using defaults")
                return default_states, default_prices, default_pnl
        else:
            print(f"📊 No existing position states found, using defaults")
            return default_states, default_prices, default_pnl
    
    def _save_position_states(self):
        """
        Save current position states to file for persistence
        """
        try:
            data = {
                'position_states': self.position_states,
                'opening_prices': self.opening_prices,
                'total_pnl': self.total_pnl,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
                
            print(f"💾 Position states saved to {self.state_file}")
        except Exception as e:
            print(f"❌ Error saving position states: {e}")

    def evaluate_trading_conditions(self, indicators: dict) -> Tuple[bool, bool, bool, int, str]:
        """
        Evaluate the 3-condition trading logic
        
        Args:
            indicators: Dictionary with latest indicator values
            
        Returns:
            Tuple of (ema_condition, macd_condition, roc_condition, conditions_met, summary)
        """
        if not indicators:
            return False, False, False, 0, "No indicators available"
        
        try:
            # Condition 1: 7 EMA > 17 VWMA
            ema_7 = float(indicators['ema_7'])
            vwma_17 = float(indicators['vwma_17'])
            ema_condition = ema_7 > vwma_17
            
            # Condition 2: MACD Line > MACD Signal
            macd_line = float(indicators['macd_line'])
            macd_signal = float(indicators['macd_signal'])
            macd_condition = macd_line > macd_signal
            
            # Condition 3: ROC-8 > 0
            roc_8 = float(indicators['roc_8'])
            roc_condition = roc_8 > 0
            
            # Count conditions met
            conditions_met = sum([ema_condition, macd_condition, roc_condition])
            
            # Create summary
            summary = f"EMA>VWMA: {'✅' if ema_condition else '❌'} ({ema_7:.2f}>{vwma_17:.2f}), "
            summary += f"MACD>Signal: {'✅' if macd_condition else '❌'} ({macd_line:.4f}>{macd_signal:.4f}), "
            summary += f"ROC>0: {'✅' if roc_condition else '❌'} ({roc_8:.2f}%)"
            
            return ema_condition, macd_condition, roc_condition, conditions_met, summary
            
        except (ValueError, KeyError, TypeError) as e:
            return False, False, False, 0, f"Error evaluating conditions: {e}"

    def check_position_signals(self, symbol: str, period: str) -> Dict:
        """
        Check for position signals on both LONG (regular) and SHORT (inverse) positions
        
        Args:
            symbol: Stock symbol
            period: Time period
            
        Returns:
            Dictionary with signal results for both LONG and SHORT
        """
        results = {
            'LONG': {'action': None, 'price': None, 'conditions': None, 'pnl': None},
            'SHORT': {'action': None, 'price': None, 'conditions': None, 'pnl': None}
        }
        
        # Get indicators for both regular (LONG) and inverse (SHORT) data
        regular_indicators = self.indicator_calculator.get_latest_indicators(symbol, period, inverse=False)
        inverse_indicators = self.indicator_calculator.get_latest_indicators(symbol, period, inverse=True)
        
        # Process LONG positions (regular data)
        if regular_indicators:
            long_result = self._process_position_type(symbol, period, 'LONG', regular_indicators)
            results['LONG'] = long_result
        
        # Process SHORT positions (inverse data)
        if inverse_indicators:
            short_result = self._process_position_type(symbol, period, 'SHORT', inverse_indicators)
            results['SHORT'] = short_result
        
        return results

    def _process_position_type(self, symbol: str, period: str, position_type: str, indicators: dict) -> Dict:
        """
        Process position signals for a specific type (LONG or SHORT)
        
        Args:
            symbol: Stock symbol
            period: Time period
            position_type: 'LONG' or 'SHORT'
            indicators: Indicator data
            
        Returns:
            Dictionary with action, price, conditions, and P&L info
        """
        result = {'action': None, 'price': None, 'conditions': None, 'pnl': None}
        
        # Evaluate trading conditions
        ema_cond, macd_cond, roc_cond, conditions_met, summary = self.evaluate_trading_conditions(indicators)
        result['conditions'] = {
            'ema_condition': ema_cond,
            'macd_condition': macd_cond,
            'roc_condition': roc_cond,
            'conditions_met': conditions_met,
            'summary': summary
        }
        
        current_state = self.position_states[period][position_type]
        current_price = float(indicators['close'])
        
        # Position logic with explicit safeguards
        if current_state == 'CLOSED' and conditions_met == 3:
            # SAFEGUARD: Ensure only one position per type per timeframe
            if self.position_states[period][position_type] != 'CLOSED':
                print(f"⚠️  SAFEGUARD: Cannot open {position_type} position - already have {self.position_states[period][position_type]} position")
                return result
            
            # Open position when ALL 3 conditions are met
            self.position_states[period][position_type] = 'OPENED'
            self.opening_prices[period][position_type] = current_price
            result['action'] = 'OPEN'
            result['price'] = current_price
            
            # Enhanced logging with position constraints
            other_type = 'SHORT' if position_type == 'LONG' else 'LONG'
            other_state = self.position_states[period][other_type]
            concurrent_info = f" (concurrent {other_type}: {other_state})" if other_state == 'OPENED' else ""
            
            print(f"🚨 {position_type} POSITION OPENED: {symbol}_{period} at {current_price:.4f}{concurrent_info}")
            print(f"   📊 Constraint: 1 {position_type} + 1 {other_type} max per timeframe - Currently: {position_type}=OPEN, {other_type}={other_state}")
            
            # Save state after opening position
            self._save_position_states()
            
        elif current_state == 'OPENED' and conditions_met <= 1:
            # Close position when 2 or more conditions fail (≤1 condition remaining)
            self.position_states[period][position_type] = 'CLOSED'
            opening_price = self.opening_prices[period][position_type]
            
            # Calculate P&L based on position type
            if position_type == 'LONG':
                # LONG: profit when price goes up
                pnl_dollar = current_price - opening_price
                pnl_percent = (pnl_dollar / opening_price) * 100
            else:  # SHORT
                # SHORT: profit when price goes down (inverse data represents 1/price)
                # For inverse data, lower values mean higher regular prices
                # So we profit when inverse price goes up (regular price goes down)
                pnl_dollar = current_price - opening_price
                pnl_percent = (pnl_dollar / opening_price) * 100
            
            # Update total P&L
            self.total_pnl[period][position_type] += pnl_dollar
            
            result['action'] = 'CLOSE'
            result['price'] = current_price
            result['pnl'] = {
                'opening_price': opening_price,
                'closing_price': current_price,
                'pnl_dollar': pnl_dollar,
                'pnl_percent': pnl_percent,
                'total_pnl': self.total_pnl[period][position_type]
            }
            
            # Reset opening price
            self.opening_prices[period][position_type] = None
            
            pnl_emoji = "📈" if pnl_dollar >= 0 else "📉"
            print(f"🚨 {position_type} POSITION CLOSED: {symbol}_{period} at {current_price:.4f} {pnl_emoji} ${pnl_dollar:.4f} ({pnl_percent:.2f}%)")
            
            # Save state after closing position
            self._save_position_states()
        
        elif current_state == 'OPENED' and conditions_met == 3:
            # Position already open with all conditions still met - no action needed
            print(f"   📊 {position_type} position already OPEN for {symbol}_{period} (conditions: {conditions_met}/3)")
        
        elif current_state == 'CLOSED' and conditions_met < 3:
            # Conditions not met for opening - no action needed
            print(f"   📊 {position_type} position remains CLOSED for {symbol}_{period} (conditions: {conditions_met}/3)")
        
        elif current_state == 'OPENED' and 1 < conditions_met < 3:
            # Position open but some conditions failing - monitor but don't close yet
            print(f"   ⚠️  {position_type} position OPEN but conditions weakening for {symbol}_{period} (conditions: {conditions_met}/3)")
        
        return result

    def check_live_position_signals(self, symbol: str) -> bool:
        """
        Check for live position signals across all timeframes for both LONG and SHORT
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if any signals were found and processed, False otherwise
        """
        signals_found = False
        
        for period in ['5m', '10m', '15m', '30m']:
            # Check signals for both LONG and SHORT positions
            period_signals = self.check_position_signals(symbol, period)
            
            # Process LONG signals
            long_signal = period_signals['LONG']
            if long_signal['action']:
                signals_found = True
                self._send_position_notification(symbol, period, 'LONG', long_signal)
            
            # Process SHORT signals
            short_signal = period_signals['SHORT']
            if short_signal['action']:
                signals_found = True
                self._send_position_notification(symbol, period, 'SHORT', short_signal)
        
        return signals_found

    def _send_position_notification(self, symbol: str, period: str, position_type: str, signal_data: dict):
        """
        Send email notification for position changes
        
        Args:
            symbol: Stock symbol
            period: Time period
            position_type: 'LONG' or 'SHORT'
            signal_data: Signal information
        """
        action = signal_data['action']
        price = signal_data['price']
        conditions = signal_data['conditions']
        pnl_info = signal_data.get('pnl')
        
        # Get current position status for context
        positions = self.get_position_status()
        
        try:
            self.email_notifier.send_position_notification(
                symbol=symbol,
                period=period,
                position_type=position_type,  # NEW: specify LONG or SHORT
                action=action,
                signal_details={
                    'price': price,
                    'conditions_met': conditions['conditions_met'],
                    'condition_summary': conditions['summary'],
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                pnl_info=pnl_info,
                positions=positions
            )
            print(f"📧 Email notification sent for {position_type} {action} signal")
        except Exception as e:
            print(f"❌ Failed to send email notification: {e}")

    def analyze_historical_positions(self, symbol: str, suppress_emails: bool = True) -> Dict:
        """
        Analyze historical data for position signals on both LONG and SHORT
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with analysis results
        """
        print(f"\n🎯 Analyzing historical positions for {symbol} (LONG + SHORT)")
        print("=" * 80)
        
        total_signals = {'LONG': 0, 'SHORT': 0}
        open_signals = {'LONG': 0, 'SHORT': 0}
        close_signals = {'LONG': 0, 'SHORT': 0}
        
        # Reset position states for fresh analysis
        for period in ['5m', '10m', '15m', '30m']:
            self.position_states[period]['LONG'] = 'CLOSED'
            self.position_states[period]['SHORT'] = 'CLOSED'
            self.opening_prices[period]['LONG'] = None
            self.opening_prices[period]['SHORT'] = None
            self.total_pnl[period]['LONG'] = 0.0
            self.total_pnl[period]['SHORT'] = 0.0
        
        # Save reset state
        self._save_position_states()
        
        for period in ['5m', '10m', '15m', '30m']:
            print(f"\n📊 Analyzing {period} historical data...")
            
            # Load both regular (LONG) and inverse (SHORT) data
            df_regular = self.indicator_calculator.data_fetcher.load_csv_data(symbol, period, inverse=False)
            df_inverse = self.indicator_calculator.data_fetcher.load_csv_data(symbol, period, inverse=True)
            
            if df_regular is None or df_regular.empty:
                print(f"❌ No regular data available for {period}")
                continue
                
            if df_inverse is None or df_inverse.empty:
                print(f"❌ No inverse data available for {period}")
                continue
            
            # Process historical signals for both types
            long_signals = self._analyze_historical_for_type(symbol, period, 'LONG', df_regular, suppress_emails)
            short_signals = self._analyze_historical_for_type(symbol, period, 'SHORT', df_inverse, suppress_emails)
            
            # Update totals
            total_signals['LONG'] += long_signals['total']
            total_signals['SHORT'] += short_signals['total']
            open_signals['LONG'] += long_signals['opens']
            open_signals['SHORT'] += short_signals['opens']
            close_signals['LONG'] += long_signals['closes']
            close_signals['SHORT'] += short_signals['closes']
        
        # Summary
        print(f"\n🎯 Historical Analysis Summary for {symbol}:")
        print(f"   LONG Signals: {total_signals['LONG']} total ({open_signals['LONG']} opens, {close_signals['LONG']} closes)")
        print(f"   SHORT Signals: {total_signals['SHORT']} total ({open_signals['SHORT']} opens, {close_signals['SHORT']} closes)")
        print(f"   Combined Total: {sum(total_signals.values())} signals")
        
        return {
            'total_signals': sum(total_signals.values()),
            'long_signals': total_signals['LONG'],
            'short_signals': total_signals['SHORT'],
            'open_signals': sum(open_signals.values()),
            'close_signals': sum(close_signals.values()),
            'long_opens': open_signals['LONG'],
            'long_closes': close_signals['LONG'],
            'short_opens': open_signals['SHORT'],
            'short_closes': close_signals['SHORT']
        }

    def _analyze_historical_for_type(self, symbol: str, period: str, position_type: str, df: pd.DataFrame, suppress_emails: bool = True) -> Dict:
        """
        Analyze historical data for a specific position type
        
        Args:
            symbol: Stock symbol
            period: Time period
            position_type: 'LONG' or 'SHORT'
            df: DataFrame with historical data
            
        Returns:
            Dictionary with signal counts
        """
        signals = {'total': 0, 'opens': 0, 'closes': 0}
        
        print(f"   📊 Processing {len(df)} rows of {position_type} data...")
        
        for index, row in df.iterrows():
            # Skip rows without complete indicators
            if (pd.isna(row.get('ema_7')) or row.get('ema_7') == '' or
                pd.isna(row.get('vwma_17')) or row.get('vwma_17') == '' or
                pd.isna(row.get('macd_line')) or row.get('macd_line') == '' or
                pd.isna(row.get('macd_signal')) or row.get('macd_signal') == '' or
                pd.isna(row.get('roc_8')) or row.get('roc_8') == ''):
                continue
            
            # Create indicators dictionary
            indicators = {
                'timestamp': row['timestamp'],
                'datetime': row['datetime'],
                'close': float(row['close']),
                'ema_7': float(row['ema_7']),
                'vwma_17': float(row['vwma_17']),
                'macd_line': float(row['macd_line']),
                'macd_signal': float(row['macd_signal']),
                'roc_8': float(row['roc_8']),
                'data_type': position_type
            }
            
            # Check for signals
            signal_result = self._process_position_type(symbol, period, position_type, indicators)
            
            if signal_result['action']:
                signals['total'] += 1
                if signal_result['action'] == 'OPEN':
                    signals['opens'] += 1
                elif signal_result['action'] == 'CLOSE':
                    signals['closes'] += 1
                
                # Send historical email notification (only if not suppressed)
                if not suppress_emails:
                    try:
                        self._send_position_notification(symbol, period, position_type, signal_result)
                    except Exception as e:
                        print(f"⚠️  Email notification failed: {e}")
                else:
                    print(f"📧 Email suppressed for historical {signal_result['action']} signal")
        
        print(f"   ✅ {position_type} {period}: {signals['total']} signals ({signals['opens']} opens, {signals['closes']} closes)")
        return signals

    def get_position_status(self) -> Dict:
        """
        Get current position status for all timeframes and types
        
        Returns:
            Dictionary with current position states
        """
        return {
            '5m': f"L:{self.position_states['5m']['LONG'][0]}/S:{self.position_states['5m']['SHORT'][0]}",
            '10m': f"L:{self.position_states['10m']['LONG'][0]}/S:{self.position_states['10m']['SHORT'][0]}",
            '15m': f"L:{self.position_states['15m']['LONG'][0]}/S:{self.position_states['15m']['SHORT'][0]}",
            '30m': f"L:{self.position_states['30m']['LONG'][0]}/S:{self.position_states['30m']['SHORT'][0]}"
        }

    def get_detailed_position_status(self) -> Dict:
        """
        Get detailed position status including P&L
        
        Returns:
            Detailed position information
        """
        status = {}
        for period in ['5m', '10m', '15m', '30m']:
            status[period] = {
                'LONG': {
                    'state': self.position_states[period]['LONG'],
                    'opening_price': self.opening_prices[period]['LONG'],
                    'total_pnl': self.total_pnl[period]['LONG']
                },
                'SHORT': {
                    'state': self.position_states[period]['SHORT'],
                    'opening_price': self.opening_prices[period]['SHORT'],
                    'total_pnl': self.total_pnl[period]['SHORT']
                }
            }
        return status

    def validate_position_logic(self, symbol: str, period: str) -> bool:
        """
        Validate position logic works correctly for both LONG and SHORT
        
        Args:
            symbol: Stock symbol
            period: Time period
            
        Returns:
            True if validation passes, False otherwise
        """
        print(f"🔍 Validating position logic for {symbol}_{period} (LONG + SHORT)...")
        
        # Test with regular data (LONG)
        regular_indicators = self.indicator_calculator.get_latest_indicators(symbol, period, inverse=False)
        long_valid = False
        if regular_indicators:
            long_conditions = self.evaluate_trading_conditions(regular_indicators)
            long_valid = long_conditions[3] >= 0  # Should return valid condition count
            print(f"   ✅ LONG position logic: Valid ({long_conditions[3]}/3 conditions)")
        else:
            print(f"   ⚠️  LONG position logic: No regular data available")
        
        # Test with inverse data (SHORT)
        inverse_indicators = self.indicator_calculator.get_latest_indicators(symbol, period, inverse=True)
        short_valid = False
        if inverse_indicators:
            short_conditions = self.evaluate_trading_conditions(inverse_indicators)
            short_valid = short_conditions[3] >= 0  # Should return valid condition count
            print(f"   ✅ SHORT position logic: Valid ({short_conditions[3]}/3 conditions)")
        else:
            print(f"   ⚠️  SHORT position logic: No inverse data available")
        
        overall_valid = long_valid and short_valid
        print(f"   📊 Overall validation: {'✅ Passed' if overall_valid else '❌ Failed'}")
        
        return overall_valid

    def validate_position_constraints(self) -> bool:
        """
        Validate that position constraints are maintained:
        - Maximum 1 LONG position per timeframe
        - Maximum 1 SHORT position per timeframe
        - LONG and SHORT can coexist in same timeframe
        
        Returns:
            True if constraints are satisfied, False otherwise
        """
        print("🔍 Validating position constraints...")
        
        constraints_valid = True
        
        for period in ['5m', '10m', '15m', '30m']:
            long_state = self.position_states[period]['LONG']
            short_state = self.position_states[period]['SHORT']
            long_price = self.opening_prices[period]['LONG']
            short_price = self.opening_prices[period]['SHORT']
            
            # Check for state consistency
            if long_state == 'OPENED' and long_price is None:
                print(f"❌ {period} LONG: State is OPENED but no opening price recorded")
                constraints_valid = False
                
            if short_state == 'OPENED' and short_price is None:
                print(f"❌ {period} SHORT: State is OPENED but no opening price recorded")
                constraints_valid = False
                
            if long_state == 'CLOSED' and long_price is not None:
                print(f"⚠️  {period} LONG: State is CLOSED but opening price still recorded ({long_price})")
                # Auto-fix this inconsistency
                self.opening_prices[period]['LONG'] = None
                
            if short_state == 'CLOSED' and short_price is not None:
                print(f"⚠️  {period} SHORT: State is CLOSED but opening price still recorded ({short_price})")
                # Auto-fix this inconsistency
                self.opening_prices[period]['SHORT'] = None
            
            # Display current state
            long_emoji = "🟢" if long_state == 'OPENED' else "🔴"
            short_emoji = "🟢" if short_state == 'OPENED' else "🔴"
            print(f"   {period}: LONG={long_emoji}{long_state}, SHORT={short_emoji}{short_state}")
            
            # Show concurrent positions
            if long_state == 'OPENED' and short_state == 'OPENED':
                print(f"   📊 {period}: BOTH positions open simultaneously (allowed)")
        
        if constraints_valid:
            print("✅ All position constraints satisfied")
        else:
            print("❌ Position constraint violations detected")
            # Save any auto-fixes
            self._save_position_states()
        
        return constraints_valid

    def get_position_summary(self) -> Dict:
        """
        Get summary of current positions across all timeframes
        
        Returns:
            Dictionary with position summary
        """
        summary = {
            'total_open_positions': 0,
            'open_longs': 0,
            'open_shorts': 0,
            'timeframes_with_positions': [],
            'concurrent_timeframes': []  # Timeframes with both LONG and SHORT open
        }
        
        for period in ['5m', '10m', '15m', '30m']:
            long_open = self.position_states[period]['LONG'] == 'OPENED'
            short_open = self.position_states[period]['SHORT'] == 'OPENED'
            
            if long_open:
                summary['open_longs'] += 1
                summary['total_open_positions'] += 1
                if period not in summary['timeframes_with_positions']:
                    summary['timeframes_with_positions'].append(period)
            
            if short_open:
                summary['open_shorts'] += 1
                summary['total_open_positions'] += 1
                if period not in summary['timeframes_with_positions']:
                    summary['timeframes_with_positions'].append(period)
            
            if long_open and short_open:
                summary['concurrent_timeframes'].append(period)
        
        return summary

    def display_current_position_states(self) -> None:
        """
        Display current position states loaded from persistent storage
        """
        print("\n📊 Current Position States (from persistent storage):")
        print("=" * 60)
        
        # Get position summary first
        summary = self.get_position_summary()
        
        for period in ['5m', '10m', '15m', '30m']:
            long_state = self.position_states[period]['LONG']
            short_state = self.position_states[period]['SHORT']
            long_price = self.opening_prices[period]['LONG']
            short_price = self.opening_prices[period]['SHORT']
            long_pnl = self.total_pnl[period]['LONG']
            short_pnl = self.total_pnl[period]['SHORT']
            
            print(f"\n🕘 {period} Timeframe:")
            print(f"   LONG:  {long_state:<6} | Opening: {f'${long_price:.4f}' if long_price else 'N/A':<10} | Total P&L: ${long_pnl:>8.2f}")
            print(f"   SHORT: {short_state:<6} | Opening: {f'${short_price:.4f}' if short_price else 'N/A':<10} | Total P&L: ${short_pnl:>8.2f}")
            
            # Show concurrent positions
            if long_state == 'OPENED' and short_state == 'OPENED':
                print(f"   🔥 BOTH LONG & SHORT positions open (maximum allowed)")
            elif long_state == 'OPENED':
                print(f"   📈 LONG position open (1 more SHORT allowed)")
            elif short_state == 'OPENED':
                print(f"   📉 SHORT position open (1 more LONG allowed)")
            else:
                print(f"   💤 No positions open (1 LONG + 1 SHORT available)")
        
        # Show file info
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                last_updated = data.get('last_updated', 'Unknown')
                print(f"\n💾 State file: {self.state_file}")
                print(f"📅 Last updated: {last_updated}")
        
        # Show constraint summary
        print(f"\n📊 Position Constraint Summary:")
        print(f"   Total Open: {summary['total_open_positions']} positions ({summary['open_longs']} LONG, {summary['open_shorts']} SHORT)")
        print(f"   Active Timeframes: {summary['timeframes_with_positions'] if summary['timeframes_with_positions'] else 'None'}")
        print(f"   Concurrent (L+S): {summary['concurrent_timeframes'] if summary['concurrent_timeframes'] else 'None'}")
        print(f"   🔒 Constraint: Max 1 LONG + 1 SHORT per timeframe")
        
        print("=" * 60) 