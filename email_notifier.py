#!/usr/bin/env python3
"""
Email Notifier Module
Handles email notifications for position changes, including LONG and SHORT positions
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Optional

class EmailNotifier:
    def __init__(self):
        self.enabled = False
        self.sender = None
        self.password = None
        self.recipients = []
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        
        self._load_credentials()
    
    def _load_credentials(self):
        """Load email credentials from environment file"""
        try:
            env_file = "email_credentials.env"
            if not os.path.exists(env_file):
                print("📧 Email credentials file not found, email notifications disabled")
                return
            
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        if key == "EMAIL_ALERTS_ENABLED":
                            self.enabled = value.lower() == 'true'
                        elif key == "EMAIL_SENDER":
                            self.sender = value
                        elif key == "EMAIL_PASSWORD":
                            self.password = value
                        elif key == "EMAIL_TO":
                            self.recipients = [email.strip() for email in value.split(',')]
                        elif key == "EMAIL_SMTP_SERVER":
                            self.smtp_server = value
                        elif key == "EMAIL_SMTP_PORT":
                            self.smtp_port = int(value)
            
            if self.enabled:
                print(f"📧 Email notifications enabled for {len(self.recipients)} recipients")
            else:
                print("📧 Email notifications disabled in configuration")
                
        except Exception as e:
            print(f"❌ Error loading email credentials: {e}")
            self.enabled = False
    
    def test_configuration(self) -> bool:
        """
        Test email configuration
        
        Returns:
            True if configuration is valid, False otherwise
        """
        if not self.enabled:
            print("📧 Email notifications are disabled")
            return False
        
        if not all([self.sender, self.password, self.recipients]):
            print("❌ Email configuration incomplete")
            return False
        
        try:
            # Test SMTP connection
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender, self.password)
            server.quit()
            print("✅ Email configuration test successful")
            return True
        except Exception as e:
            print(f"❌ Email configuration test failed: {e}")
            return False
    
    def send_position_notification(self, symbol: str, period: str, position_type: str, action: str, 
                                 signal_details: Dict, pnl_info: Optional[Dict], positions: Dict) -> bool:
        """
        Send email notification for position changes (LONG or SHORT)
        
        Args:
            symbol: Stock symbol
            period: Time period
            position_type: 'LONG' or 'SHORT'
            action: 'OPEN' or 'CLOSE'
            signal_details: Dictionary with signal information
            pnl_info: P&L information (for CLOSE actions)
            positions: Current position status
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            return True  # Not an error if disabled
        
        try:
            # Extract signal details
            price = signal_details.get('price', 'Unknown')
            conditions_met = signal_details.get('conditions_met', 0)
            condition_summary = signal_details.get('condition_summary', 'Unknown')
            timestamp = signal_details.get('timestamp', 'Unknown')
            
            # Create subject with position type
            if action == 'OPEN':
                subject_emoji = "🟢"
                action_text = f"OPEN {position_type}"
                price_info = f"${price:.4f}" if isinstance(price, (int, float)) else str(price)
            else:  # CLOSE
                if pnl_info and pnl_info.get('pnl_dollar', 0) >= 0:
                    subject_emoji = "📈"
                    pnl_dollar = pnl_info['pnl_dollar']
                    price_info = f"${price:.4f} 📈${pnl_dollar:.4f}"
                else:
                    subject_emoji = "📉"
                    pnl_dollar = pnl_info['pnl_dollar'] if pnl_info else 0
                    price_info = f"${price:.4f} 📉${abs(pnl_dollar):.4f}"
                action_text = f"CLOSE {position_type}"
            
            subject = f"{subject_emoji} {symbol} {period} - {action_text} at {price_info}"
            
            # Create email body with position type information
            body = f"""🚨 {symbol} {period} - {action_text} POSITION at ${price:.4f}

Position Change Details:
- Symbol: {symbol}
- Timeframe: {period}
- Position Type: {position_type} {'(Regular Data)' if position_type == 'LONG' else '(Inverse Data)'}
- Action: {action_text}
- Time: {timestamp}
- Price: ${price:.4f}
- Conditions Met: {conditions_met}/3

Technical Conditions:
{condition_summary}
"""
            
            # Add P&L analysis for CLOSE actions
            if action == 'CLOSE' and pnl_info:
                opening_price = pnl_info.get('opening_price', 0)
                closing_price = pnl_info.get('closing_price', 0)
                pnl_dollar = pnl_info.get('pnl_dollar', 0)
                pnl_percent = pnl_info.get('pnl_percent', 0)
                total_pnl = pnl_info.get('total_pnl', 0)
                
                pnl_emoji = "📈" if pnl_dollar >= 0 else "📉"
                
                body += f"""
P&L Analysis ({position_type} Position):
- Opening Price: ${opening_price:.4f}
- Closing Price: ${closing_price:.4f}
- Profit/Loss: {pnl_emoji} ${pnl_dollar:.4f} ({pnl_percent:+.2f}%)
- Total P&L ({period} {position_type}): ${total_pnl:.4f}
"""
            
            # Add current position status
            body += f"""
Current Positions Status:
- 1m: {positions.get('1m', 'Unknown')}
- 5m: {positions.get('5m', 'Unknown')}  
- 15m: {positions.get('15m', 'Unknown')}

Position Types: L=LONG(Regular), S=SHORT(Inverse)
Trading Logic: Open when ALL 3 conditions met, Close when ≤1 condition remains
"""
            
            # Send email
            return self._send_email(subject, body)
            
        except Exception as e:
            print(f"❌ Error creating position notification: {e}")
            return False
    
    def _send_email(self, subject: str, body: str) -> bool:
        """
        Send email with given subject and body
        
        Args:
            subject: Email subject
            body: Email body
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = ', '.join(self.recipients)
            msg['Subject'] = subject
            
            # Add body
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender, self.password)
            text = msg.as_string()
            server.sendmail(self.sender, self.recipients, text)
            server.quit()
            
            print(f"📧 Email sent successfully to {len(self.recipients)} recipients")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            return False
    
    def send_test_email(self) -> bool:
        """
        Send a test email to verify configuration
        
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            print("📧 Email notifications are disabled")
            return False
        
        subject = "🧪 Schwab Market Data - Email Test"
        body = """This is a test email from the Schwab Market Data Coordinator.

If you receive this email, your email notification configuration is working correctly.

System Features:
- LONG positions (Regular price data)
- SHORT positions (Inverse price data)  
- Multi-timeframe tracking (1m, 5m, 15m)
- Real-time P&L calculations
- Technical indicator signals (EMA, VWMA, MACD, ROC)

Position Types:
- LONG: Based on regular OHLC price data
- SHORT: Based on inverse (1/price) OHLC data
- Same 3-condition logic applied to both types

You will receive notifications when:
- LONG positions open/close based on regular data conditions
- SHORT positions open/close based on inverse data conditions
- P&L analysis for all closed positions

Happy trading! 📈📉
"""
        
        return self._send_email(subject, body) 