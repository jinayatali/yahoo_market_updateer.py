#!/usr/bin/env python3
"""
Yahoo Finance Market Data Updater for GitHub Actions
Updates market data for Yahoo-sourced TSX-V companies
Runs on schedule: Intraday (5 min), Daily (6 AM EST), Weekly (Sunday 6 PM EST)
"""

import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
from datetime import datetime
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

# All 589 Yahoo Finance TSX Venture companies
YAHOO_COMPANIES = ['ABA.V', 'ENA.V', 'GOK.V', 'GOOD.V', 'HIDE.V']

# Firebase configuration (from environment variable)
FIREBASE_PROJECT_ID = 'canada-stocks-3c74f'

# ============================================================================
# INITIALIZATION
# ============================================================================

def initialize_firebase():
    """Initialize Firebase with credentials from environment"""
    if not firebase_admin._apps:
        cred_json = os.environ.get('FIREBASE_CREDENTIALS')
        if not cred_json:
            raise ValueError("FIREBASE_CREDENTIALS environment variable not set")
        
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    
    return firestore.client()

# ============================================================================
# UPDATE FUNCTIONS
# ============================================================================

def update_intraday_data(db):
    """
    Update real-time market data (every 5 minutes during market hours)
    Fields: Price, Volume, DayHigh, DayLow, Change, ChangePercent, Open
    """
    print(f"\n{'='*70}")
    print("INTRADAY UPDATE - Yahoo Finance Companies")
    print(f"{'='*70}")
    print(f"Updating {len(YAHOO_COMPANIES)} companies...")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(YAHOO_COMPANIES, 1):
        try:
            # Fetch data from Yahoo Finance
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Extract ticker without suffix
            clean_ticker = ticker.replace('.V', '').replace('.TO', '').replace('.CN', '')
            
            # Prepare realtime data
            realtime_data = {
                'price': info.get('currentPrice') or info.get('regularMarketPrice'),
                'volume': info.get('volume') or info.get('regularMarketVolume'),
                'dayHigh': info.get('dayHigh') or info.get('regularMarketDayHigh'),
                'dayLow': info.get('dayLow') or info.get('regularMarketDayLow'),
                'open': info.get('open') or info.get('regularMarketOpen'),
                'change': info.get('regularMarketChange'),
                'changePercent': info.get('regularMarketChangePercent')
            }
            
            # Also create flat fields for backward compatibility
            flat_data = {
                'Price': realtime_data['price'],
                'Volume': realtime_data['volume'],
                'DayHigh': realtime_data['dayHigh'],
                'DayLow': realtime_data['dayLow'],
                'Open': realtime_data['open'],
                'Change': realtime_data['change'],
                'ChangePercent': realtime_data['changePercent']
            }
            
            # Update Firebase
            db.collection('market_data').document(clean_ticker).set({
                'ticker': clean_ticker,
                'fullTicker': ticker,
                'realtime': realtime_data,
                **flat_data,
                'lastUpdated': firestore.SERVER_TIMESTAMP,
                'dataSource': 'Yahoo Finance'
            }, merge=True)
            
            success_count += 1
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(YAHOO_COMPANIES)} ({success_count} success, {fail_count} failed)")
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            fail_count += 1
            print(f"  Error updating {ticker}: {str(e)}")
    
    print(f"\n{'='*70}")
    print(f"INTRADAY UPDATE COMPLETE")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

def update_daily_data(db):
    """
    Update daily market data (once at 6:00 AM EST)
    Fields: MarketCap, PE, PreviousClose, 52WeekHigh, 52WeekLow, Beta, AvgVolume
    """
    print(f"\n{'='*70}")
    print("DAILY UPDATE - Yahoo Finance Companies")
    print(f"{'='*70}")
    print(f"Updating {len(YAHOO_COMPANIES)} companies...")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(YAHOO_COMPANIES, 1):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            clean_ticker = ticker.replace('.V', '').replace('.TO', '').replace('.CN', '')
            
            # Prepare daily data
            daily_data = {
                'marketCap': info.get('marketCap'),
                'peRatio': info.get('trailingPE') or info.get('forwardPE'),
                'previousClose': info.get('previousClose') or info.get('regularMarketPreviousClose'),
                'week52High': info.get('fiftyTwoWeekHigh'),
                'week52Low': info.get('fiftyTwoWeekLow'),
                'beta': info.get('beta'),
                'avgVolume': info.get('averageVolume') or info.get('averageVolume10days')
            }
            
            # Flat fields for backward compatibility
            flat_data = {
                'MarketCap': daily_data['marketCap'],
                'PE': daily_data['peRatio'],
                'PreviousClose': daily_data['previousClose'],
                '52WeekHigh': daily_data['week52High'],
                '52WeekLow': daily_data['week52Low'],
                'Beta': daily_data['beta'],
                'AvgVolume': daily_data['avgVolume']
            }
            
            # Update Firebase
            db.collection('market_data').document(clean_ticker).set({
                'ticker': clean_ticker,
                'fullTicker': ticker,
                'daily': daily_data,
                **flat_data,
                'lastUpdated': firestore.SERVER_TIMESTAMP,
                'dataSource': 'Yahoo Finance'
            }, merge=True)
            
            success_count += 1
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(YAHOO_COMPANIES)} ({success_count} success, {fail_count} failed)")
            
            time.sleep(0.1)
            
        except Exception as e:
            fail_count += 1
            print(f"  Error updating {ticker}: {str(e)}")
    
    print(f"\n{'='*70}")
    print(f"DAILY UPDATE COMPLETE")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

def update_weekly_data(db):
    """
    Update weekly market data (Sunday 6:00 PM EST)
    Fields: SharesOutstanding, EPS, Float
    """
    print(f"\n{'='*70}")
    print("WEEKLY UPDATE - Yahoo Finance Companies")
    print(f"{'='*70}")
    print(f"Updating {len(YAHOO_COMPANIES)} companies...")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(YAHOO_COMPANIES, 1):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            clean_ticker = ticker.replace('.V', '').replace('.TO', '').replace('.CN', '')
            
            # Prepare monthly/weekly data
            monthly_data = {
                'sharesOutstanding': info.get('sharesOutstanding'),
                'eps': info.get('trailingEps') or info.get('forwardEps'),
                'float': info.get('floatShares')
            }
            
            # Flat fields for backward compatibility
            flat_data = {
                'SharesOutstanding': monthly_data['sharesOutstanding'],
                'EPS': monthly_data['eps'],
                'Float': monthly_data['float']
            }
            
            # Update Firebase
            db.collection('market_data').document(clean_ticker).set({
                'ticker': clean_ticker,
                'fullTicker': ticker,
                'monthly': monthly_data,
                **flat_data,
                'lastUpdated': firestore.SERVER_TIMESTAMP,
                'dataSource': 'Yahoo Finance'
            }, merge=True)
            
            success_count += 1
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(YAHOO_COMPANIES)} ({success_count} success, {fail_count} failed)")
            
            time.sleep(0.1)
            
        except Exception as e:
            fail_count += 1
            print(f"  Error updating {ticker}: {str(e)}")
    
    print(f"\n{'='*70}")
    print(f"WEEKLY UPDATE COMPLETE")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import sys
    
    # Determine update type from command line argument
    update_type = sys.argv[1] if len(sys.argv) > 1 else 'intraday'
    
    print(f"\n{'='*70}")
    print(f"YAHOO FINANCE MARKET DATA UPDATER")
    print(f"Update Type: {update_type.upper()}")
    print(f"{'='*70}\n")
    
    try:
        # Initialize Firebase
        db = initialize_firebase()
        
        # Run appropriate update
        if update_type == 'intraday':
            update_intraday_data(db)
        elif update_type == 'daily':
            update_daily_data(db)
        elif update_type == 'weekly':
            update_weekly_data(db)
        else:
            print(f"Unknown update type: {update_type}")
            print("Valid types: intraday, daily, weekly")
            sys.exit(1)
        
        print("\nUpdate completed successfully!")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
