import pandas as pd
import json
import time
import random
from curl_cffi import requests
import os
from datetime import datetime

class NSEFinancialScraper:
    def __init__(self):
        self.session = requests.Session()
        self.master_df = None
        self.master_file = "NSE_ALL_COMPANIES_FINANCIALS.csv"
        
    def get_nse_symbols(self):
        """
        Get list of all NSE symbols (excluding SME)
        """
        # This is a subset - you can expand this list or fetch from NSE website
        nse_symbols = [
            # Nifty 50 companies
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "HINDUNILVR.NS",
            "ICICIBANK.NS", "SBIN.NS", "BHARTIARTL.NS", "HDFC.NS", "KOTAKBANK.NS",
            "ITC.NS", "LT.NS", "AXISBANK.NS", "BAJFINANCE.NS", "ASIAN.NS",
            "MARUTI.NS", "WIPRO.NS", "HCLTECH.NS", "ULTRACEMCO.NS", "TITAN.NS",
            "NESTLEIND.NS", "SUNPHARMA.NS", "BAJAJFINSV.NS", "TECHM.NS", "ONGC.NS",
            "HDFCLIFE.NS", "TATAMOTORS.NS", "NTPC.NS", "POWERGRID.NS", "M&M.NS",
            "TATASTEEL.NS", "JSWSTEEL.NS", "GRASIM.NS", "INDUSINDBK.NS", "BAJAJ-AUTO.NS",
            "DRREDDY.NS", "BRITANNIA.NS", "SBILIFE.NS", "DIVISLAB.NS", "ADANIPORTS.NS",
            "COALINDIA.NS", "HINDALCO.NS", "EICHERMOT.NS", "UPL.NS", "BPCL.NS",
            "SHREECEM.NS", "HEROMOTOCO.NS", "CIPLA.NS", "IOC.NS", "ADANIENT.NS",
            
            # Additional major companies
            "DABUR.NS", "GODREJCP.NS", "PIDILITIND.NS", "HAVELLS.NS", "BERGEPAINT.NS",
            "MARICO.NS", "COLPAL.NS", "MUTHOOTFIN.NS", "PAGEIND.NS", "TATACONSUM.NS",
            "APOLLOHOSP.NS", "TORNTPHARM.NS", "BIOCON.NS", "DMART.NS", "MCDOWELL-N.NS",
            "PEL.NS", "AMBUJACEM.NS", "SIEMENS.NS", "BANDHANBNK.NS", "BANKBARODA.NS",
            "CANBK.NS", "PNB.NS", "INDIGO.NS", "IRCTC.NS", "ZEEL.NS",
            "SAIL.NS", "VEDL.NS", "GMRINFRA.NS", "IDEA.NS", "ASHOKLEY.NS",
            "TATAPOWER.NS", "FEDERALBNK.NS", "RECLTD.NS", "PFC.NS", "MOTHERSUMI.NS",
            "CADILAHC.NS", "LUPIN.NS", "AUROPHARMA.NS", "GLENMARK.NS", "ALKEM.NS",
            "JUBLFOOD.NS", "VOLTAS.NS", "CROMPTON.NS", "BATAINDIA.NS", "TRENT.NS",
            "MINDTREE.NS", "LTTS.NS", "MPHASIS.NS", "COFORGE.NS", "PERSISTENT.NS",
            
            # Add more companies as needed - NSE has ~1800 companies
            # You can fetch the complete list from NSE website or use a pre-built list
        ]
        
        # To get complete list, you can scrape from NSE or use this approach:
        """
        # Option 1: Read from NSE official equity list
        # Download from: https://www.nseindia.com/market-data/live-equity-market
        
        # Option 2: Use nsetools library
        # from nsetools import Nse
        # nse = Nse()
        # all_stocks = nse.get_stock_codes()
        # nse_symbols = [f"{symbol}.NS" for symbol in all_stocks.keys() if symbol != 'SYMBOL']
        """
        
        return nse_symbols
    
    def load_existing_data(self):
        """
        Load existing master CSV if it exists
        """
        if os.path.exists(self.master_file):
            try:
                self.master_df = pd.read_csv(self.master_file)
                print(f"Loaded existing data: {len(self.master_df)} rows")
                
                # Get list of already fetched companies
                if 'symbol' in self.master_df.columns:
                    fetched_symbols = self.master_df['symbol'].unique().tolist()
                    print(f"Already have data for {len(fetched_symbols)} companies")
                    return fetched_symbols
            except Exception as e:
                print(f"Error loading existing file: {e}")
                self.master_df = pd.DataFrame()
        else:
            self.master_df = pd.DataFrame()
            print("Starting fresh - no existing data file")
        
        return []
    
    def fetch_company_data(self, symbol):
        """
        Fetch financial data for a single company
        """
        url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}"
        
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Referer': f'https://www.perplexity.ai/finance/{symbol}',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        params = {
            'version': '2.18',
            'source': 'default'
        }
        
        try:
            # Try with browser impersonation
            for browser in ['chrome120', 'chrome110', 'firefox120']:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    impersonate=browser,
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return self._process_company_data(data, symbol)
                elif response.status_code == 404:
                    print(f"  {symbol}: Not found on Perplexity")
                    return None
                elif response.status_code == 403:
                    continue
                    
        except Exception as e:
            print(f"  {symbol}: Error - {str(e)[:50]}")
        
        return None
    
    def _process_company_data(self, data, symbol):
        """
        Process financial data for a company
        """
        all_annual_data = []
        
        # Extract annual data
        annual_data = None
        if isinstance(data, dict) and 'annual' in data:
            annual_data = data['annual']
        
        if annual_data:
            for statement in annual_data:
                if isinstance(statement, dict) and 'data' in statement:
                    statement_type = statement.get('type', '')
                    
                    for year_data in statement['data']:
                        date = year_data.get('date', '')
                        
                        # Find or create year entry
                        year_entry = None
                        for entry in all_annual_data:
                            if entry.get('date') == date:
                                year_entry = entry
                                break
                        
                        if year_entry is None:
                            year_entry = {
                                'symbol': symbol,  # Add symbol to identify company
                                'date': date
                            }
                            all_annual_data.append(year_entry)
                        
                        # Add all metrics
                        for key, value in year_data.items():
                            if key not in ['date', 'symbol']:
                                # Avoid column name conflicts
                                if key in ['link', 'finalLink']:
                                    column_name = f"{statement_type}_{key}"
                                else:
                                    column_name = key
                                year_entry[column_name] = value
        
        if all_annual_data:
            df = pd.DataFrame(all_annual_data)
            return df
        
        return None
    
    def update_master_data(self, new_data):
        """
        Update master DataFrame with new company data
        """
        if new_data is None or new_data.empty:
            return
        
        if self.master_df is None or self.master_df.empty:
            self.master_df = new_data
        else:
            # Remove existing data for this symbol if updating
            symbol = new_data['symbol'].iloc[0]
            self.master_df = self.master_df[self.master_df['symbol'] != symbol]
            
            # Append new data
            self.master_df = pd.concat([self.master_df, new_data], ignore_index=True)
    
    def save_master_data(self):
        """
        Save master DataFrame to CSV
        """
        if self.master_df is not None and not self.master_df.empty:
            # Sort by symbol and date
            if 'date' in self.master_df.columns:
                self.master_df['date'] = pd.to_datetime(self.master_df['date'])
                self.master_df = self.master_df.sort_values(['symbol', 'date'])
                self.master_df['date'] = self.master_df['date'].dt.strftime('%Y-%m-%d')
            
            # Save to CSV
            self.master_df.to_csv(self.master_file, index=False)
            print(f"\n✓ Saved {len(self.master_df)} rows to {self.master_file}")
            
            # Show summary
            unique_symbols = self.master_df['symbol'].nunique()
            print(f"  Total companies: {unique_symbols}")
            print(f"  Total rows: {len(self.master_df)}")
            print(f"  Columns: {len(self.master_df.columns)}")
    
    def fetch_all_companies(self, limit=None, skip_existing=True):
        """
        Fetch data for all NSE companies
        """
        # Load existing data
        existing_symbols = self.load_existing_data() if skip_existing else []
        
        # Get list of all NSE symbols
        all_symbols = self.get_nse_symbols()
        
        # Filter out already fetched symbols
        symbols_to_fetch = [s for s in all_symbols if s not in existing_symbols]
        
        if limit:
            symbols_to_fetch = symbols_to_fetch[:limit]
        
        print(f"\nWill fetch data for {len(symbols_to_fetch)} companies")
        print("="*60)
        
        successful = 0
        failed = 0
        
        for i, symbol in enumerate(symbols_to_fetch, 1):
            print(f"\n[{i}/{len(symbols_to_fetch)}] Fetching {symbol}...", end=" ")
            
            # Fetch data
            company_data = self.fetch_company_data(symbol)
            
            if company_data is not None:
                self.update_master_data(company_data)
                successful += 1
                print(f"✓ ({len(company_data)} years)")
                
                # Save periodically (every 10 companies)
                if successful % 10 == 0:
                    self.save_master_data()
                    print(f"  [Checkpoint: Saved after {successful} companies]")
            else:
                failed += 1
                print("✗")
            
            # Rate limiting
            if i < len(symbols_to_fetch):
                delay = random.uniform(2, 4)
                time.sleep(delay)
        
        # Final save
        self.save_master_data()
        
        print("\n" + "="*60)
        print(f"COMPLETED: {successful} successful, {failed} failed")
        print(f"Master file: {self.master_file}")
        
        return self.master_df

# Utility function to get complete NSE list
def get_complete_nse_list():
    """
    Get complete list of NSE symbols from various sources
    """
    # Method 1: Hardcoded comprehensive list (partial shown here)
    # You can expand this or fetch from NSE website
    
    # Method 2: Use nsetools
    try:
        from nsetools import Nse
        nse = Nse()
        all_stocks = nse.get_stock_codes()
        # Filter out INDEX symbols and SME stocks
        symbols = []
        for code, name in all_stocks.items():
            if code != 'SYMBOL' and not code.startswith('INDEX'):
                # Add more filters to exclude SME stocks if needed
                symbols.append(f"{code}.NS")
        return symbols
    except ImportError:
        print("Install nsetools for complete list: pip install nsetools")
        return None

# Main execution
if __name__ == "__main__":
    print("="*60)
    print("NSE FINANCIAL DATA AGGREGATOR")
    print("="*60)
    print("This will fetch financial data for all NSE companies")
    print("and save to a single CSV file")
    print("-"*60)
    
    scraper = NSEFinancialScraper()
    
    # Options
    LIMIT = None  # Set to a number to limit companies (None = all)
    SKIP_EXISTING = True  # Skip companies already in the CSV
    
    # Fetch all companies
    df = scraper.fetch_all_companies(limit=LIMIT, skip_existing=SKIP_EXISTING)
    
    if df is not None and not df.empty:
        print("\n" + "="*60)
        print("FINAL SUMMARY")
        print("-"*60)
        print(f"File: {scraper.master_file}")
        print(f"Companies: {df['symbol'].nunique()}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Show sample
        print("\nSample data (first 5 rows):")
        print(df[['symbol', 'date', 'revenue', 'netIncome']].head())
