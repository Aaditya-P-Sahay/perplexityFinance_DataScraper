import requests
import pandas as pd
import json
import time
import random
from urllib.parse import quote

class PerplexityFinancialScraper:
    def __init__(self):
        self.session = requests.Session()
        # Randomize user agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0'
        ]
        self.user_agent = random.choice(user_agents)
        
    def fetch_financial_data(self, symbol):
        """
        Fetch financial data using various bypass techniques
        """
        # Method 1: Try direct API with enhanced headers
        data = self._try_direct_api(symbol)
        if data:
            return self._process_data(data, symbol)
        
        # Method 2: Try alternative endpoints
        data = self._try_alternative_endpoints(symbol)
        if data:
            return self._process_data(data, symbol)
        
        # Method 3: Try with pre-flight requests
        data = self._try_with_preflight(symbol)
        if data:
            return self._process_data(data, symbol)
        
        print(f"All methods failed for {symbol}")
        return None
    
    def _try_direct_api(self, symbol):
        """
        Direct API call with comprehensive headers
        """
        print(f"Attempting direct API call for {symbol}...")
        
        url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}"
        
        headers = {
            'Host': 'www.perplexity.ai',
            'User-Agent': self.user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': f'https://www.perplexity.ai/finance/{symbol}',
            'X-Requested-With': 'XMLHttpRequest',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        params = {
            'version': '2.18',
            'source': 'default'
        }
        
        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                print("Success with direct API!")
                return response.json()
            else:
                print(f"Direct API failed with status: {response.status_code}")
        except Exception as e:
            print(f"Direct API error: {e}")
        
        return None
    
    def _try_alternative_endpoints(self, symbol):
        """
        Try alternative API endpoints or patterns
        """
        print(f"Trying alternative endpoints for {symbol}...")
        
        # Alternative URL patterns
        endpoints = [
            f"https://www.perplexity.ai/api/finance/data/{symbol}",
            f"https://www.perplexity.ai/finance/api/v1/financials/{symbol}",
            f"https://api.perplexity.ai/finance/{symbol}/financials",
            f"https://www.perplexity.ai/rest/v2/finance/financials/{symbol}"
        ]
        
        for endpoint in endpoints:
            headers = {
                'User-Agent': self.user_agent,
                'Accept': 'application/json',
                'Referer': 'https://www.perplexity.ai/',
                'Origin': 'https://www.perplexity.ai'
            }
            
            try:
                response = self.session.get(endpoint, headers=headers, timeout=5)
                if response.status_code == 200:
                    print(f"Success with endpoint: {endpoint}")
                    return response.json()
            except:
                continue
        
        return None
    
    def _try_with_preflight(self, symbol):
        """
        Try with preflight requests to establish session
        """
        print(f"Trying with session establishment for {symbol}...")
        
        # First, visit the main finance page to establish session
        base_url = f"https://www.perplexity.ai/finance/{symbol}"
        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        
        try:
            # Step 1: Get the main page
            response = self.session.get(base_url, headers=headers)
            time.sleep(1)  # Small delay
            
            # Step 2: Try API with established session
            api_url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}"
            api_headers = {
                'User-Agent': self.user_agent,
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': base_url
            }
            
            params = {'version': '2.18', 'source': 'default'}
            response = self.session.get(api_url, headers=api_headers, params=params)
            
            if response.status_code == 200:
                print("Success with session establishment!")
                return response.json()
            else:
                print(f"Session method failed with status: {response.status_code}")
        except Exception as e:
            print(f"Session method error: {e}")
        
        return None
    
    def _process_data(self, data, symbol):
        """
        Process the financial data into a DataFrame
        """
        all_annual_data = []
        
        if 'annual' in data:
            for statement in data['annual']:
                statement_type = statement.get('type', 'UNKNOWN')
                
                if 'data' in statement and statement['data']:
                    for year_data in statement['data']:
                        date = year_data.get('date', '')
                        year_entry = None
                        
                        for entry in all_annual_data:
                            if entry.get('date') == date:
                                year_entry = entry
                                break
                        
                        if year_entry is None:
                            year_entry = {'date': date}
                            all_annual_data.append(year_entry)
                        
                        for key, value in year_data.items():
                            if key != 'date':
                                if key in ['link', 'finalLink']:
                                    column_name = f"{statement_type}_{key}"
                                else:
                                    column_name = key
                                year_entry[column_name] = value
        
        df = pd.DataFrame(all_annual_data)
        
        if not df.empty:
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Save to CSV
            filename = f"{symbol.replace('.', '_')}_financials.csv"
            df.to_csv(filename, index=False)
            print(f"Data saved to {filename}")
            print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
            
            return df
        
        return None

# Alternative approach using a different data source
def try_yahoo_finance_fallback(symbol):
    """
    Fallback to Yahoo Finance if Perplexity fails
    """
    try:
        import yfinance as yf
        print(f"\nTrying Yahoo Finance fallback for {symbol}...")
        
        # Convert symbol format if needed (remove .NS for Yahoo)
        yahoo_symbol = symbol
        
        ticker = yf.Ticker(yahoo_symbol)
        
        # Get financial data
        financials = ticker.financials.T  # Income statement
        balance_sheet = ticker.balance_sheet.T
        cash_flow = ticker.cashflow.T
        
        # Combine all statements
        df = pd.concat([financials, balance_sheet, cash_flow], axis=1)
        
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Reset index to have date as a column
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        
        # Save to CSV
        filename = f"{symbol.replace('.', '_')}_yahoo_financials.csv"
        df.to_csv(filename, index=False)
        print(f"Yahoo Finance data saved to {filename}")
        
        return df
        
    except ImportError:
        print("yfinance not installed. Install with: pip install yfinance")
    except Exception as e:
        print(f"Yahoo Finance error: {e}")
    
    return None

# Main execution
if __name__ == "__main__":
    # List of companies to fetch
    companies = [
        "ETERNAL.NS",
        # Add more companies here
        # "RELIANCE.NS",
        # "TCS.NS",
    ]
    
    scraper = PerplexityFinancialScraper()
    
    for symbol in companies:
        print(f"\n{'='*60}")
        print(f"Processing: {symbol}")
        print('='*60)
        
        # Try Perplexity first
        df = scraper.fetch_financial_data(symbol)
        
        # If Perplexity fails, try Yahoo Finance
        if df is None:
            print("\nPerplexity methods failed. Trying Yahoo Finance...")
            df = try_yahoo_finance_fallback(symbol)
        
        if df is not None:
            print(f"Successfully fetched data for {symbol}")
        else:
            print(f"Failed to fetch data for {symbol}")
        
        time.sleep(2)  # Delay between requests