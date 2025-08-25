import pandas as pd
import json
import time
import random
from curl_cffi import requests

class PerplexityCurlScraper:
    def __init__(self):
        # Use curl-cffi session that can impersonate browsers
        self.session = requests.Session()
        
    def fetch_financial_data(self, symbol):
        """
        Fetch financial data using curl-cffi to bypass Cloudflare
        """
        print(f"Fetching data for {symbol} using curl-cffi...")
        
        # Method 1: Direct API call with browser impersonation
        data = self._direct_api_with_impersonation(symbol)
        if data:
            return self._process_data(data, symbol)
        
        # Method 2: Try with session establishment
        data = self._session_based_approach(symbol)
        if data:
            return self._process_data(data, symbol)
        
        # Method 3: Extract from HTML page
        data = self._extract_from_html(symbol)
        if data:
            return self._process_data(data, symbol)
        
        print(f"Failed to fetch data for {symbol}")
        return None
    
    def _direct_api_with_impersonation(self, symbol):
        """
        Direct API call using browser impersonation
        """
        print("Method 1: Direct API with browser impersonation...")
        
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
            # Use different browser impersonations
            for browser in ['chrome120', 'chrome110', 'chrome107', 'firefox120', 'safari17_0']:
                print(f"  Trying with {browser} impersonation...")
                
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    impersonate=browser,
                    timeout=10
                )
                
                if response.status_code == 200:
                    print(f"  ✓ SUCCESS with {browser}!")
                    return response.json()
                elif response.status_code == 403:
                    print(f"  ✗ 403 with {browser}")
                    continue
                else:
                    print(f"  ✗ Status {response.status_code} with {browser}")
                    
        except Exception as e:
            print(f"  Error: {e}")
        
        return None
    
    def _session_based_approach(self, symbol):
        """
        Establish session first, then fetch data
        """
        print("Method 2: Session-based approach...")
        
        try:
            # Step 1: Visit home page
            print("  Loading home page...")
            home_response = requests.get(
                'https://www.perplexity.ai',
                impersonate='chrome120',
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            )
            
            if home_response.status_code != 200:
                print(f"  Failed to load home page: {home_response.status_code}")
                return None
            
            # Extract cookies
            cookies = home_response.cookies
            print(f"  Got {len(cookies)} cookies")
            
            time.sleep(random.uniform(1, 2))
            
            # Step 2: Visit finance page
            print(f"  Loading finance page for {symbol}...")
            finance_url = f'https://www.perplexity.ai/finance/{symbol}'
            finance_response = requests.get(
                finance_url,
                impersonate='chrome120',
                cookies=cookies,
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://www.perplexity.ai/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            )
            
            if finance_response.status_code != 200:
                print(f"  Failed to load finance page: {finance_response.status_code}")
            else:
                # Update cookies
                cookies.update(finance_response.cookies)
                
            time.sleep(random.uniform(1, 2))
            
            # Step 3: Fetch API data
            print("  Fetching API data...")
            api_url = f'https://www.perplexity.ai/rest/finance/financials/{symbol}'
            api_response = requests.get(
                api_url,
                impersonate='chrome120',
                cookies=cookies,
                headers={
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': finance_url,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                params={
                    'version': '2.18',
                    'source': 'default'
                }
            )
            
            if api_response.status_code == 200:
                print("  ✓ SUCCESS with session approach!")
                return api_response.json()
            else:
                print(f"  API request failed: {api_response.status_code}")
                
        except Exception as e:
            print(f"  Session error: {e}")
        
        return None
    
    def _extract_from_html(self, symbol):
        """
        Extract data from HTML page if API fails
        """
        print("Method 3: Extracting from HTML...")
        
        url = f'https://www.perplexity.ai/finance/{symbol}'
        
        try:
            response = requests.get(
                url,
                impersonate='chrome120',
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            )
            
            if response.status_code == 200:
                html = response.text
                
                # Look for JSON data in various places
                patterns = [
                    ('id="__NEXT_DATA__"', '</script>'),
                    ('window.__INITIAL_DATA__ = ', ';'),
                    ('window.__PRELOADED_STATE__ = ', ';'),
                    ('"financials":', '}}},'),
                ]
                
                for start_pattern, end_pattern in patterns:
                    if start_pattern in html:
                        start = html.find(start_pattern)
                        if 'id="__NEXT_DATA__"' in start_pattern:
                            start = html.find('>', start) + 1
                        else:
                            start = html.find(start_pattern) + len(start_pattern)
                            if '=' in start_pattern:
                                start = html.find('=', start - len(start_pattern)) + 1
                        
                        if end_pattern == '</script>':
                            end = html.find(end_pattern, start)
                        elif end_pattern == ';':
                            end = html.find(end_pattern, start)
                        else:
                            # Find matching brackets
                            bracket_count = 0
                            for i, char in enumerate(html[start:], start):
                                if char == '{':
                                    bracket_count += 1
                                elif char == '}':
                                    bracket_count -= 1
                                    if bracket_count == 0:
                                        end = i + 1
                                        break
                        
                        json_str = html[start:end].strip()
                        
                        try:
                            data = json.loads(json_str)
                            
                            # Look for financial data in the extracted JSON
                            if self._find_financial_data(data):
                                print("  ✓ Found financial data in HTML!")
                                return self._extract_financial_data(data)
                                
                        except json.JSONDecodeError:
                            continue
                            
            else:
                print(f"  Failed to load page: {response.status_code}")
                
        except Exception as e:
            print(f"  HTML extraction error: {e}")
        
        return None
    
    def _find_financial_data(self, data):
        """
        Check if data contains financial information
        """
        if isinstance(data, dict):
            if 'annual' in data or 'financials' in data:
                return True
            for value in data.values():
                if self._find_financial_data(value):
                    return True
        elif isinstance(data, list):
            for item in data:
                if self._find_financial_data(item):
                    return True
        return False
    
    def _extract_financial_data(self, data):
        """
        Extract financial data from nested structure
        """
        if isinstance(data, dict):
            if 'annual' in data:
                return data
            if 'financials' in data:
                return data['financials']
            if 'props' in data and 'pageProps' in data['props']:
                page_props = data['props']['pageProps']
                if 'financials' in page_props:
                    return page_props['financials']
                if 'data' in page_props:
                    return page_props['data']
            
            for value in data.values():
                result = self._extract_financial_data(value)
                if result:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self._extract_financial_data(item)
                if result:
                    return result
        return None
    
    def _process_data(self, data, symbol):
        """
        Process financial data into DataFrame
        """
        all_annual_data = []
        
        # Handle different data structures
        annual_data = None
        if isinstance(data, dict):
            if 'annual' in data:
                annual_data = data['annual']
            elif 'data' in data and isinstance(data['data'], dict) and 'annual' in data['data']:
                annual_data = data['data']['annual']
        
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
                            year_entry = {'date': date}
                            all_annual_data.append(year_entry)
                        
                        # Add all metrics
                        for key, value in year_data.items():
                            if key != 'date':
                                # Handle duplicate keys
                                if key in ['link', 'finalLink']:
                                    column_name = f"{statement_type}_{key}"
                                else:
                                    column_name = key
                                year_entry[column_name] = value
        
        if all_annual_data:
            df = pd.DataFrame(all_annual_data)
            
            # Sort by date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Save to CSV
            filename = f"{symbol.replace('.', '_')}_financials.csv"
            df.to_csv(filename, index=False)
            
            print(f"\n✓ Data saved to {filename}")
            print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
            
            # Show preview
            preview_cols = ['date', 'revenue', 'netIncome', 'totalAssets', 'operatingCashFlow']
            available_cols = [col for col in preview_cols if col in df.columns]
            if available_cols:
                print("\nPreview:")
                print(df[available_cols].head())
            
            return df
        
        return None

# Main execution
if __name__ == "__main__":
    print("="*60)
    print("PERPLEXITY FINANCIAL SCRAPER - CURL-CFFI VERSION")
    print("="*60)
    print("\nFirst install: pip install curl-cffi pandas")
    print("-"*60)
    
    companies = [
        "ETERNAL.NS",
        # Add more companies here
        # "RELIANCE.NS",
        # "TCS.NS",
    ]
    
    scraper = PerplexityCurlScraper()
    
    for symbol in companies:
        print(f"\nProcessing: {symbol}")
        print("-"*40)
        
        df = scraper.fetch_financial_data(symbol)
        
        if df is None:
            print(f"\n✗ Failed to extract {symbol}")
            print("If curl-cffi also fails, the only remaining option is:")
            print("1. Use browser cookies from a logged-in session")
            print("2. Use a paid proxy service with residential IPs")
            print("3. Use browser automation (Selenium/Playwright)")
        
        # Delay between requests
        if symbol != companies[-1]:
            delay = random.uniform(3, 5)
            print(f"\nWaiting {delay:.1f} seconds before next request...")
            time.sleep(delay)