import requests
import pandas as pd
import json
import time
import random
import cloudscraper
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class PerplexityForceExtractor:
    def __init__(self):
        # Use cloudscraper to bypass Cloudflare
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Add retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.scraper.mount("http://", adapter)
        self.scraper.mount("https://", adapter)
        
    def fetch_financial_data(self, symbol):
        """
        Aggressively try to get data from Perplexity
        """
        print(f"Attempting to fetch {symbol}...")
        
        # Method 1: Try with cloudscraper
        data = self._cloudscraper_method(symbol)
        if data:
            return self._process_data(data, symbol)
        
        # Method 2: Try with proxy rotation
        data = self._proxy_method(symbol)
        if data:
            return self._process_data(data, symbol)
        
        # Method 3: Try raw HTML parsing
        data = self._html_parse_method(symbol)
        if data:
            return self._process_data(data, symbol)
        
        print(f"Failed to fetch {symbol}")
        return None
    
    def _cloudscraper_method(self, symbol):
        """
        Use cloudscraper to bypass Cloudflare protection
        """
        print("Method 1: Cloudscraper bypass...")
        
        # First, load the main page to get tokens
        main_url = f"https://www.perplexity.ai/finance/{symbol}"
        
        try:
            # Get main page first
            response = self.scraper.get(main_url)
            time.sleep(random.uniform(1, 3))
            
            # Extract any tokens from the page
            cookies = self.scraper.cookies.get_dict()
            
            # Now try the API
            api_url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}"
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Referer': main_url,
                'Sec-Ch-Ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'X-Requested-With': 'XMLHttpRequest'
            }
            
            params = {
                'version': '2.18',
                'source': 'default'
            }
            
            response = self.scraper.get(api_url, headers=headers, params=params, cookies=cookies)
            
            if response.status_code == 200:
                print("SUCCESS: Got data via cloudscraper!")
                return response.json()
            else:
                print(f"Cloudscraper failed: {response.status_code}")
                
        except Exception as e:
            print(f"Cloudscraper error: {e}")
        
        return None
    
    def _proxy_method(self, symbol):
        """
        Try using free proxy servers
        """
        print("Method 2: Trying with proxy servers...")
        
        # List of free proxy servers (these change frequently)
        proxies_list = [
            {'http': 'http://103.155.217.156:41456', 'https': 'http://103.155.217.156:41456'},
            {'http': 'http://103.152.112.145:80', 'https': 'http://103.152.112.145:80'},
            {'http': 'http://20.219.180.149:3129', 'https': 'http://20.219.180.149:3129'},
            None  # Also try without proxy
        ]
        
        url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}"
        
        for proxy in proxies_list:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': f'https://www.perplexity.ai/finance/{symbol}'
                }
                
                params = {'version': '2.18', 'source': 'default'}
                
                response = requests.get(
                    url, 
                    headers=headers, 
                    params=params,
                    proxies=proxy,
                    timeout=10,
                    verify=False  # Disable SSL verification for proxies
                )
                
                if response.status_code == 200:
                    print(f"SUCCESS: Got data via proxy!")
                    return response.json()
                    
            except:
                continue
        
        return None
    
    def _html_parse_method(self, symbol):
        """
        Try to extract data from HTML if API fails
        """
        print("Method 3: HTML parsing fallback...")
        
        url = f"https://www.perplexity.ai/finance/{symbol}"
        
        try:
            response = self.scraper.get(url)
            
            if response.status_code == 200:
                # Look for JSON data embedded in HTML
                html_content = response.text
                
                # Common patterns where data might be embedded
                patterns = [
                    'window.__INITIAL_DATA__ = ',
                    'window.__PRELOADED_STATE__ = ',
                    '<script type="application/json" id="__NEXT_DATA__">',
                    'data-financials="',
                    '"financials":',
                ]
                
                for pattern in patterns:
                    if pattern in html_content:
                        start = html_content.find(pattern) + len(pattern)
                        
                        # Try to extract JSON
                        if pattern.endswith('= '):
                            # Find the end of the JSON object
                            end = html_content.find('</script>', start)
                            json_str = html_content[start:end].strip().rstrip(';')
                        elif pattern == '<script type="application/json" id="__NEXT_DATA__">':
                            end = html_content.find('</script>', start)
                            json_str = html_content[start:end]
                        else:
                            # Try to find JSON boundaries
                            bracket_count = 0
                            end = start
                            for i, char in enumerate(html_content[start:], start):
                                if char == '{':
                                    bracket_count += 1
                                elif char == '}':
                                    bracket_count -= 1
                                    if bracket_count == 0:
                                        end = i + 1
                                        break
                            json_str = html_content[start:end]
                        
                        try:
                            data = json.loads(json_str)
                            # Look for financial data in the extracted JSON
                            if self._find_financial_data(data):
                                print("SUCCESS: Extracted data from HTML!")
                                return data
                        except:
                            continue
                            
        except Exception as e:
            print(f"HTML parsing error: {e}")
        
        return None
    
    def _find_financial_data(self, data):
        """
        Recursively search for financial data in JSON structure
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
    
    def _process_data(self, data, symbol):
        """
        Process the financial data into DataFrame
        """
        all_annual_data = []
        
        # Handle different possible data structures
        if isinstance(data, dict):
            if 'annual' in data:
                annual_data = data['annual']
            elif 'data' in data and 'annual' in data['data']:
                annual_data = data['data']['annual']
            elif 'financials' in data:
                annual_data = data['financials'].get('annual', [])
            else:
                # Try to find annual data recursively
                annual_data = self._extract_annual_data(data)
        else:
            annual_data = []
        
        if annual_data:
            for statement in annual_data:
                if isinstance(statement, dict):
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
                                    year_entry[key] = value
        
        if all_annual_data:
            df = pd.DataFrame(all_annual_data)
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            filename = f"{symbol.replace('.', '_')}_financials.csv"
            df.to_csv(filename, index=False)
            print(f"Data saved to {filename}")
            print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
            
            return df
        
        return None
    
    def _extract_annual_data(self, data, depth=0):
        """
        Recursively extract annual data from nested structure
        """
        if depth > 5:  # Prevent infinite recursion
            return []
        
        if isinstance(data, dict):
            if 'annual' in data:
                return data['annual']
            for key, value in data.items():
                result = self._extract_annual_data(value, depth + 1)
                if result:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self._extract_annual_data(item, depth + 1)
                if result:
                    return result
        
        return []

# Main execution
if __name__ == "__main__":
    # Install required package
    print("Make sure to install: pip install cloudscraper requests pandas")
    print("="*60)
    
    companies = [
        "ETERNAL.NS",
        # Add more companies
    ]
    
    extractor = PerplexityForceExtractor()
    
    for symbol in companies:
        print(f"\nProcessing {symbol}...")
        print("-"*40)
        
        df = extractor.fetch_financial_data(symbol)
        
        if df is not None:
            print(f"✓ Successfully extracted data for {symbol}")
        else:
            print(f"✗ Could not extract data for {symbol}")
            print("\nThe API is heavily protected. Without authentication cookies,")
            print("it's nearly impossible to access. You need to either:")
            print("1. Use browser cookies (as shown in previous solution)")
            print("2. Use alternative data sources")
            print("3. Use web scraping tools like Playwright/Selenium")
        
        time.sleep(3)  # Delay between requests