import requests
import pandas as pd
import json
import time
import random
from requests import Session

class PerplexitySessionScraper:
    def __init__(self):
        self.session = Session()
        # CRITICAL: Headers must be in exact order as Chrome sends them
        self.session.headers = {}
        
    def fetch_financial_data(self, symbol):
        """
        Mimic exact browser behavior to get financial data
        """
        print(f"Setting up session for {symbol}...")
        
        # Step 1: Initialize session with home page
        if not self._init_session():
            return None
            
        # Step 2: Navigate to finance section
        if not self._navigate_to_finance(symbol):
            return None
            
        # Step 3: Fetch the actual data
        data = self._fetch_api_data(symbol)
        if data:
            return self._process_data(data, symbol)
            
        return None
    
    def _init_session(self):
        """
        Initialize session by visiting home page first
        """
        print("Step 1: Initializing session...")
        
        # First request - home page
        url = "https://www.perplexity.ai"
        
        # Headers in EXACT order Chrome sends them
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        try:
            response = self.session.get(url, headers=headers, allow_redirects=True)
            if response.status_code == 200:
                print(f"  ✓ Home page loaded, got {len(response.cookies)} cookies")
                
                # Extract any CSRF tokens or session IDs from the response
                if '_pplx_session' in response.text:
                    print("  ✓ Session token found in response")
                    
                time.sleep(random.uniform(0.5, 1.5))  # Human-like delay
                return True
            else:
                print(f"  ✗ Failed to load home page: {response.status_code}")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
            
        return False
    
    def _navigate_to_finance(self, symbol):
        """
        Navigate to finance page to establish context
        """
        print(f"Step 2: Navigating to finance page for {symbol}...")
        
        url = f"https://www.perplexity.ai/finance/{symbol}"
        
        # Updated headers for navigation
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'referer': 'https://www.perplexity.ai/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        try:
            response = self.session.get(url, headers=headers)
            if response.status_code == 200:
                print(f"  ✓ Finance page loaded")
                
                # Look for embedded data in the HTML
                if '__NEXT_DATA__' in response.text:
                    print("  ✓ Found Next.js data in page")
                    data = self._extract_nextjs_data(response.text)
                    if data:
                        return data
                        
                time.sleep(random.uniform(0.5, 1.5))
                return True
            else:
                print(f"  ✗ Failed to load finance page: {response.status_code}")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
            
        return False
    
    def _fetch_api_data(self, symbol):
        """
        Fetch the actual API data
        """
        print(f"Step 3: Fetching API data for {symbol}...")
        
        url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}"
        
        # XHR request headers - different from navigation
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'referer': f'https://www.perplexity.ai/finance/{symbol}',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        
        params = {
            'version': '2.18',
            'source': 'default'
        }
        
        try:
            response = self.session.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                print("  ✓ SUCCESS! Got financial data")
                return response.json()
            elif response.status_code == 403:
                print("  ✗ 403 Forbidden - Cloudflare blocked the request")
                print("    Trying to extract data from HTML instead...")
                return self._fallback_html_extraction(symbol)
            else:
                print(f"  ✗ Failed with status: {response.status_code}")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
            
        return None
    
    def _extract_nextjs_data(self, html):
        """
        Extract Next.js preloaded data from HTML
        """
        try:
            start = html.find('id="__NEXT_DATA__"')
            if start != -1:
                start = html.find('>', start) + 1
                end = html.find('</script>', start)
                json_str = html[start:end]
                data = json.loads(json_str)
                
                # Navigate through Next.js structure to find financials
                if 'props' in data:
                    if 'pageProps' in data['props']:
                        page_props = data['props']['pageProps']
                        if 'financials' in page_props:
                            return page_props['financials']
                        elif 'data' in page_props:
                            return page_props['data']
                            
        except Exception as e:
            print(f"    Could not extract Next.js data: {e}")
            
        return None
    
    def _fallback_html_extraction(self, symbol):
        """
        Try to extract financial data from the HTML page itself
        """
        url = f"https://www.perplexity.ai/finance/{symbol}"
        
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                # Look for various patterns where data might be embedded
                patterns = [
                    ('window.__INITIAL_STATE__', ';'),
                    ('window.__PRELOADED_STATE__', ';'),
                    ('window.__DATA__', ';'),
                    ('{"annual":', '}]}'),
                ]
                
                for start_pattern, end_pattern in patterns:
                    if start_pattern in response.text:
                        start = response.text.find(start_pattern)
                        if start != -1:
                            start += len(start_pattern)
                            if start_pattern.startswith('window.'):
                                start = response.text.find('=', start) + 1
                            
                            # Find the end of JSON
                            if end_pattern == ';':
                                end = response.text.find(end_pattern, start)
                            else:
                                # Find matching closing brackets
                                bracket_count = 0
                                end = start
                                for i, char in enumerate(response.text[start:], start):
                                    if char == '{':
                                        bracket_count += 1
                                    elif char == '}':
                                        bracket_count -= 1
                                        if bracket_count == 0:
                                            end = i + 1
                                            break
                            
                            json_str = response.text[start:end].strip()
                            try:
                                data = json.loads(json_str)
                                if 'annual' in str(data):
                                    print("    ✓ Found financial data in HTML!")
                                    return data
                            except:
                                continue
                                
        except Exception as e:
            print(f"    HTML extraction error: {e}")
            
        return None
    
    def _process_data(self, data, symbol):
        """
        Process the financial data into DataFrame
        """
        all_annual_data = []
        
        # Navigate through possible data structures
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
                        year_entry = None
                        
                        # Find or create year entry
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
                                year_entry[key] = value
        
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
            preview_cols = ['date', 'revenue', 'netIncome', 'totalAssets']
            available_cols = [col for col in preview_cols if col in df.columns]
            if available_cols:
                print("\nPreview:")
                print(df[available_cols].to_string())
            
            return df
        
        print("  ✗ No annual data found in response")
        return None

# Main execution
if __name__ == "__main__":
    companies = ["ETERNAL.NS"]
    
    print("="*60)
    print("PERPLEXITY FINANCIAL DATA EXTRACTOR")
    print("="*60)
    
    for symbol in companies:
        print(f"\nProcessing: {symbol}")
        print("-"*40)
        
        scraper = PerplexitySessionScraper()
        df = scraper.fetch_financial_data(symbol)
        
        if df is None:
            print(f"\n✗ Failed to extract {symbol}")
            print("\nThe issue: Cloudflare detects automated requests by checking:")
            print("- TLS fingerprint (can't fake without browser)")
            print("- JavaScript execution (requires real browser engine)")
            print("- Session tokens (need authenticated login)")
            print("\nOnly solution: Use browser cookies from logged-in session")
        
        time.sleep(3)