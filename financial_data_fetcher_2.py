import requests
import pandas as pd
import json
from datetime import datetime

def fetch_financial_data(symbol):
    """
    Fetch financial data from Perplexity API with proper headers
    """
    url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}?version=2.18&source=default"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        # Try with headers first
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data with headers: {e}")
        
        # Try with session and different headers
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.perplexity.ai/',
                'Origin': 'https://www.perplexity.ai'
            })
            response = session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e2:
            print(f"Error fetching data with session: {e2}")
            return None

def load_from_local_file(filename):
    """
    Load financial data from a local JSON file (fallback option)
    """
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Local file {filename} not found")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {filename}: {e}")
        return None

def extract_annual_data(data):
    """
    Extract annual financial data and organize it into a structured format
    """
    if not data or 'annual' not in data:
        print("No annual data found")
        return None
    
    annual_data = data['annual']
    
    # Initialize the result dictionary
    result = {}
    
    # Process each financial statement type
    for statement in annual_data:
        statement_type = statement['type']
        statement_data = statement['data']
        
        for year_data in statement_data:
            year = year_data.get('calendarYear', year_data.get('date', ''))
            
            if year not in result:
                result[year] = {'year': year}
            
            # Add all fields from this statement to the year's data
            for key, value in year_data.items():
                if key not in ['symbol', 'reportedCurrency', 'cik', 'fillingDate', 
                              'acceptedDate', 'calendarYear', 'period', 'link', 'finalLink']:
                    # Create a unique column name with statement type prefix
                    if statement_type == 'INCOME_STATEMENT':
                        prefix = 'IS_'
                    elif statement_type == 'BALANCE_SHEET':
                        prefix = 'BS_'
                    elif statement_type == 'CASH_FLOW':
                        prefix = 'CF_'
                    elif statement_type == 'KEY_STATS':
                        prefix = 'KS_'
                    else:
                        prefix = f'{statement_type}_'
                    
                    column_name = f"{prefix}{key}"
                    result[year][column_name] = value
    
    return result

def create_csv_from_financial_data(symbol, output_filename=None, local_file=None):
    """
    Main function to fetch data and create CSV
    """
    print(f"Processing financial data for {symbol}...")
    
    # Try to fetch from API first, then fall back to local file
    raw_data = None
    
    if local_file:
        print(f"Loading data from local file: {local_file}")
        raw_data = load_from_local_file(local_file)
    else:
        print("Attempting to fetch from API...")
        raw_data = fetch_financial_data(symbol)
        
        # If API fails, try to load from a default local file
        if not raw_data:
            default_local_file = f"{symbol}_data.json"
            print(f"API failed, trying local file: {default_local_file}")
            raw_data = load_from_local_file(default_local_file)
    
    if not raw_data:
        print("Failed to get data from both API and local file")
        print("\nTo use local data:")
        print("1. Save your JSON data to a file (e.g., 'ETERNAL.NS_data.json')")
        print("2. Run: create_csv_from_financial_data('ETERNAL.NS', local_file='ETERNAL.NS_data.json')")
        return
    
    # Extract annual data
    structured_data = extract_annual_data(raw_data)
    
    if not structured_data:
        print("No data to process")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(structured_data, orient='index')
    
    # Sort by year
    df = df.sort_values('year')
    
    # Reorder columns to have year first, then group by statement type
    columns = list(df.columns)
    columns.remove('year')
    
    # Group columns by statement type
    income_cols = [col for col in columns if col.startswith('IS_')]
    balance_cols = [col for col in columns if col.startswith('BS_')]
    cashflow_cols = [col for col in columns if col.startswith('CF_')]
    keystats_cols = [col for col in columns if col.startswith('KS_')]
    other_cols = [col for col in columns if not any(col.startswith(prefix) for prefix in ['IS_', 'BS_', 'CF_', 'KS_'])]
    
    # Reorder columns
    ordered_columns = ['year'] + income_cols + balance_cols + cashflow_cols + keystats_cols + other_cols
    df = df.reindex(columns=ordered_columns)
    
    # Generate filename if not provided
    if not output_filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{symbol}_financial_data_{timestamp}.csv"
    
    # Save to CSV
    df.to_csv(output_filename, index=False)
    
    print(f"Data saved to {output_filename}")
    print(f"Shape: {df.shape}")
    print(f"Years covered: {sorted(df['year'].unique())}")
    
    # Display first few columns and rows as preview
    print("\nPreview of the data:")
    preview_cols = ['year'] + list(df.columns[1:6])  # Show year + first 5 data columns
    print(df[preview_cols].to_string())
    
    return df

def create_clean_column_names(df):
    """
    Optional function to create cleaner column names
    """
    column_mapping = {
        'IS_revenue': 'Revenue',
        'IS_grossProfit': 'Gross_Profit',
        'IS_operatingIncome': 'Operating_Income',
        'IS_netIncome': 'Net_Income',
        'IS_eps': 'EPS',
        'IS_ebitda': 'EBITDA',
        'BS_totalAssets': 'Total_Assets',
        'BS_totalLiabilities': 'Total_Liabilities',
        'BS_totalStockholdersEquity': 'Shareholders_Equity',
        'BS_cashAndCashEquivalents': 'Cash_and_Equivalents',
        'BS_totalDebt': 'Total_Debt',
        'CF_operatingCashFlow': 'Operating_Cash_Flow',
        'CF_freeCashFlow': 'Free_Cash_Flow',
        'CF_capitalExpenditure': 'Capital_Expenditure',
        'KS_marketCapitalization': 'Market_Cap',
        'KS_enterpriseValue': 'Enterprise_Value'
    }
    
    # Rename columns if they exist
    df_renamed = df.rename(columns=column_mapping)
    return df_renamed

# Example usage
if __name__ == "__main__":
    # Since you have the JSON data, let's create a JSON file first and then process it
    
    # Method 1: Use with local JSON file (RECOMMENDED for now)
    # First, save your JSON data to a file named 'ETERNAL_NS_data.json'
    print("=== Method 1: Using Local JSON File ===")
    print("1. Save your JSON data to 'ETERNAL_NS_data.json'")
    print("2. Then run the script")
    
    symbol = "ETERNAL.NS"
    
    # Try API first, then fallback to local file
    df = create_csv_from_financial_data(symbol)
    
    # Method 2: Direct processing if you have JSON data as string
    print("\n=== Method 2: Direct Processing ===")
    
    # If you want to process the data directly from your uploaded file:
    # You can copy the JSON content and process it directly
    sample_json_str = '''
    {
      "annual": [
        {
          "type": "INCOME_STATEMENT",
          "data": [...]
        }
      ]
    }
    '''
    
    print("To process data directly:")
    print("1. Copy your JSON data")
    print("2. Use: process_json_string(json_string, 'ETERNAL.NS')")

def process_json_string(json_string, symbol):
    """
    Process JSON string directly without file I/O
    """
    try:
        raw_data = json.loads(json_string)
        structured_data = extract_annual_data(raw_data)
        
        if not structured_data:
            print("No data to process")
            return
        
        df = pd.DataFrame.from_dict(structured_data, orient='index')
        df = df.sort_values('year')
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{symbol}_financial_data_{timestamp}.csv"
        
        # Save to CSV
        df.to_csv(output_filename, index=False)
        print(f"Data saved to {output_filename}")
        return df
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None