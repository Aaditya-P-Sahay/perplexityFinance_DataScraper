import requests
import pandas as pd
import json
from datetime import datetime

def fetch_financial_data(symbol):
    """
    Fetch financial data from Perplexity API
    """
    url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}?version=2.18&source=default"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
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

def create_csv_from_financial_data(symbol, output_filename=None):
    """
    Main function to fetch data and create CSV
    """
    print(f"Fetching financial data for {symbol}...")
    
    # Fetch the data
    raw_data = fetch_financial_data(symbol)
    
    if not raw_data:
        print("Failed to fetch data")
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
    # Example for the company in your data
    symbol = "ETERNAL.NS"
    
    # Create the CSV file
    df = create_csv_from_financial_data(symbol)
    
    # Optionally create a version with cleaner column names
    if df is not None:
        df_clean = create_clean_column_names(df.copy())
        clean_filename = f"{symbol}_financial_data_clean.csv"
        df_clean.to_csv(clean_filename, index=False)
        print(f"\nClean version saved to {clean_filename}")

# You can also use it for other companies:
# df = create_csv_from_financial_data("AAPL")  # For Apple
# df = create_csv_from_financial_data("MSFT")  # For Microsoft