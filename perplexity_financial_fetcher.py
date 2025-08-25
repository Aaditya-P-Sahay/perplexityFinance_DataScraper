import requests
import pandas as pd
import json
from datetime import datetime

def fetch_financial_data(symbol="ETERNAL.NS"):
    """
    Fetch financial data from Perplexity API and save to CSV
    """
    # URL for the API
    url = f"https://www.perplexity.ai/rest/finance/financials/{symbol}?version=2.18&source=default"
    
    # Headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.perplexity.ai/',
    }
    
    try:
        # Fetch data from API
        print(f"Fetching data for {symbol}...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Initialize list to store all annual data
        all_annual_data = []
        
        # Process annual data only
        if 'annual' in data:
            for statement in data['annual']:
                statement_type = statement.get('type', 'UNKNOWN')
                
                if 'data' in statement and statement['data']:
                    for year_data in statement['data']:
                        # Find existing year entry or create new one
                        date = year_data.get('date', '')
                        year_entry = None
                        
                        for entry in all_annual_data:
                            if entry.get('date') == date:
                                year_entry = entry
                                break
                        
                        if year_entry is None:
                            year_entry = {'date': date}
                            all_annual_data.append(year_entry)
                        
                        # Add all metrics from this statement to the year entry
                        for key, value in year_data.items():
                            if key != 'date':
                                # Prefix with statement type to avoid conflicts
                                column_name = f"{statement_type}_{key}" if key in ['symbol', 'reportedCurrency', 'period'] else key
                                year_entry[column_name] = value
        
        # Convert to DataFrame
        df = pd.DataFrame(all_annual_data)
        
        # Sort by date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
        
        # Reorder columns to have date first, then alphabetically
        if not df.empty:
            cols = df.columns.tolist()
            if 'date' in cols:
                cols.remove('date')
                cols = ['date'] + sorted(cols)
                df = df[cols]
        
        # Save to CSV
        filename = f"{symbol}_annual_financials.csv"
        df.to_csv(filename, index=False)
        print(f"\nData successfully saved to {filename}")
        print(f"Total rows (years): {len(df)}")
        print(f"Total columns (metrics): {len(df.columns)}")
        
        # Display first few columns and rows as preview
        print("\nPreview of data (first 5 columns, all years):")
        preview_cols = df.columns[:5].tolist()
        print(df[preview_cols].to_string())
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Alternative: Process from local JSON file if API is not accessible
def process_local_json(json_file_path, output_csv="ETERNAL.NS_annual_financials.csv"):
    """
    Process financial data from a local JSON file
    Use this if you've saved the API response to a file
    """
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Initialize list to store all annual data
        all_annual_data = []
        
        # Process annual data only
        if 'annual' in data:
            for statement in data['annual']:
                statement_type = statement.get('type', 'UNKNOWN')
                
                if 'data' in statement and statement['data']:
                    for year_data in statement['data']:
                        # Find existing year entry or create new one
                        date = year_data.get('date', '')
                        year_entry = None
                        
                        for entry in all_annual_data:
                            if entry.get('date') == date:
                                year_entry = entry
                                break
                        
                        if year_entry is None:
                            year_entry = {'date': date}
                            all_annual_data.append(year_entry)
                        
                        # Add all metrics from this statement to the year entry
                        for key, value in year_data.items():
                            if key != 'date':
                                # Prefix with statement type for duplicate keys
                                column_name = f"{statement_type}_{key}" if key in ['symbol', 'reportedCurrency', 'period'] else key
                                year_entry[column_name] = value
        
        # Convert to DataFrame
        df = pd.DataFrame(all_annual_data)
        
        # Sort by date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
        
        # Reorder columns
        if not df.empty:
            cols = df.columns.tolist()
            if 'date' in cols:
                cols.remove('date')
                cols = ['date'] + sorted(cols)
                df = df[cols]
        
        # Save to CSV
        df.to_csv(output_csv, index=False)
        print(f"\nData successfully saved to {output_csv}")
        print(f"Total rows (years): {len(df)}")
        print(f"Total columns (metrics): {len(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"Error processing local JSON: {e}")
        return None

# Main execution
if __name__ == "__main__":
    # Try fetching from API
    df = fetch_financial_data("ETERNAL.NS")
    
    # If API fetch fails and you have the JSON saved locally, uncomment below:
    # df = process_local_json("perplexityEternalPrettyPrint.txt")
    
    if df is not None:
        print("\n✅ Financial data extraction complete!")
        
        # Optional: Display column names for reference
        print("\nAll available metrics (column names):")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:3}. {col}")
