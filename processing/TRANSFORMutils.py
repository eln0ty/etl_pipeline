# Data transformation utilities

import pandas as pd
from io import StringIO

def transform_csv(csv_content):
    print("Transforming CSV data...")
    try:
        df = pd.read_csv(StringIO(csv_content), delimiter=",", on_bad_lines="skip")
        df["CustomerID"] = df["CustomerID"].fillna(0).astype("int64")
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        raise Exception(f"Error transforming CSV: {e}")

def validate_data(df):
    required_columns = ['invoiceno', 'stockcode', 'description', 'quantity', 
                        'invoicedate', 'unitprice', 'customerid', 'country']
    
    # Check if all required columns are present
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise Exception(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Select only the required columns
    df = df[required_columns]
    
    # Convert timestamp
    try:
        df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    except Exception as e:
        raise Exception(f"Error converting invoicedate to timestamp: {e}")
    
    return df
