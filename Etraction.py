import pandas as pd

# data extraction function
def run_extraction():
    
    try:
        data = pd.read_csv(r'zipco_transaction.csv')
        print("Data extraction successful.")
    except Exception as e:
        print(f"Data extraction failed: {e}")