# import needed packages
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os


def run_loading():

    # load data from csv files
    data = pd.read_csv('cleaned_data.csv')
    products = pd.read_csv('products.csv')
    customers = pd.read_csv('customers.csv')    
    staff = pd.read_csv('staff.csv')
    transactions = pd.read_csv('transactions.csv')
    

    # data loading to Azure Blob Storage from .env file
    load_dotenv()

    connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
    # Try multiple possible container name variable names
    container_name = os.getenv('ZICOFOOD_CONTAINER_NAME') or os.getenv('zicofoodscontainer') or os.getenv('container_name')

    # Validate credentials
    if not connect_str:
        print("ERROR: AZURE_CONNECTION_STRING_VALUE not found in .env file")
    elif not container_name:
        print("ERROR: Container name not found. Please set one of these in .env:")
        print("  - ZICOFOOD_CONTAINER_NAME=your_container_name")
        print("  - zicofoodscontainer=your_container_name")
        print("  - container_name=your_container_name")
    else:
        print(f"✓ Connection string found")
        print(f"✓ Container name: {container_name}")

    # create BlobServiceClient object
    if not container_name:
        print("Cannot proceed: container name is missing. Please fix the .env file first.")
    else:
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_client = blob_service_client.get_container_client(container_name)
            
            # function to upload file to blob storage
            files = [
                (data, 'rawdata/cleaned_data.csv'),
                (products, 'products/products.csv'),
                (customers, 'customers/customers.csv'),
                (staff, 'staff/staff.csv'),
                (transactions, 'transactions/transactions.csv')
            ]
            
            for file, blob_name in files:
                blob_client = container_client.get_blob_client(blob_name)
                output = file.to_csv(index=False)
                blob_client.upload_blob(output, overwrite=True)
                print(f"✓ Uploaded {blob_name} to Azure Blob Storage.")
                
        except Exception as e:
            print(f"ERROR uploading to Azure: {e}")
    