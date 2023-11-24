import pandas as pd
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from FileFormat import FormatClass
import io
import os
import shutil
import traceback
import warnings
warnings.filterwarnings('ignore')

class AzureFileSystem():
    meta_data = "file_system_ingestion_meta_data.xlsx"
    def __init__(self, account_sheet = '', files_sheet = ''):
        self.account_sheet = account_sheet
        self.files_sheet = files_sheet

    def preprocess_columns(self, df = pd.DataFrame()):
        try:
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].str.strip()
            return df
        except:
            print(traceback.format_exc())
            return 'preprocess_column failed'
    def fetch_active_accounts(self):
        fetch_acc = pd.read_excel(AzureFileSystem.meta_data, sheet_name = self.account_sheet)
        fetch_acc = fetch_acc[fetch_acc['isActive']==1]
        fetch_acc= self.preprocess_columns(fetch_acc)
        return fetch_acc

    def fetch_active_files(self):
        fetch_files = pd.read_excel(AzureFileSystem.meta_data, sheet_name = self.files_sheet)
        fetch_files = fetch_files[fetch_files['isActive'] == 1]
        fetch_files = self.preprocess_columns(fetch_files)
        return fetch_files

    def store_data(self, df = pd.DataFrame, file_path = '', table_name = "table"):
        filename = f"{table_name}.csv"
        df.to_csv(filename, index=False)
        destination = os.path.join(file_path, filename)
        shutil.move(filename, destination)
        print(f"Table '{table_name}' extracted and saved as '{filename}'.")
    
    def azure_ingestion_engine(self, df = pd.DataFrame(), connection_string=''):
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        for row in df.itertuples():
            container_name = getattr(row, 'container')
            blob_name = getattr(row, 'blob_path')
            format = getattr(row, 'format')
            file_name = getattr(row, 'modified_file_name')
            container_client = blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            file_data = blob_client.download_blob().readall()
            bytes_io = io.BytesIO(file_data)
            if format == 'parquet':
                print("Parquet")
                df = pd.read_parquet(bytes_io)
            if format == 'csv':
                print("CSV")
                df = pd.read_csv(bytes_io)
            print(df)
            target_directory = r'C:\ingestion_file_system_3\Ingested_files\Azure'
            table_name = file_name.split('.')[0]
            self.store_data(df, target_directory, table_name)
    

    def azure_file_ingestion(self):
        accounts_df = self.fetch_active_accounts()
        for account in accounts_df.itertuples():
            sid = getattr(account,'SID')
            connection_str = getattr(account, 'conn_string')

            files_df = self.fetch_active_files()
            files_df = files_df[files_df['SID'] == sid]
            azure_df = FormatClass(files_df, meta_data = self.meta_data)
            azure_csv_files = azure_df.handle_csv('csv_files')
            # self.azure_ingestion_engine(azure_csv_files, connection_str)
            self.azure_ingestion_engine(azure_csv_files, connection_str)
            print()
            azure_parquet_files = azure_df.handle_parquet('parquet_files')
            self.azure_ingestion_engine(azure_parquet_files, connection_str)
            print()
            
    


