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
    '''
    Class to handle ingesting files from Azure Blob Storage.
    '''
    meta_data = "file_system_ingestion_meta_data.xlsx"
    def __init__(self, account_sheet = '', files_sheet = ''):
        """
        Initializes the AzureFileSystem class.

        Args:
            account_sheet (str): Name of the sheet containing account details (default is '').
            files_sheet (str): Name of the sheet containing file details (default is '').
        """
        self.account_sheet = account_sheet
        self.files_sheet = files_sheet

    def preprocess_columns(self, df = pd.DataFrame()):
        """
        Preprocesses the columns of a DataFrame by removing leading and trailing spaces.

        Args:
            df (pd.DataFrame): Input DataFrame to preprocess (default is an empty DataFrame).

        Returns:
            pd.DataFrame: Preprocessed DataFrame.
            str: Error message if preprocessing fails.
        """
        try:
            # Strip leading and trailing spaces from object columns
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].str.strip()
            return df
        except:
            print(traceback.format_exc())
            return 'preprocess_column failed'
            
    def fetch_active_accounts(self):
        """
        Fetches the active Azure storage accounts from the meta-data file.

        Returns:
            pd.DataFrame: DataFrame containing the active Azure storage accounts.
        """
        fetch_acc = pd.read_excel(AzureFileSystem.meta_data, sheet_name = self.account_sheet)
        fetch_acc = fetch_acc[fetch_acc['isActive']==1]
        fetch_acc= self.preprocess_columns(fetch_acc)
        return fetch_acc

    def fetch_active_files(self):
        """
        Fetches the active files from the meta-data file.

        Returns:
            pd.DataFrame: DataFrame containing the active files.
        """
        fetch_files = pd.read_excel(AzureFileSystem.meta_data, sheet_name = self.files_sheet)
        fetch_files = fetch_files[fetch_files['isActive'] == 1]
        fetch_files = self.preprocess_columns(fetch_files)
        return fetch_files

    def store_data(self, df = pd.DataFrame, file_path = '', table_name = "table"):
        """
        Stores the DataFrame as a CSV file.

        Args:
            df (pd.DataFrame): DataFrame to store.
            file_path (str): Path to store the file (default is '').
            table_name (str): Name of the table (default is "table").
        """
        filename = f"{table_name}.csv"
        df.to_csv(filename, index=False)
        destination = os.path.join(file_path, filename)
        shutil.move(filename, destination)
        print(f"Table '{table_name}' extracted and saved as '{filename}'.")
    
    def azure_ingestion_engine(self, df = pd.DataFrame(), connection_string=''):
        """
        Performs file ingestion from Azure Blob Storage.

        Args:
            df (pd.DataFrame): DataFrame containing the Azure file details.
            connection_string (str): Connection string for Azure Blob Storage.
        """
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
            target_directory = r'C:\ingestion_file_system_final\Ingested_files\Azure'
            table_name = file_name.split('.')[0]
            self.store_data(df, target_directory, table_name)
    

    def azure_file_ingestion(self):
        """
        Perform the Azure file ingestion process.
        """
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
            
    


