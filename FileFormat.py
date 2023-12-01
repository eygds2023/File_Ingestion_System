import pandas as pd



class FormatClass():
    """
    Class to handle file format conversions.
    """
    
    def __init__(self, files_df = pd.DataFrame(), meta_data = ''):
        """
        Initializes the FormatClass.

        Args:
            files_df (pd.DataFrame): DataFrame containing file details (default is an empty DataFrame).
            meta_data (str): Name of the meta-data file (default is '').
        """
        self.files_df = files_df
        self.meta_data = meta_data

    def handle_parquet(self, format_sheet = ''):
        """
        Handles Parquet files.

        Args:
            format_sheet (str): Name of the sheet containing Parquet format details (default is '').

        Returns:
            pd.DataFrame: DataFrame containing Parquet file details.
        """
        parquet_df = pd.read_excel(self.meta_data, sheet_name = format_sheet)
        parquet_files_df = self.files_df[self.files_df['format'] == 'parquet']
        file_system_parquet = parquet_files_df.merge(parquet_df, on = 'FUID', how= 'left')
        file_system_parquet = file_system_parquet.rename(columns={'file_name_x': 'file_name'})
        file_system_parquet = file_system_parquet[['FUID','file_name', 'container', 'blob_path', 'format']]
        file_system_parquet['modified_file_name'] = file_system_parquet.apply(lambda row: f"{row['file_name'].split('.')[0]}_{row['FUID']}.parquet", axis=1)
        
        return file_system_parquet
    def handle_csv(self, format_sheet = ''):
        """
        Handles CSV files.
        format_sheet (str): Name of the sheet containing CSV format details (default is '').

        Returns:
            pd.DataFrame: DataFrame containing CSV file details.
        """
        csv_df = pd.read_excel(self.meta_data, sheet_name = format_sheet)
        csv_files_df = self.files_df[self.files_df['format'] == 'csv']
        file_system_csv = csv_files_df.merge(csv_df, on = 'FUID', how= 'left')
        file_system_csv = file_system_csv.rename(columns={'file_name_x': 'file_name'})
        file_system_csv = file_system_csv[['FUID','file_name', 'container', 'blob_path', 'format']]
        file_system_csv['modified_file_name'] = file_system_csv.apply(lambda row: f"{row['file_name'].split('.')[0]}_{row['FUID']}.csv", axis=1)
        return file_system_csv
    
    def handle_orc(self, format_sheet = ''):
        """
        Handles ORC (Optimized Row Columnar) files.

        Args:
            format_sheet (str): Name of the sheet containing ORC format details (default is '').

        Returns:
            pd.DataFrame: DataFrame containing ORC file details.
        """
        # TODO: Implement ORC file handling
        pass
        
    def handle_avro(self, format_sheet = ''):
        """
        Handles Avro files.

        Args:
            format_sheet (str): Name of the sheet containing Avro format details (default is '').

        Returns:
            pd.DataFrame: DataFrame containing Avro file details.
        """
        # TODO: Implement Avro file handling
        pass