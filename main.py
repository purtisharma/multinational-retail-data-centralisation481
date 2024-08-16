# Import necessary modules
import database_utils as utils  # Module for handling database connections
import data_extraction as extract  # Module for extracting data from various sources
import data_cleaning as clean  # Module for cleaning and processing data
import pandas as pd  # Library for data manipulation

# Initialize instances of database connector, data extractor, and data cleaner
connector = utils.DatabaseConnector()  # Creates a connection to the database
extractor = extract.DataExtractor()  # Handles data extraction from different sources
cleaner = clean.DataCleaning()  # Performs data cleaning operations

# Define the path where CSV files will be saved for inspection
file_path_prefix = "/Users/PurtiSharma/OneDrive - CARE International/code/ai_core/downloads/"

# Configure pandas options for better display of large data frames
pd.set_option('display.max_columns', 2000)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)

# Define a dictionary to manage data extraction, cleaning, and storage for each type of data
data_processes = {
    "users": {
        "read": lambda: extractor.read_rds_table(connector, "legacy_users"),
        "clean": cleaner.clean_user_data,
        "db_table": "dim_users"
    },
    "cards": {
        "read": lambda: extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"),
        "clean": cleaner.clean_card_data,
        "db_table": "dim_card_details"
    },
    "stores": {
        "read": extractor.retrieve_stores_data,
        "clean": cleaner.clean_store_data,
        "db_table": "dim_store_details"
    },
    "products": {
        "read": extractor.extract_from_s3,
        "clean": cleaner.clean_products_data,
        "db_table": "dim_products"
    },
    "orders": {
        "read": lambda: extractor.read_rds_table(connector, "orders_table"),
        "clean": cleaner.clean_orders_data,
        "db_table": "orders_table"
    },
    "dates": {
        "read": extractor.retrieve_date_details,
        "clean": cleaner.clean_date_data,
        "db_table": "dim_date_times"
    }
}

def process_data(name, process):
    """
    Handles the data processing workflow for each data type.
    
    Args:
        name (str): The name of the data type (e.g., 'users').
        process (dict): A dictionary containing the read function, clean function, and database table name.
    """
    # Step 1: Read raw data using the specified read function
    raw_data = process["read"]()
    # Save the raw data to a CSV file for inspection
    raw_data.to_csv(f"{file_path_prefix}{name}_dirty.csv")
    
    # Step 2: Clean the raw data using the specified clean function
    clean_data = process["clean"](raw_data)
    # Save the cleaned data to a CSV file for inspection
    clean_data.to_csv(f"{file_path_prefix}{name}_clean.csv")
    
    # Step 3: Upload the cleaned data to the database
    connector.upload_to_db(clean_data, process["db_table"])

# Iterate over each data type and apply the process_data function
for name, process in data_processes.items():
    process_data(name, process)
