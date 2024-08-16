import os
import csv
import pandas as pd
import requests
import tabula
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import boto3


class DataExtractor:
    BASE_API_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod"
    S3_BUCKET_NAME = "data-handling-public"
    S3_REGION = "eu-west-2"
    DATE_DETAILS_KEY = "date_details.json"
    PRODUCTS_KEY = "products.csv"

    ## Purpose: The data_extraction module handles the retrieval of data from various sources, such as databases, PDFs, APIs, and cloud storage (S3).
    # Key Functions:
    ##1 read_rds_table(connector, table): Reads a table from an RDS database and returns it as a DataFrame.
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("X_API_KEY")
        self.aws_access = os.getenv("AWS_ACCESS")
        self.aws_secret = os.getenv("AWS_SECRET")

    def _get_headers(self):
        return {"x-api-key": self.api_key}
    def read_rds_table(self, connector, table: str):
        return pd.read_sql_table(table, connector.init_db_engine())

    ## 2 retrieve_pdf_data(url): Extracts data from a PDF document available at a given URL.
    def retrieve_pdf_data(self, url: str) -> pd.DataFrame:
        return tabula.read_pdf(url, pages="all", multiple_tables=False)[0]
    
    ## 3 list_number_of_stores(): Gets the total number of stores from an API.
    def list_number_of_stores(self) -> int:
        response = requests.get(f"{self.BASE_API_URL}/number_stores", headers=self._get_headers())
        return response.json().get("number_stores", 0)

    ##4 retrieve_store_data(store_id): Retrieves detailed store data for a specific store ID from an API
    def retrieve_store_data(self, store_id: int) -> dict:
        url = f"{self.BASE_API_URL}/store_details/{store_id}"
        return requests.get(url, headers=self._get_headers()).json()

    ## 5 retrieve_stores_data(): Aggregates store data from multiple API calls concurrently.
    def retrieve_stores_data(self) -> pd.DataFrame:
        with ThreadPoolExecutor() as executor:
            store_ids = range(self.list_number_of_stores())
            response = list(executor.map(self.retrieve_store_data, store_ids))
            print("retrieve_Stores_data complete")
            executor.shutdown(wait=True)
        return pd.DataFrame(response)

    ##6 extract_from_s3(): Retrieves product data from an S3 bucket.
    def extract_from_s3(self) -> pd.DataFrame:
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.aws_access,
            aws_secret_access_key=self.aws_secret,
            region_name=self.S3_REGION
        )
        obj = s3.get_object(Bucket=self.S3_BUCKET_NAME, Key=self.PRODUCTS_KEY)
        data = obj['Body'].read().decode('utf-8').splitlines()
        return pd.DataFrame(list(csv.reader(data)))

    ## 7 retrieve_date_details(): Retrieves date details from an S3 URL.
    def retrieve_date_details(self) -> pd.DataFrame:
        url = f"https://{self.S3_BUCKET_NAME}.s3.eu-west-1.amazonaws.com/{self.DATE_DETAILS_KEY}"
        return pd.DataFrame(requests.get(url, headers=self._get_headers()).json())

