import json
import sqlalchemy as alch

# database_connector.py
# database_connector.py

import yaml
from sqlalchemy import create_engine, inspect
import psycopg2  # PostgreSQL adapter

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
        self.creds = self.read_db_creds(creds_file)  # Load credentials
        self.engine = self.init_db_engine()  # Initialize SQLAlchemy engine
        self.local_engine = self.init_local_db_engine()

    def read_db_creds(self, creds_file):
        """
        Load database credentials from a YAML file.
        """
        with open(creds_file, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        """
        Initialize and return a SQLAlchemy engine using credentials.
        """
        creds = self.creds
        connection_string = (
            f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
            f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return create_engine(connection_string)

    def init_local_db_engine(self):
        """
        Initialize and return a SQLAlchemy engine using credentials.
        """
        creds = self.creds
        connection_string = (
            f"postgresql+psycopg2://{creds['LDB_USER']}:{creds['LDB_PASSWORD']}@"
            f"{creds['LDB_HOST']}:{creds['LDB_PORT']}/{creds['LDB_DATABASE']}"
        )
        return create_engine(connection_string)
    
    def get_connection(self):
        """
        Get a database connection from the engine.
        """
        return self.engine.connect()
    
    def get_local_connection(self):
        """
        Get a database connection from the engine.
        """
        return self.local_engine.connect()
    
    def list_db_tables(self):
        """
        List all tables in the database.
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        """
        Upload a DataFrame to a specified table in the database.
        """
        df.to_sql(table_name, self.local_engine, if_exists='replace', index=False)


# class DatabaseConnector:
#     def __init__(self, creds_file: str = "db_creds.json") -> None:
#         """Initializes the DatabaseConnector with database credentials."""
#         # Step: Configuration
#         self.creds = self.read_db_creds(creds_file)
    
#     def read_db_creds(self, creds_file: str) -> dict:
#         """Reads database credentials from a JSON file."""
#         # Step: Configuration
#         with open(creds_file, 'r') as f:
#             return json.load(f)
    
#     def init_db_engine(self, creds_key_prefix: str = 'RDS') -> alch.engine.base.Engine:
#         """Initializes and returns a SQLAlchemy engine using the specified credentials."""
#         return alch.create_engine(
#             f"postgresql+psycopg://{self.creds[f'{creds_key_prefix}_USER']}:{self.creds[f'{creds_key_prefix}_PASSWORD']}@"
#             f"{self.creds[f'{creds_key_prefix}_HOST']}:{self.creds[f'{creds_key_prefix}_PORT']}/"
#             f"{self.creds[f'{creds_key_prefix}_DATABASE']}")
    
#     def list_db_tables(self) -> list:
#         """Lists all tables in the database."""
#         return alch.inspect(self.init_db_engine()).get_table_names()

#     def upload_to_db(self, df, name: str, creds_key_prefix: str = 'LOC') -> None:
#         """Uploads a DataFrame to the database with the specified name."""
#         engine = self.init_db_engine(creds_key_prefix)
#         df.to_sql(name, engine, if_exists='replace')
