import pandas as pd



##Purpose: The data_cleaning module is responsible for transforming raw data into a clean and consistent format. It ensures that data is in a usable state before it is uploaded to the database.
## Key Functions:

import pandas as pd

##1 clean_user_data(df): Cleans user data by formatting dates and phone numbers, and removing rows with missing critical information.
class DataCleaning:
    def clean_user_data(self, df):
        df = df.dropna()  # Remove rows with null values
#b Cleaning and formatting date of birth 
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')  # Convert to datetime
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')  # Convert to datetime
        df = df.dropna(subset=['date_of_birth', 'join_date'])  # Drop rows with invalid dates
        #b Cleaning and formatting phone numbers 
        df["phone_number"] = df["phone_number"].str.replace(r'[^0-9+\-()\s]', '', regex=True) 
        df["phone_number"] = df["phone_number"].str.strip().str.replace(r'\s+', ' ', regex=True) 
        #c Cleaning and formatting address
        df['address'] = df['address'].str.replace('\n', ', ') 
        df = df.dropna(subset=['date_of_birth', 'join_date'], how='all') 
        return df

## 2 clean_card_data(df): Cleans card data by formatting card numbers, remove null and ensuring valid expiry dates.
    def clean_card_data(self, df):
        """
        Cleans card data by:
        - Removing rows where values match column names
        - Formatting card numbers
        - Converting dates to datetime
        - Filtering valid expiry dates
        
        Args:
            df (pd.DataFrame): DataFrame with card data.
        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        # Remove rows where all values are the column names
        df = df[~df.apply(lambda row: all(row == df.columns), axis=1)]

        # Keep only digits in 'card_number'
        df['card_number'] = df['card_number'].str.replace(r'\D', '', regex=True)

        # Convert 'date_payment_confirmed' to datetime
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        # Filter rows with valid 'expiry_date' format MM/YY
        df = df[df['expiry_date'].str.match(r"^(0[1-9]|1[0-2])\/\d{2}$").fillna(False)]
        print("Finish clean_card_data")
        return df

    ## 3 clean_store_data(df): Cleans store data by removing unnecessary columns and converting data types.
    def clean_store_data(self, df):
        # Remove unnecessary columns if they exist
        df = df.drop(columns=['message', 'lat', 'index'], errors='ignore')
    
        # Replace newlines in addresses with spaces
        df['address'] = df['address'].str.replace('\n', ' ', regex=False)
        
        # Remove leading 'ee' from continent names
        df['continent'] = df['continent'].str.replace('^ee', '', regex=True)
        
        # Convert opening dates to datetime format
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        
        # Convert longitude and latitude to numeric values
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        
        # Convert staff numbers to integers
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce', downcast='integer')
        
        # Remove rows where all of the critical columns are missing
        df = df.dropna(subset=['staff_numbers', 'longitude', 'opening_date', 'latitude'], how='all')
        print(df)
        return df
    
    def convert_product_weights(self, w: str):
        """Converts product weight strings to kilograms."""
        w = w.strip()  # Remove any leading or trailing whitespace

        try:
            if 'kg' in w:
                return float(w.replace('kg', ''))  # Return value in kg
            elif 'g' in w:
                return float(w.replace('g', '')) / 1000  # Convert grams to kilograms
            elif 'ml' in w:
                return float(w.replace('ml', '')) / 1000  # Convert milliliters to kilograms
            else:
                return None  # Return None for unrecognized units
        except ValueError:
            return None  # Return None if conversion fails
    ## 4 clean_products_data(df): Cleans products data by converting weight units and formatting dates.
    def clean_products_data(self, df):
        # Remove the first column if it exists
        df = df.drop(columns=[0], errors='ignore')
        
        # Set the first row as the header and remove it from the data
        df.columns = df.iloc[0]
        df = df.drop(0)
        
        # Reset the index of the DataFrame
        df = df.reset_index(drop=True)
        
        # Convert 'date_added' column to datetime format
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        
        # Apply the weight conversion function to 'weight' column
        df['weight'] = df['weight'].apply(self.convert_product_weights)
        
        # Drop rows where 'date_added' is missing
        df = df.dropna(subset=['date_added'])
    
        return df
    ## 5 clean_orders_data(df): Cleans orders data by removing unnecessary columns.
    def clean_orders_data(self, df):
        # Remove specified columns if they exist
        df = df.drop(columns=['level_0', 'index', 'first_name', 'last_name', '1'], errors='ignore')
        return df
    ## 6 clean_date_data(df): Cleans date data by converting date-related columns to numeric types and dropping rows with missing values.
    def clean_date_data(self, df):
        # Convert columns to numeric types
        df['month'] = pd.to_numeric(df['month'], errors='coerce', downcast='integer')
        df['year'] = pd.to_numeric(df['year'], errors='coerce', downcast='integer')
        df['day'] = pd.to_numeric(df['day'], errors='coerce', downcast='integer')
        # Drop rows with any missing values
        df = df.dropna(how='any')
        return df
