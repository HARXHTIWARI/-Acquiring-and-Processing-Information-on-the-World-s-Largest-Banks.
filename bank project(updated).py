# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import numpy as np
import sqlite3

# Initialize known variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
output_csv_path = "./Largest_banks_data.csv"
log_file_path = "code_log.txt"

# Log progress function
def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} : {message}\n"
    with open(log_file_path, "a") as log_file:
        log_file.write(log_entry)

# Log the first entry
log_progress("Preliminaries complete. Initiating ETL process")

# Extract function
def extract(url):
    log_progress("Data extraction started")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('span', {'id': 'By_market_capitalization'}).find_next('table')
    df = pd.read_html(str(table), thousands=',')[0]
    df.columns = [col.lower().replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]
    market_cap_column = [col for col in df.columns if 'market' in col.lower() and 'cap' in col.lower()][0]
    df[market_cap_column] = pd.to_numeric(df[market_cap_column].astype(str).str.replace('[^\d.]', '', regex=True), errors='coerce')
    log_progress("Data extraction complete. Initiating Transformation process")
    return df

# Function call for extract() and print the returning data frame
data_frame = extract(url)
print(data_frame)

# Transformation function
def transform(df, exchange_rate_path):
    exchange_rate = pd.read_csv(exchange_rate_path, index_col=0)
    required_currencies = ['GBP', 'EUR', 'INR']
    
    if not all(currency in exchange_rate.index for currency in required_currencies):
        raise ValueError("Exchange rate CSV is missing required currencies.")

    exchange_rate_dict = exchange_rate.loc[required_currencies].to_dict()[exchange_rate.columns[0]]

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate_dict['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate_dict['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate_dict['INR'], 2)

    return df

# Example function call
exchange_rate_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
data_frame = pd.DataFrame({
    'MC_USD_Billion': [155.87, 140.83, 432.92, 231.52, 148.90],
    # ... other columns ...
})

transformed_df = transform(data_frame, exchange_rate_path)
print(transformed_df)

# CSV Loading function
def load_to_csv(df, output_csv_path):
    df.to_csv(output_csv_path, index=False)

# Example function call
output_csv_path = './Largest_banks_data.csv'
load_to_csv(transformed_df, output_csv_path)

# Database Loading function
def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, index=False, if_exists='replace')

# Example function call
conn = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
load_to_db(transformed_df, conn, table_name)

# Query execution function
def run_queries(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    print(f"Query Statement: {query}")
    print("Query Output:")
    for row in result:
        print(row)
    print("\n")

# Function calls with queries
query_1 = "SELECT * FROM Largest_banks"
query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_3 = "SELECT * FROM Largest_banks LIMIT 5"

run_queries(query_1, conn)
run_queries(query_2, conn)
run_queries(query_3, conn)
