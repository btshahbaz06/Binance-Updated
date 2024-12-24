import os
import datetime
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import mysql.connector
from dotenv import load_dotenv



load_dotenv()

# Access environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db = os.getenv('DB')


# Function to write logs
def write_log(message, log_file="log.txt"):
    with open(log_file, "a") as log:
        log.write('[INFO] - ')
        log.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")

# Function to fetch live coin prices from Binance API
def get_api_data(api_url):
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            write_log("Successfully fetched API data.")
            return {item['symbol']: item['price'] for item in response.json()}
        else:
            write_log(f"API Error: Status Code {response.status_code}")
            return {}
    except Exception as e:
        write_log(f"Error fetching API data: {str(e)}")
        return {}

# Function to process a single coin row
def process_coin(row, api_prices):
    trading_pair = row['Trading Pair']
    base_asset = row['Base Asset']
    quote_asset = row['Quote Asset']

    if trading_pair in api_prices:
        price = api_prices[trading_pair]
        return {
            'Name': f"{base_asset}/{quote_asset}",
            'Symbol': trading_pair,
            'Price': price,
            'Timestamp': datetime.datetime.now()
        }
    else:
        return {
            'Name': f"{base_asset}/{quote_asset}",
            'Symbol': trading_pair,
            'Price': "N/A",
            'Timestamp': datetime.datetime.now()
        }

# Function to store data in MySQL
def store_in_mysql(data, db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            symbol VARCHAR(255),
            price VARCHAR(50),
            timestamp DATETIME
        )
        """
        cursor.execute(create_table_query)

        insert_query = """
        INSERT INTO crypto_prices (name, symbol, price, timestamp)
        VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(insert_query, [
            (item['Name'], item['Symbol'], item['Price'], item['Timestamp'])
            for item in data
        ])

        connection.commit()
        write_log("Data successfully stored in MySQL.")
    except mysql.connector.Error as e:
        write_log(f"MySQL Error: {str(e)}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main script
if __name__ == "__main__":
    write_log("Script execution started.")

    # Read input data
    input_file = 'All Coins - 2000+ results.csv'  # Replace with your input file path
    file_data = pd.read_csv(input_file)
    write_log("Loaded input CSV file.")

    # Binance API URL
    api_url = 'https://api.binance.com/api/v3/ticker/price'

    # MySQL configuration
    db_config = {
        'host': db_host,
        'user': db_user,
        'password': db_pass,
        'database': db,
    }

    while True:
        # Fetch API prices
        api_prices = get_api_data(api_url)

        # Initialize list to store results
        results = []
        not_found = []
        found = []

        # Process data with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(process_coin, row, api_prices): row
                for _, row in file_data.iterrows()
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    if result['Price'] == "N/A":
                        not_found.append(result['Symbol'])
                    else:
                        found.append(result['Symbol'])
                except Exception as e:
                    write_log(f"Error processing row: {str(e)}")

        # Store results in MySQL
        store_in_mysql(results, db_config)

        # Update log based on prices received
        if not not_found:
            write_log("Session done: Every price received.")
        else:
            write_log(f"Session incomplete: Prices not found for trading pairs: {', '.join(not_found)}. Prices found for: {', '.join(found)}.")

        # Wait for 10 seconds before the next iteration
        time.sleep(10)
