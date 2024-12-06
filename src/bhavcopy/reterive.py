import pandas as pd
import json
import psycopg2
from . import config

#CONFIG_PATH = r'E:\Bhavcopy_Package\BhavCopyMark1\BhavCopy\src\bhavcopy\config.json'

# Mapping for headers between the two tables
COLUMN_MAPPING = {
    "SYMBOL": "TckrSymb",
    "SERIES": "SctySrs",
    "OPEN": "OpnPric",
    "HIGH": "HghPric",
    "LOW": "LwPric",
    "CLOSE": "ClsPric",
    "LAST": "LastPric",
    "PREVCLOSE": "PrvsClsgPric",
    "TOTTRDQTY": "TtlTradgVol",
    "TOTTRDVAL": "TtlTrfVal",
    "TIMESTAMP": "TradDt",
    "TOTALTRADES": "TtlNbOfTxsExctd",
    "ISIN": "ISIN"
}

def get_config_data():
    """Load configuration data from the JSON file."""
    # with open(CONFIG_PATH, 'r') as file:
    #     return json.load(file)
    return config.conf


def establish_connection():
    """Establish and return a database connection."""
    config = get_config_data()
    return psycopg2.connect(
        host=config['hostname'],
        database=config['database'],
        user=config['username'],
        password=config['pwd'],
        port=config['port']
    )

def fetch_data(conn, startdate, enddate, symbol, series, table_name):
    """Fetch data from the specified table based on provided parameters."""
    cur = conn.cursor()

    if table_name == "bhavcopies_cm":
        query = """
            SELECT * FROM  bhavcopies_cm
            WHERE timestamp >= %s AND timestamp <= %s
            AND symbol = %s AND series = %s
        """
        columns = [
            "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST",
            "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN"
        ]
       
    elif table_name == "bhavcopies_udiff":
        query = """
            SELECT * FROM bhavcopies_udiff 
            WHERE TradDt >= %s AND TradDt <= %s 
            AND TckrSymb = %s AND SctySrs = %s
        """
        columns = [
            "TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN",
            "TckrSymb", "SctySrs", "XpryDt", "FininstrmActlXpryDt", "StrkPric", "OptnTp",
            "FinInstrmNm", "OpnPric", "HghPric", "LwPric", "ClsPric", "LastPric",
            "PrvsClsgPric", "UndrlygPric", "SttlmPric", "OpnIntrst", "ChngInOpnIntrst",
            "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd", "SsnId", "NewBrdLotQty",
            "Rmks", "Rsvd1", "Rsvd2", "Rsvd3", "Rsvd4"
        ]
    else:
        raise ValueError("Invalid table name provided.")

    cur.execute(query, (startdate, enddate, symbol, series))
    
    result = cur.fetchall()

    return pd.DataFrame(result, columns=columns)

def map_columns(dataframe, mapping, source_table):
    """Map the columns of the given DataFrame to a unified format."""
    if source_table == "bhavcopies_cm":
        dataframe = dataframe.rename(columns=mapping)

    return dataframe

def get_bhavcopy(startdate, enddate, symbols, series):
    """Get the BhavCopy data for multiple symbols over a specified date range, ensuring consistent column mapping across different data sources.
        This function retrieves historical data from 2016-01-01 to yesterday's date
Parameters:
    startdate (str): The starting date for the data retrieval in 'YYYY-MM-DD' format.
    enddate (str): The ending date for the data retrieval in 'YYYY-MM-DD' format.
    symbols (list): A list of financial symbols (e.g., stock tickers) for which data is to be fetched.
    series (str): The type of data series to retrieve (e.g., 'EQ', 'GB','GS','SG').

Examples:
    Example 1: Fetching Equity Data for Specific Stocks
        start_date = '2023-01-01'
        end_date = '2023-01-31'
        symbols = ['TCS', 'TECHM', 'HDFCBANK']
        series = 'EQ'
        bhavcopy_data = get_bhavcopy(start_date, end_date, symbols, series)

    Example 2: Fetching Gold Bond Data for Multiple Symbols
        start_date = '2023-02-01'
        end_date = '2023-02-15'
        symbols = ['SGBSEP31II', 'SGBSEP27']
        series = 'GB'
        bhavcopy_data = get_bhavcopy(start_date, end_date, symbols, series)

    Example 3: Fetching Data for a Single Symbol
        start_date = '2023-03-01'
        end_date = '2023-03-10'
        symbols = ['TCS']
        series = 'EQ'
        bhavcopy_data = get_bhavcopy(start_date, end_date, symbols, series)

    Example 4: Fetching Data Over a Longer Date Range
        start_date = '2016-01-01'
        end_date = '2023-04-30'
        symbols = ['TCS', 'TECHM', 'HDFCBANK']
        series = 'EQ'
        bhavcopy_data = get_bhavcopy(start_date, end_date, symbols, series)
"""
    conn = None
    all_data = pd.DataFrame()  # Initialize an empty DataFrame for all symbols
    
    try:
        conn = establish_connection()

        for symbol in symbols:
            print(f"Fetching data for symbol: {symbol}")
            
            # Fetch data from both tables
            cm_data = fetch_data(conn, startdate, enddate, symbol, series, "bhavcopies_cm")
            udiff_data = fetch_data(conn, startdate, enddate, symbol, series, "bhavcopies_udiff")

            # Map columns for consistency
            cm_data_mapped = map_columns(cm_data, COLUMN_MAPPING, "bhavcopies_cm")

            # Merge data for this symbol
            combined_data = pd.merge(udiff_data, cm_data_mapped, how="outer")
            
            # Append to the cumulative DataFrame
            all_data = pd.concat([all_data, combined_data], ignore_index=True)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

    return all_data

def main():
    """Main function to run the script."""
    # Prompt the user for inputs
    startdate = '2023-01-02'  # input("Enter the start date (YYYY-MM-DD): ")
    enddate = '2023-01-02'  # input("Enter the end date (YYYY-MM-DD): ")
    symbols = ['TCS', 'TECHM', 'INFY']  # List of symbols to process
    series = 'EQ'  # input("Enter the series (e.g., EQ, BE, etc.): ").upper()

    # Fetch and display combined data for all symbols
    all_data = get_bhavcopy(startdate, enddate, symbols, series)
    if not all_data.empty:
        print("BhavCopy Data for All Symbols:")
        print(all_data.to_string(index=False))
    else:
        print("No data found for the specified criteria.")

if __name__ == "__main__":
    main()

