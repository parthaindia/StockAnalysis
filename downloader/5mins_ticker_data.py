import os
import time
import logging
import yfinance as yf
import pandas as pd


# Configure logging
def setup_logging(log_directory):
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(
        filename=os.path.join(log_directory, f"5min_ticker_{time.strftime('%Y%m%d_%H%M%S')}.log"),
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

    error_logger = logging.getLogger("error_logger")
    error_handler = logging.FileHandler(os.path.join(log_directory, "error_log.log"))
    error_handler.setLevel(logging.ERROR)
    error_logger.addHandler(error_handler)

    return error_logger


# Function to create a directory
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Directory '{directory}' created.")


# Function to fetch 5-minute interval data for a ticker
def fetch_5min_data(ticker, start_date, end_date):
    try:
        stock_data = yf.download(ticker, start=start_date, end=end_date, interval="5m")
        stock_data['Ticker'] = ticker  # Add a Ticker column
        return stock_data
    except Exception as e:
        raise Exception(f"Error fetching 5-min data for {ticker}: {e}")


# Function to calculate additional metrics for the data
def calculate_metrics(data):
    try:
        data['Gap Percent'] = ((data['Open'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
        data['Midday Value'] = data.between_time('12:00', '12:15')['High'].max() if not data.empty else None
        data['Max High to Midday'] = data.between_time('09:30', '12:00')['High'].max()
        return data
    except Exception as e:
        raise Exception(f"Error calculating metrics: {e}")


# Function to fetch additional info for a ticker
def fetch_ticker_info(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            "Market Cap": info.get("marketCap", None),
            "Sector": info.get("sector", None),
            "Company Short Name": info.get("shortName", None)
        }
    except Exception as e:
        raise Exception(f"Error fetching ticker info for {ticker}: {e}")


from datetime import datetime, timedelta


def convert_utc_to_ist(utc_time_str):
    # Parse the given UTC time string
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")

    # IST is UTC + 5 hours 30 minutes
    ist_offset = timedelta(hours=5, minutes=30)

    # Convert UTC to IST
    ist_time = utc_time + ist_offset

    return ist_time.strftime("%Y-%m-%d %H:%M:%S")


# Main function to generate and save the data for each ticker
def generate_5min_data_per_ticker(tickers, start_date, end_date, output_directory, log_directory):
    create_directory(output_directory)

    # Setup logging
    error_logger = setup_logging(log_directory)

    for ticker in tickers:
        try:
            logging.info(f"Processing {ticker}...")
            print(f"Processing {ticker}...")
            data = fetch_5min_data(ticker, start_date, end_date)

            if data.empty:
                error_logger.error(f"No data found for {ticker}.")
                continue



            # Fetch ticker info
            info = fetch_ticker_info(ticker)
            data['Market Cap'] = info['Market Cap']
            data['Sector'] = info['Sector']
            data['Company Short Name'] = info['Company Short Name']

            # Add the Date column
            data['Date'] = data.index + pd.Timedelta(hours=5, minutes=30)

            # Select required columns
            data = data[[
                'Ticker', 'Date', 'Open', 'Close', 'Volume', 'Market Cap', 'Sector',
                'Company Short Name'
            ]]

            # Save to a CSV file named after the ticker
            file_path = os.path.join(output_directory, f"{ticker}.csv")
            data.to_csv(file_path, index=False)
            logging.info(f"Data for {ticker} saved to {file_path}")

        except Exception as e:
            error_logger.error(f"Error processing {ticker}: {e}")


import pandas as pd


def extract_and_process_tickers(input_file):
    try:
        # Read the CSV
        df = pd.read_csv(input_file)
        if 'SYMBOL' not in df.columns:
            raise ValueError("The input file must contain a 'SYMBOL' column.")

        # Append ".NS" to each SYMBOL
        df['SYMBOL'] = df['SYMBOL'].astype(str) + ".NS"

        # Extract the tickers
        tickers = df['SYMBOL'].tolist()

        # Log tickers
        print(f"Extracted {len(tickers)} tickers: {tickers}")
        return tickers

    except FileNotFoundError:
        print(f"Error: File not found - {input_file}")
    except Exception as e:
        print(f"Error processing the file: {e}")

    return []


if __name__ == "__main__":
    start_date = "2024-12-27"
    end_date = "2024-12-28"
    output_directory = "nse_data_per_ticker"
    log_directory = "logs"
    input_file = "../resources/output.csv"
    generate_5min_data_per_ticker(extract_and_process_tickers(input_file), start_date, end_date, output_directory, log_directory)
    # generate_5min_data_per_ticker(tickers, start_date, end_date, output_directory,
    #                               log_directory)
