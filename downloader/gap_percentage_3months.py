import os
import time
import yfinance as yf
import pandas as pd
import logging


# Custom logging setup function
def setup_logging(log_directory):
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # General logging
    logging.basicConfig(
        filename=os.path.join(log_directory, f"process_log_{time.strftime('%Y%m%d_%H%M%S')}.log"),
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

    # Error-specific logging
    error_logger = logging.getLogger("error_logger")
    error_handler = logging.FileHandler(os.path.join(log_directory, "error_log.log"))
    error_handler.setLevel(logging.ERROR)
    error_logger.addHandler(error_handler)

    return error_logger


# Function to fetch data from Yahoo Finance for individual tickers
def fetch_data(ticker, period="3mo", interval="1d", logger=None):
    try:
        logger.info(f"Fetching data for {ticker}...") if logger else print(f"Fetching data for {ticker}...")
        df = yf.download(ticker, period=period, interval=interval)
        df['ticker'] = ticker
        logger.info(f"Data fetching for {ticker} successful.") if logger else print(
            f"Data fetching for {ticker} successful.")
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}") if logger else print(
            f"Error fetching data for {ticker}: {e}")
        return None


# Function to calculate gap percentage
def calculate_gap_percentage(df, logger=None):
    try:
        logger.info("Calculating gap percentage...") if logger else print("Calculating gap percentage...")
        prev_close = df['Close'].shift(1)
        open = df['Open']
        gap_percentage = ((open - prev_close) / prev_close) * 100
        gap_target = prev_close + 0.2 * (open - prev_close)
        df['pre_close_price'] = df['Close'].shift(1)
        df['gap_percent'] = gap_percentage
        df['gap_target'] = gap_target
        logger.info("Gap percentage calculation successful.") if logger else print(
            "Gap percentage calculation successful.")
        return df
    except Exception as e:
        logger.error(f"Error calculating gap percentage: {e}") if logger else print(
            f"Error calculating gap percentage: {e}")
        return df


# Function to calculate 30-day average volume
def calculate_avg_volume(df, logger=None):
    try:
        logger.info("Calculating 30-day average volume...") if logger else print("Calculating 30-day average volume...")
        df['avg_volume_30'] = df['Volume'].rolling(window=30, min_periods=1).mean()  # 30-day average volume
        logger.info("30-day average volume calculation successful.") if logger else print(
            "30-day average volume calculation successful.")
        return df
    except Exception as e:
        logger.error(f"Error calculating 30-day average volume: {e}") if logger else print(
            f"Error calculating 30-day average volume: {e}")
        return df


# Function to calculate relative volume
def calculate_relative_volume(df, logger=None):
    try:
        logger.info("Calculating relative volume...") if logger else print("Calculating relative volume...")
        volume = df['Volume']
        avg_volume = df['avg_volume_30']
        relative_volume = (volume / avg_volume)
        df['relative_volume'] = relative_volume
        logger.info("Relative volume calculation successful.") if logger else print(
            "Relative volume calculation successful.")
        return df
    except Exception as e:
        logger.error(f"Error calculating relative volume: {e}") if logger else print(
            f"Error calculating relative volume: {e}")
        return df


def calculate_relative_volume2(df, logger=None):
    try:
        logger.info("Calculating relative volume...") if logger else print("Calculating relative volume...")

        # Check if the required columns exist in the DataFrame
        if 'Volume' not in df.columns or 'avg_volume_30' not in df.columns:
            raise ValueError("'Volume' or 'avg_volume_30' column missing in DataFrame")

        # Ensure no missing values
        if df['avg_volume_30'].isnull:
            null_volume_rows = df[df['Volume'].isnull()]
            print("Rows with NaN in 'Volume' column:")
            print(null_volume_rows)

            # Find rows where 'avg_volume_30' is NaN
            null_avg_volume_rows = df[df['avg_volume_30'].isnull()]
            print("Rows with NaN in 'avg_volume_30' column:")
            print(null_avg_volume_rows)
            raise ValueError("Missing values in 'Volume' or 'avg_volume_30'.")

        # Calculate relative volume
        relative_volume = df['Volume'] / df['avg_volume_30']
        df['relative_volume'] = relative_volume

        logger.info("Relative volume calculation successful.") if logger else print(
            "Relative volume calculation successful.")
        return df

    except Exception as e:
        logger.error(f"Error calculating relative volume: {e}") if logger else print(
            f"Error calculating relative volume: {e}")
        return df


# Function to save the results to a single CSV file for all tickers
def save_all_to_csv(all_data, ticker,output_directory, logger=None):
    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        file_path = os.path.join(output_directory, f"{ticker}.csv")
        logger.info(f"Saving all results to {file_path}...") if logger else print(
            f"Saving all results to {file_path}...")
        all_data.to_csv(file_path, index=False)
        logger.info(f"All results successfully saved to {file_path}.") if logger else print(
            f"All results successfully saved to {file_path}.")
    except Exception as e:
        logger.error(f"Error saving all results to CSV: {e}") if logger else print(
            f"Error saving all results to CSV: {e}")


# Main function to orchestrate the analysis for each ticker separately and then save to one CSV
def analyze_stocks(tickers, log_directory,data_directory):
    # Set up logging
    logger = setup_logging(log_directory)

    try:
        logger.info("Starting stock analysis for tickers...")

        # Initialize an empty list to store all the processed data
        all_results = []

        # Iterate over each ticker and perform analysis
        for ticker in tickers:
            logger.info(f"Starting analysis for {ticker}...")
            print(f"Starting analysis for {ticker}...")

            # Step 1: Fetch Data
            df = fetch_data(ticker, logger=logger)
            if df is None:
                logger.error(f"Data fetching failed for {ticker}, analysis aborted.")
                continue

            # Step 2: Calculate gap percentage
            df = calculate_gap_percentage(df, logger=logger)

            # Step 3: Calculate 30-day average volume
            df = calculate_avg_volume(df, logger=logger)

            # Step 4: Calculate relative volume
            # df = calculate_relative_volume2(df, logger=logger)

            # Step 5: Reset index and sort the DataFrame for clarity
            df.reset_index(inplace=True)
            df.sort_values(by=['Date'], inplace=True)

            logger.info(f"Analysis for {ticker} completed successfully.")

            # Append the processed DataFrame for each ticker to the list
            save_all_to_csv(df[['ticker', 'Date', 'gap_percent','gap_target','pre_close_price','Open','Close', 'avg_volume_30']], ticker, data_directory, logger=logger)

        # Step 6: Save the combined DataFrame to a single CSV file

        logger.info("All stock analyses completed successfully and saved to one CSV.")

    except Exception as e:
        logger.error(f"Error during stock analysis: {e}")

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
# Example usage:
if __name__ == "__main__":
    input_file = "../resources/output.csv"
    tickers = extract_and_process_tickers(input_file) # Example Indian tickers
    log_directory = "logs"
    data_directory = "data"  # Log directory path
    analyze_stocks(tickers, log_directory, data_directory)
