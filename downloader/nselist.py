import os

import nselib
from nselib import capital_market

# Define the directory and file path
directory = 'resources'  # Folder where you want to save the file
file_name = 'output.csv'
file_path = os.path.join(directory, file_name)

# Check if the directory exists, and create it if it doesn't
if not os.path.exists(directory):
    os.makedirs(directory)
    print(f"Directory '{directory}' created.")

# Save the DataFrame as a CSV file
df = capital_market.equity_list()
df.to_csv(file_path, index=False)
print(f"DataFrame saved to {file_path}")