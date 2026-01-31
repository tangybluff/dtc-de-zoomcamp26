# Follow along Module 01 Workshop
# Import the sys module to access command-line arguments
import sys
# Import the pandas library for data manipulation
import pandas as pd

# Print all command-line arguments passed to the script
print("arguments", sys.argv)

# Convert the first argument after the script name to an integer (expected to be the month)
month = int(sys.argv[1])

# Create a simple DataFrame with two columns A and B
df = pd.DataFrame({"day": [1, 2], "number_passengers": [3, 4]})
# Add the month column to the DataFrame using the value from the command-line argument
df["month"] = month
# Print the first few rows of the DataFrame
print(df.head())

# Print which month the pipeline is running for
print(f"Running pipeline for month {month}")

# Save the DataFrame to a Parquet file named according to the month
df.to_parquet(f"output_month_{month}.parquet")