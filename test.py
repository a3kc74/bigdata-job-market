import pandas as pd
import os

# List of files to process
files = ['T2-CN.csv', 'T2-T6.csv', 'T2-T7.csv', 'Flexible.csv', 'Other.csv']

# List to hold the DataFrame from each file
dataframes_list = []

# Loop through each file
for file in files:
    if os.path.exists(file):
        try:
            # Read the CSV file and add it to our list
            df = pd.read_csv(file)
            dataframes_list.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
    else:
        print(f"Warning: File not found - {file}")

# Check if any data was read
if dataframes_list:
    # 1. Concatenate all DataFrames into a single one
    full_df = pd.concat(dataframes_list, ignore_index=True)
    print(f"Total records from all files: {len(full_df)}")

    # 2. Apply deduplication on the entire DataFrame (all columns)
    deduplicated_df = full_df.drop_duplicates()
    print(f"Records after deduplication: {len(deduplicated_df)}")

    # 3. Filter to get only the 'schedule' column
    if 'schedule' in deduplicated_df.columns:
        final_schedules = deduplicated_df[['schedule']].dropna()

        # As an extra step, we can drop duplicates from the final schedule list as well
        # to ensure every schedule string is unique.
        final_schedules = final_schedules.drop_duplicates().reset_index(drop=True)

        # 4. Save the result to a new CSV file
        output_file = 'schedule.csv'
        final_schedules.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"Successfully created {output_file} with {len(final_schedules)} unique schedule records.")
    else:
        print("Error: 'schedule' column not found after merging and deduplicating data.")
else:
    print("No data was read from the input files. Output file not created.")