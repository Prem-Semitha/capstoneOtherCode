import os
import pandas as pd
import concurrent.futures

def read_target_values(file_path):
    """
    Read target values from a file.

    Args:
    file_path (str): The path to the file containing target values.

    Returns:
    list: A list of target values.
    """
    with open(file_path, 'r') as file:
        target_values = [line.strip() for line in file.readlines()]
    return target_values

def find_values_in_csv(file_path, search_values):
    """
    Search for specific values in a single CSV file and return rows containing those values.

    Args:
    file_path (str): The path to the CSV file.
    search_values (set): A set of values to search for in the CSV file.

    Returns:
    list: A list of tuples containing the filename and the rows where the values are found.
    """
    print(f"Searching in file: {os.path.basename(file_path)}")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {os.path.basename(file_path)}: {e}")
        return []
    
    matching_rows = []
    for row_index, row in df.iterrows():
        if any(value in search_values for value in row.values):
            matching_rows.append((os.path.basename(file_path), row_index, row))

        if row_index % 100 == 0:
            print(f"Processed {row_index} rows in {os.path.basename(file_path)}")

    return matching_rows

def find_values_in_csv_files(directory_path, search_values):
    """
    Search for specific values in all CSV files within a given directory using multithreading,
    and compile rows containing those values into a single DataFrame.

    Args:
    directory_path (str): The path to the directory containing CSV files.
    search_values (list): A list of values to search for in the CSV files.

    Returns:
    pd.DataFrame: A DataFrame containing all matching rows from all CSV files.
    """
    occurrences = []
    
    # Get a list of all CSV files in the directory
    csv_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]
    print(f"Found {len(csv_files)} CSV files in directory")

    # Use ThreadPoolExecutor to process files concurrently
    max_threads = 16  # Increase the number of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_file = {executor.submit(find_values_in_csv, file, set(search_values)): file for file in csv_files}
        
        for future in concurrent.futures.as_completed(future_to_file):
            try:
                result = future.result()
                if result:
                    occurrences.extend(result)
            except Exception as e:
                print(f"Error processing file: {e}")

    # Combine all matching rows into a single DataFrame
    all_matching_rows = [row for _, _, row in occurrences]
    if all_matching_rows:
        combined_df = pd.DataFrame(all_matching_rows)
    else:
        combined_df = pd.DataFrame()

    return combined_df

# Example usage:
directory_path = '/path/to/csv/files'  # Replace with your directory path
targets_file_path = '/path/to/containers.txt'  # Replace with the path to your targets file

# Read target values from the file
search_values = read_target_values(targets_file_path)

# Find all matching rows across the CSV files
matching_rows_df = find_values_in_csv_files(directory_path, search_values)

# Save the results to a new CSV file
output_csv_path = '/path/to/output/matching_rows.csv'  # Replace with your desired output file path
if not matching_rows_df.empty:
    matching_rows_df.to_csv(output_csv_path, index=False)
    print(f"Matching rows saved to {output_csv_path}")
else:
    print("No matching rows found.")

