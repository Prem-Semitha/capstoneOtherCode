import os
import pandas as pd
import concurrent.futures

def find_value_in_csv(file_path, search_value):
    """
    Search for a specific value in a single CSV file.

    Args:
    file_path (str): The path to the CSV file.
    search_value (str): The value to search for in the CSV file.

    Returns:
    tuple: A tuple containing the filename and a list of (row, column) tuples where the value is found.
    """
    df = pd.read_csv(file_path)
    result = []
    
    for row_index, row in df.iterrows():
        for col_index, value in row.items():
            if value == search_value:
                result.append((row_index, col_index))
                
    return os.path.basename(file_path), result

def find_value_in_csv_files(directory_path, search_value):
    """
    Search for a specific value in all CSV files within a given directory using multithreading.

    Args:
    directory_path (str): The path to the directory containing CSV files.
    search_value (str): The value to search for in the CSV files.

    Returns:
    dict: A dictionary with file names as keys and lists of (row, column) tuples where the value is found.
    """
    occurrences = {}
    
    # Get a list of all CSV files in the directory
    csv_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]
    
    # Use ThreadPoolExecutor to process files concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(find_value_in_csv, file, search_value): file for file in csv_files}
        
        for future in concurrent.futures.as_completed(future_to_file):
            filename, result = future.result()
            if result:
                occurrences[filename] = result

    return occurrences

# Example usage:
directory_path = '/path/to/csv/files'  # Replace with your directory path
search_value = 'target_value'  # Replace with the value you are searching for

occurrences = find_value_in_csv_files(directory_path, search_value)

# Print the occurrences
if occurrences:
    for filename, positions in occurrences.items():
        print(f"Occurrences in file {filename}:")
        for pos in positions:
            print(f"Row: {pos[0]}, Column: {pos[1]}")
else:
    print(f"No occurrences of '{search_value}' found in any CSV file.")

