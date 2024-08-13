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
    list: A list of tuples containing the filename, row index, column name, and the value found.
    """
    df = pd.read_csv(file_path)
    result = []
    
    for row_index, row in df.iterrows():
        for col_name, value in row.items():
            if value == search_value:
                result.append((os.path.basename(file_path), row_index, col_name, value))
                
    return result

def find_value_in_csv_files(directory_path, search_value):
    """
    Search for a specific value in all CSV files within a given directory using multithreading.

    Args:
    directory_path (str): The path to the directory containing CSV files.
    search_value (str): The value to search for in the CSV files.

    Returns:
    list: A list of tuples containing the filename, row index, column name, and the value found.
    """
    occurrences = []
    
    # Get a list of all CSV files in the directory
    csv_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]
    
    # Use ThreadPoolExecutor to process files concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(find_value_in_csv, file, search_value): file for file in csv_files}
        
        for future in concurrent.futures.as_completed(future_to_file):
            result = future.result()
            if result:
                occurrences.extend(result)

    return occurrences

# Example usage:
directory_path = '/path/to/csv/files'  # Replace with your directory path
search_value = 'target_value'  # Replace with the value you are searching for

occurrences = find_value_in_csv_files(directory_path, search_value)

# Print the occurrences
if occurrences:
    for occurrence in occurrences:
        print(f"Found in file {occurrence[0]} - Row: {occurrence[1]}, Column: {occurrence[2]}, Value: {occurrence[3]}")
else:
    print(f"No occurrences of '{search_value}' found in any CSV file.")

