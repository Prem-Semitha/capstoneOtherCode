import os
import pandas as pd

def find_value_in_csv_files(directory_path, search_value):
    """
    Search for a specific value in all CSV files within a given directory.

    Args:
    directory_path (str): The path to the directory containing CSV files.
    search_value (str): The value to search for in the CSV files.

    Returns:
    dict: A dictionary with file names as keys and lists of (row, column) tuples where the value is found.
    """
    occurrences = {}

    # Iterate through all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)

            # Find occurrences of the search value
            result = []
            for row_index, row in df.iterrows():
                for col_index, value in row.items():
                    if value == search_value:
                        result.append((row_index, col_index))

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

