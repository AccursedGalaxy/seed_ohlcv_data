import os
import sys

# define root directory as one level up from this file's directory
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

data_folder_path = os.path.join(root_dir, 'data')

# Check if the folder exists
if os.path.exists(data_folder_path) and os.path.isdir(data_folder_path):
    # Iterate over all files in the data folder
    for filename in os.listdir(data_folder_path):
        # Check if the file is a CSV file
        if filename.endswith('.csv'):
            # Construct the full path to the CSV file
            file_path = os.path.join(data_folder_path, filename)

            # Open the file in write mode to clear its contents
            with open(file_path, 'w') as file:
                pass  # Do nothing, just open the file in write mode to clear it

            print(f"Cleared the contents of {filename}")
else:
    print(
        f"The folder {data_folder_path} does not exist or is not a directory")
