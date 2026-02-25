import json
import csv
import os

def convert_json_to_csv(json_file_path, csv_file_path):
    """
    Converts a JSON file containing a list of dictionaries to a CSV file.
    """
    if not os.path.exists(json_file_path):
        print(f"Error: File {json_file_path} not found.")
        return

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)

        if not data or not isinstance(data, list):
            print("Error: JSON data is empty or not a list.")
            return

        # Use the keys from the first dictionary as fieldnames
        # We use a set to collect all possible keys in case records are inconsistent
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())
        
        # Sort keys to ensure consistent column ordering, or define a specific order
        # For this specific dataset, we might want a specific order if we know it.
        # But for a general script, we'll just sort them or use the first record's keys.
        fieldnames = list(data[0].keys())

        with open(csv_file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"Successfully converted {json_file_path} to {csv_file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    input_json = "db/exports/top_10_coal_companies_2024 copy.json"
    output_csv = "db/exports/top_10_coal_companies_2024.csv"
    convert_json_to_csv(input_json, output_csv)
