import os
import pandas as pd
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

# Define the categories based on land holding (in hectares)
categories = {
    "Marginal": (0, 1),
    "Small": (1, 2),
    "Semi-medium": (2, 4),
    "Medium": (4, 10),
    "Large": (10, float('inf'))
}

# Function to categorize total area
def categorize_area(total_area):
    for category, (min_area, max_area) in categories.items():
        if min_area <= total_area < max_area:
            return category
    return None

# Function to process each village file and return the processed data
def process_village_file(file_path, taluka_name_marathi, taluka_name_english):
    village_code, village_name_marathi = os.path.splitext(os.path.basename(file_path))[0].split(' ', 1)
    village_name_english = transliterate(village_name_marathi, sanscript.DEVANAGARI, sanscript.ITRANS).title().replace("-", "")
    
    # Remove the last two zeros from the village code
    village_code = village_code[:-2]
    
    df = pd.read_excel(file_path)
    
    area_sums = {category: 0 for category in categories}
    area_counts = {category: 0 for category in categories}
    
    for index, row in df.iterrows():
        total_area = float(row['Total Area'])
        category = categorize_area(total_area)
        if category:
            area_sums[category] += total_area
            area_counts[category] += 1
    
    return {
        "village_code": village_code,
        "village_name_marathi": village_name_marathi,
        "village_name_english": village_name_english,
        "taluka_name_marathi": taluka_name_marathi,
        "taluka_name_english": taluka_name_english,
        **area_sums,
        **{f"{category}_count": count for category, count in area_counts.items()}
    }

# Function to process all village files in a taluka and return the processed data
def process_taluka_files(taluka_path):
    taluka_name_full = os.path.basename(taluka_path)
    taluka_name_number, taluka_name_marathi = taluka_name_full.split(' ', 1)
    taluka_name_english = transliterate(taluka_name_marathi, sanscript.DEVANAGARI, sanscript.ITRANS).title().replace("-", "")
    
    village_files = [os.path.join(taluka_path, f) for f in os.listdir(taluka_path) if f.endswith('.xlsx')]
    
    processed_data = []
    
    for i, village_file in enumerate(village_files, start=1):
        print(f"Processing village {i}/{len(village_files)} in taluka '{taluka_name_marathi}'...")
        processed_data.append(process_village_file(village_file, taluka_name_marathi, taluka_name_english))
    
    return processed_data

if __name__ == "__main__":
    # Define the root directory where the taluka folders are stored
    root_directory = "./07 अमरावती"  # Update this path
    output_csv_file = "./district_data.csv"  # Update this path
    output_xlsx_file = "./district_data.xlsx"  # Update this path

    taluka_folders = [os.path.join(root_directory, folder) for folder in os.listdir(root_directory) if os.path.isdir(os.path.join(root_directory, folder))]
    
    all_data = []

    for i, taluka_folder in enumerate(taluka_folders, start=1):
        print(f"Processing taluka {i}/{len(taluka_folders)}...")
        all_data.extend(process_taluka_files(taluka_folder))
    
    keys = [
        "village_code", "village_name_marathi", "village_name_english", 
        "taluka_name_marathi", "taluka_name_english"
    ] + list(categories.keys()) + [f"{category}_count" for category in categories]

    df = pd.DataFrame(all_data, columns=keys)
    df.to_csv(output_csv_file, index=False, encoding='utf-8-sig')
    df.to_excel(output_xlsx_file, index=False)
    print(f"Data processing complete. Output saved to '{output_csv_file}' and '{output_xlsx_file}'.")
