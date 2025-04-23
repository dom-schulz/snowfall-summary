import pandas as pd

# Load both CSVs
resorts_us = pd.read_csv("resorts_us.csv")
resort_coords = pd.read_csv("resort_coords.csv")

# Merge on 'Resort'
merged_df = pd.merge(resort_coords, resorts_us[['Resort', 'ID']], on='Resort', how='left')

# Reorder columns to make 'ID' the first column
cols = ['ID'] + [col for col in merged_df.columns if col != 'ID']
merged_df = merged_df[cols]

# Save to CSV
merged_df.to_csv("final_resorts_us.csv", index=False)

# Optional: Preview
print(merged_df.head())