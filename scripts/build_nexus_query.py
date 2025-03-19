from bs4 import BeautifulSoup
import os
import pyperclip
import pandas as pd
from pathlib import Path
# Path to your Excel file

ROOT_DIR = Path(__file__).resolve().parent.parent
excel_file = ROOT_DIR / 'files' / 'query.xlsx'
output_file = ROOT_DIR / 'output' /  'queries_per_faculty.txt'

# Read the Excel file
# Adjust sheet_name if needed, or remove to use the first sheet by default.
df = pd.read_excel(excel_file, sheet_name=0)

# Optional: rename columns for clarity if needed, e.g.,
# df.rename(columns={"Organisational unit-0": "Organisational unit",
#                    "Name variant-1": "Name variant"}, inplace=True)

# Group the data by the "Organisational unit" column
groups = df.groupby("Organisations > Organisational unit-0")

# Define the threshold per query
limit = 1300
# Open the output file
with open(output_file, "w", encoding="utf-8") as f_out:
    # Iterate over each group (i.e., each organisational unit)
    for org_unit, group_df in groups:
        # Collect all the name variants for this organisational unit
        name_variants = group_df["Name variant > Known as name-1"].dropna().unique().tolist()

        # Chunk the name variants
        chunks = [name_variants[i:i + limit] for i in range(0, len(name_variants), limit)]

        # For each chunk, create a query
        for chunk_idx, chunk in enumerate(chunks, start=1):
            # Construct the query
            query = '("Utrecht University" OR "Universiteit Utrecht") AND (' \
                    + " OR ".join(f'"{name}"' for name in chunk) + ')'
            print(f"Number of chunks for {org_unit}:", len(chunks))
            # Write the faculty and the query
            f_out.write(f"faculty: {org_unit}\n")
            f_out.write(query + "\n\n")


print(f"Done! Queries have been written to {output_file}")
