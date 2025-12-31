import os
import re
import csv
from io import StringIO

def extract_refs_from_sql(sql_content):
    """Extract ref() calls from SQL content."""
    return re.findall(r"ref\(['\"](.*?)['\"]\)", sql_content)

# Specify the folder path
folder_path = './models/onlyfy'  # Adjust based on your dbt project structure

references = {}

# Walk through the SQL files in the specified folder
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith(".sql"):
            with open(os.path.join(root, file), 'r') as f:
                content = f.read()
                refs = extract_refs_from_sql(content)
                
                # Exclude refs that start with 'onlyfy_'
                refs = [ref for ref in refs if not ref.startswith('onlyfy_')]
                
                if refs:
                    references[file] = refs

# Write to CSV in memory using StringIO
output = StringIO()
csv_writer = csv.writer(output)
csv_writer.writerow(['Model', 'Reference'])

for model, refs in references.items():
    for ref in refs:
        csv_writer.writerow([model, ref])

# Reset buffer position to the start
output.seek(0)

# Print the CSV content
print(output.read())
