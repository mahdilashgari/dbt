##########################################################################################################################################
#                                                                                                                                        #
# generate_models_snapshot_files.py                                                                                        	             #
# 																																		 #
# Prerequisites:                                                                                                                         #
#                data.yaml file that is in the same location as this script                                                              #
#                data.yaml file content is simply a copy paste of your models/raw/[your_folder]/_[schema_name]__models.yaml              #
#                                                                                                                                        #
# Description:                                                                                                                           #
#                Generates raw table DBT configuration files, in this case files are generated for cases with no historization required  #
#                 Feel free to adjust the files as per your use case                                                                     #
#                                                                                                                                        #
# syntax:                                                                                                                                #
#                   python generate_models_snapshot_files.py                                                                             #
#                                                                                                                                        #
##########################################################################################################################################

import os
import yaml


# Function to remove previous files with a specific extension from a folder
def remove_previous_files(folder, extension):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) and filename.endswith(extension):
            os.remove(file_path)
            print(f"Removed previous file: {filename}")


# Define the function to generate SQL files
def generate_sql_files(array, prefix, folder):
    regular_folder = "dbt/models/_raw/" + folder
    snapshot_folder = "dbt/snapshots/_raw/" + folder

    # Create folders if they don't exist
    os.makedirs(regular_folder, exist_ok=True)
    os.makedirs(snapshot_folder, exist_ok=True)

    # Remove previous .sql files from the folders
    remove_previous_files(regular_folder, ".sql")
    remove_previous_files(snapshot_folder, ".sql")

    for string in array:
        filename = f"{prefix.lower()}__{string.lower()}.sql"
        snapshot_filename = f"{prefix.lower()}__{string.lower()}_snapshot.sql"
        if string.endswith("_hst"):
            content = "{{- generate_raw_model() -}}"
        else:
            content = "{{- generate_raw_current_model() -}}"

        snapshot_content = (
            "{%- snapshot " + prefix.lower() + "_" + string.lower() + "_snapshot -%}\n"
            "{%- set key_cols = ['ID'] -%}\n"
            "{{-\n"
            "    config(\n"
            "    unique_key='dbt_unique_sk',\n"
            "    target_schema=model.fqn[-3],\n"
            "    strategy='check',\n"
            "    invalidate_hard_deletes=True,\n"
            "    check_cols=['dbt_hashdiff'],\n"
            "    )\n"
            "-}}\n"
            "{{- generate_raw_snapshot(key_cols) -}}\n"
            "{%- endsnapshot -%}\n"
        )

        with open(os.path.join(regular_folder, filename), "w") as file:
            file.write(content)

        if not string.endswith("_hst"):
            if snapshot_content:
                with open(
                    os.path.join(snapshot_folder, snapshot_filename), "w"
                ) as file:
                    file.write(snapshot_content)

        print(f"Generated {filename} in '{regular_folder}' folder")
        if snapshot_content:
            print(f"Generated {snapshot_filename} in '{snapshot_folder}' folder")


# Get the path of the YAML file in the same folder as the script
script_dir = os.path.dirname(os.path.abspath(__file__))
yaml_file = os.path.join(script_dir, "data.yaml")

# Load the YAML data from the file
with open(yaml_file, "r") as file:
    data = file.read()

# Parse the YAML data
parsed_data = yaml.safe_load(data)

# Extract the names from the parsed data
names = [model["name"] for model in parsed_data["models"]]

# Example usage
file_prefix = "datafeeds"  # usually database name
folder = "datafeeds"  # usually database name
generate_sql_files(names, file_prefix, folder)
