#!/bin/bash

#### Usage:
####        macros/generate_sources_yml_for_database.sh raw
####        macros/generate_sources_yml_for_database.sh analytics central

# Argument checking
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <database_name> [<schema_name>]"
    exit 1
fi

# Get the database name from command line argument
database_name=$1

# Check if a schema name was provided
if [ "$#" -gt 1 ]; then
    schema_name=$2
    args="{database: ${database_name}, schema: ${schema_name}}"
    # Construct the YAML file name with schema
    yaml_file="models/central/_${database_name}_${schema_name}__sources.yml"
else
    args="{database: ${database_name}}"
    # Construct the YAML file name without schema
    yaml_file="models/central/_${database_name}__sources.yml"
fi

# Run the dbt command
dbt --quiet run-operation generate_source_yml_for_database --args "${args}" >> $yaml_file
