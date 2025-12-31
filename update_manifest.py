#!/usr/bin/env python3
"""
Script to download a manifest file from dbt Cloud for defer functionality of dbt.

!!! PLEASE SETUP .env FILE WITH YOUR TOKEN AS PER THE INSTRUCTIONS IN THE docs/dbt-local-install.md FILE. !!!

Auto-installs required packages (requests, python-dotenv) if missing.

Steps:
1. Read DBT_PROD_STATE_JOB_ID from .env (the job ID).
2. Retrieve the last *successful* run ID (status=10) for that job.
3. Download the manifest.json for that run.
"""

import importlib
import subprocess
import sys
import json
import os

def ensure_installed(package_name, import_name=None):
    """
    Installs the given package if it's not already installed.
    
    :param package_name: Name of the package to install (e.g. 'requests').
    :param import_name: Name of the module to import (e.g. 'requests'), if different
                        from the package name (e.g. 'python-dotenv' -> 'dotenv').
    """
    if import_name is None:
        import_name = package_name

    try:
        importlib.import_module(import_name)
    except ImportError:
        print(f"Installing package '{package_name}'...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

# Ensure required packages are installed
ensure_installed("requests")
ensure_installed("python-dotenv", "dotenv")

import requests
from dotenv import load_dotenv

def get_last_successful_run_id_for_job(job_id, account_id, host, token):
    """
    Retrieves the most recent *successful* run ID (status=10) for a given job ID.

    :param job_id: dbt Cloud job ID.
    :param account_id: The dbt Cloud account ID.
    :param host: The dbt Cloud host (e.g., 'cloud.getdbt.com' or 'emea.dbt.com').
    :param token: The dbt Cloud service token.
    :return: The latest successful run ID (int).
    """
    # status=10 means a "Success" status in dbt Cloud.
    url = (
        f"https://{host}/api/v2/accounts/{account_id}/runs/"
        f"?account_id={account_id}&job_definition_id={job_id}"
        f"&limit=1&order_by=-id&status=10"
    )
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise for any 4xx/5xx
    data = response.json()
    
    runs = data.get("data", [])
    if not runs:
        raise ValueError(f"No successful runs found for job ID {job_id}.")
    
    # The first item in the 'data' array is the latest by descending ID.
    latest_run = runs[0]
    return latest_run["id"]

def download_manifest(run_id, account_id, host, token, output_path="manifest.json"):
    """
    Downloads the dbt Cloud manifest.json file for a given run ID and saves it locally.

    :param run_id: The dbt Cloud run ID.
    :param account_id: The dbt Cloud account ID.
    :param host: The dbt Cloud host (e.g., 'cloud.getdbt.com' or 'emea.dbt.com').
    :param token: The dbt Cloud service token.
    :param output_path: Path where the manifest will be saved.
    """
    print(f"Downloading manifest for run ID: {run_id}")
    
    url = f"https://{host}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/manifest.json"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Will raise an exception for 4xx/5xx responses
    
    manifest_data = response.json()
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(manifest_data, file, indent=2)
    
    print(f"Manifest downloaded successfully to '{output_path}'.")

def main():
    """
    Main function to handle loading environment variables, retrieving last successful run ID,
    and downloading the manifest.
    """
    load_dotenv(override=True)  # Load environment variables from .env file

    # We hardcode the some values here to be able to update them for users.
    dbt_cloud_host = 'emea.dbt.com'
    dbt_cloud_account_id = '74'
    job_id = '18987'

    # Retrieve environment variables from user's .env file
    dbt_cloud_api_token = os.getenv('DBT_CLOUD_API_TOKEN')

    # Validate that we have a job ID
    if not job_id:
        print("Error: DBT_PROD_STATE_JOB_ID must be set in the .env file.")
        sys.exit(1)

    try:
        # Step 1: Get the latest *successful* run ID for the given job
        run_id = get_last_successful_run_id_for_job(
            job_id=job_id,
            account_id=dbt_cloud_account_id,
            host=dbt_cloud_host,
            token=dbt_cloud_api_token
        )

        # Step 2: Download the manifest for that run ID
        download_manifest(
            run_id=run_id,
            account_id=dbt_cloud_account_id,
            host=dbt_cloud_host,
            token=dbt_cloud_api_token,
            output_path="manifest.json"
        )

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
