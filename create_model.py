#!/usr/bin/env python3

import os
import requests
import sys
import json

# 1. Load credentials from environment variables
client_id = os.getenv("SIGMA_CLIENT_ID")
client_secret = os.getenv("SIGMA_CLIENT_SECRET")
api_base_url = os.getenv("SIGMA_API_BASE_URL", "https://api.sigmacomputing.com")

if not client_id or not client_secret:
    print("Error: Missing SIGMA_CLIENT_ID or SIGMA_CLIENT_SECRET environment variables.")
    sys.exit(1)

# 2. Authenticate and get the access token
print("Authenticating with Sigma API...")
auth_url = f"{api_base_url}/v2/auth/token"

auth_payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret
}
auth_headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

try:
    auth_response = requests.post(auth_url, data=auth_payload, headers=auth_headers)
    auth_response.raise_for_status()
    access_token = auth_response.json().get("access_token")
    print("Authentication successful!")
except requests.exceptions.RequestException as e:
    print(f"Authentication failed: {e}")
    if auth_response is not None:
        print(auth_response.text)
    sys.exit(1)

# 3. Read the local JSON file
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'sigma_model.json')

print(f"\nReading data model spec from: {file_path}")
try:
    with open(file_path, 'r') as file:
        # Load the exact JSON you provided
        data_model_payload = json.load(file)
except FileNotFoundError:
    print(f"Error: The file {file_path} was not found.")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error: The file is not valid JSON. {e}")
    sys.exit(1)

# 4. Create the Data Model
print("Creating Data Model...")
create_dm_url = f"{api_base_url}/v2/dataModels/spec"

api_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

try:
    # Pass your JSON directly as the payload, no wrappers!
    response = requests.post(create_dm_url, headers=api_headers, json=data_model_payload)
    response.raise_for_status()
    
    print("Data Model created successfully! Response:")
    print(json.dumps(response.json(), indent=2))
    
except requests.exceptions.RequestException as e:
    print(f"Failed to create Data Model: {e}")
    if response is not None:
        print(response.text)
    sys.exit(1)