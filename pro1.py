import requests
import boto3
import json
from datetime import datetime

# -------- 100% WORKING DUMMY API ----------
url = "https://dummyjson.com/users?limit=3"

# -------- CALL THE API ---------
response = requests.get(url)

print("Status Code:", response.status_code)
print("Raw Response:", response.text)   # Debug

# Valid JSON
raw_data = response.json()

# Extract only 4 columns
data = []
for user in raw_data["users"]:
    data.append({
        "id": user["id"],
        "name": user["firstName"],
        "age": user["age"],
        "city": user.get("address", {}).get("city", "Unknown")
    })

# -------- CREATE FILE NAME WITH TIME ----
file_name = f"dummy_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

# -------- SAVE LOCAL FILE ---------------
with open(file_name, "w") as f:
    json.dump(data, f, indent=4)

# -------- UPLOAD TO S3 -----------------
s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIAlkjhgfcvghjk",
    aws_secret_access_key="mYN5NLWMgEdfghjkjhgfghjk"
)

bucket = "lokeshprojectbigdata01"
folder = "api_to_s3"

s3.upload_file(file_name, bucket, f"{folder}/{file_name}")

print("Dummy data uploaded successfully!")