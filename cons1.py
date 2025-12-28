from confluent_kafka import Consumer
import boto3
import json
import time

print("\n Starting Consumer...\n")

# Kafka Consumer
c = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "g1"
})

print("\n Consumer Configured")
c.subscribe(["test"])
print("\n Subscribed to topic: test")

# S3 Client
s3 = boto3.client(
    's3',
    aws_access_key_id="AKsdfghjkjhgfghj" ,
    aws_secret_access_key="mzxdfghjklkjhgfd"
)

print("\n S3 Client Ready")

bucket = "lokeshprojectbigdata01"
buffer = []
size = 0
MAX = 10 * 1024     # 10 KB
count = 1

print("\n Waiting for Kafka messages...\n")

while True:
    msg = c.poll(1)

    if msg is None:
        continue

    if msg.error():
        print("\n Error:", msg.error())
        continue

    # Decode raw kafka JSON
    raw_json = msg.value().decode()  # {"This is": 5}

    # Convert to Python dict
    json_obj = json.loads(raw_json)

    # CLEAN KEYS → Replace spaces with _
    clean_obj = {
        key.replace(" ", "_"): value
        for key, value in json_obj.items()
    }

    # Convert dict to JSON line
    json_line = json.dumps(clean_obj)

    # Add to buffer
    buffer.append(json_line)
    size += len(json_line) + 1   # newline

    print(f"Received → {json_line}")

    # If file reaches 10 KB, create new file
    if size >= MAX:
        fname = f"data_{count}.json"

        # Write NDJSON format
        with open(fname, "w") as f:
            for line in buffer:
                f.write(line + "\n")

        print(f"\n File created: {fname}")

        # Upload to S3
        s3.upload_file(fname, bucket, fname)
        print(f" Uploaded to S3: {bucket}/{fname}")

        # Reset buffer
        buffer = []
        size = 0
        count += 1   

print("i am consumer")