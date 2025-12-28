from confluent_kafka import Producer
import json, time

p = Producer({"bootstrap.servers": "localhost:29092"})

i = 0
while True:
    d = {"This is": i}
    p.produce("test", json.dumps(d).encode())
    p.flush()
    print("Sent:", d)
    i += 1
    time.sleep(0.005)