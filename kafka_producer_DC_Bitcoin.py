# from kafka import KafkaProducer
# import requests
# import json
# import time

# # Define the API endpoint URLs
# urls = [
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/MKPRU",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/TRVOU",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/DIFF",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/MWNUS",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/AVBLS",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/BLCHS",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/HRATE",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/CPTRA"
# ]

# # Kafka configuration
# bootstrap_servers = 'localhost:9092'
# topic = 'nasdaq_data'

# # Create Kafka producer
# producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# # Produce data to Kafka topic
# for url in urls:
#     response = requests.get(url)
#     data = response.json()

#     # Send the data to Kafka topic
#     producer.send(topic, value=json.dumps(data).encode('utf-8'))

#     # Print success message
#     print(f"Data from {url} sent to Kafka topic: {topic}")
    
#     # Introduce a delay between requests (optional)
#     time.sleep(1)

# # Flush and close the Kafka producer
# producer.flush()
# producer.close()
########################################################################################################################
# from kafka import KafkaProducer
# import requests
# import json

# # Kafka configuration
# bootstrap_servers = 'localhost:9092'
# topic = 'nasdaq_data'

# # Define the API endpoint URLs
# urls = [
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/MKPRU",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/TRVOU",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/DIFF",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/MWNUS",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/AVBLS",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/BLCHS",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/HRATE",
#     "https://data.nasdaq.com/api/v3/datasets/BCHAIN/CPTRA"
# ]

# # Create Kafka producer
# producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# # Fetch data from API endpoints and send to Kafka topic
# for url in urls:
#     # Fetch data from the API
#     response = requests.get(url)
#     data = response.json()
    
#     # Print the JSON data
#     print("JSON Data:")
#     print(json.dumps(data, indent=4))
    
#     # Convert data to JSON string
#     json_data = json.dumps(data)
    
#     # Send data to Kafka topic
#     producer.send(topic, json_data.encode('utf-8'))
#     producer.flush()
    
#     # Print a message after sending the data
#     print("Data sent to Kafka topic:", topic)

# # Close the Kafka producer
# producer.close()

########################################################################################################################
from kafka import KafkaProducer
import requests
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
# topic = 'nasdaq_data'
topic = 'my-kafka-topic'

# Define the API endpoint URLs and corresponding collection names
urls = [
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/MKPRU?api_key=iaSRyL9_7hDwHZ8tATWt", "mkpru_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/TRVOU?api_key=iaSRyL9_7hDwHZ8tATWt", "trvou_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/DIFF?api_key=iaSRyL9_7hDwHZ8tATWt", "diff_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/MWNUS?api_key=iaSRyL9_7hDwHZ8tATWt", "mwnus_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/AVBLS?api_key=iaSRyL9_7hDwHZ8tATWt", "avbls_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/BLCHS?api_key=iaSRyL9_7hDwHZ8tATWt", "blchs_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/HRATE?api_key=iaSRyL9_7hDwHZ8tATWt", "hrate_collection"),
    ("https://data.nasdaq.com/api/v3/datasets/BCHAIN/CPTRA?api_key=iaSRyL9_7hDwHZ8tATWt", "cptra_collection")
]

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Fetch data from API endpoints and send to Kafka topic
for url, collection_name in urls:
    # Fetch data from the API
    response = requests.get(url)
    data = response.json()
    
    # Print the JSON data
    print("JSON Data:")
    print(json.dumps(data, indent=4))
    
    # Convert data to JSON string
    json_data = json.dumps(data)
    
    # Send data to Kafka topic with collection name as key
    producer.send(topic, key=collection_name.encode('utf-8'), value=json_data.encode('utf-8'))
    producer.flush()
    
    # Print a message after sending the data
    print("Data sent to Kafka topic:", topic)

# Close the Kafka producer
producer.close()



# https://data.nasdaq.com/api/v3/datasets/BCHAIN/MKPRU?api_key=iaSRyL9_7hDwHZ8tATWt