# from kafka import KafkaConsumer
# import pandas as pd
# import json

# # Kafka configuration
# bootstrap_servers = 'localhost:9092'
# topic = 'nasdaq_data'

# # Create a Kafka consumer instance
# consumer = KafkaConsumer(
#     bootstrap_servers=bootstrap_servers,
#     auto_offset_reset='earliest',
#     enable_auto_commit=False,
#     group_id='my-consumer-group'
# )

# # Subscribe to the Kafka topic
# consumer.subscribe(topics=[topic])

# # Create an empty list to store the data
# data_list = []

# # Consume messages from the Kafka topic
# for message in consumer:
#     # Decode the message value
#     message_value = message.value.decode('utf-8')
    
#     # Print the received message
#     print("Received message:")
#     print(message_value)
    
#     # Convert the message value from JSON to a dictionary
#     data = json.loads(message_value)
    
#     # Append the data to the list
#     data_list.append(data)
    
#     # Commit the offset manually
#     consumer.commit()
    
#     # Print a message after processing the message
#     print("Message processed and committed")
    
#     # Check if the desired number of messages is reached
#     if len(data_list) == 8:
#         break

# # Close the Kafka consumer
# consumer.close()

# # Convert the list of dictionaries to a DataFrame
# df = pd.DataFrame(data_list)

# # Print the DataFrame
# print("DataFrame:")
# print(df)
########################################################################################################################

from kafka import KafkaConsumer
import pymongo
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
# topic = 'nasdaq_data'
topic = 'my-kafka-topic'

# MongoDB configuration
mongodb_url = 'mongodb://localhost:27017/'
database_name = 'nasdaq_database'

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='my-consumer-group_1'
)

# Create a MongoDB client
client = pymongo.MongoClient(mongodb_url)

# Subscribe to the Kafka topic
consumer.subscribe(topics=[topic])

# Consume messages from the Kafka topic
for message in consumer:
    # Check if the message key is None
    if message.key is None:
        continue
    # Decode the message key and value
    collection_name = message.key.decode('utf-8')
    message_value = message.value.decode('utf-8')
    
    # Print the received message
    print("Received message:")
    print("Collection:", collection_name)
    print("Data:", message_value)
    
    # Convert the message value from JSON to a dictionary
    data = json.loads(message_value)
    
    # Select the MongoDB collection
    collection = client[database_name][collection_name]
    
    # Insert the data into the collection
    collection.insert_one(data)
    
    # Commit the offset manually
    consumer.commit()
    
    # Print a message after processing the message
    print("Message processed and committed")
    
# Close the Kafka consumer and MongoDB client
consumer.close()
client


