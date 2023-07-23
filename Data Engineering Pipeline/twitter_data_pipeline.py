import tweepy
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd

# Twitter API credentials
access_token = "paste_here"
access_token_secret = "paste_here"
consumer_key = "paste_here"
consumer_secret = "paste_here"

# MongoDB connection settings
mongodb_uri = "mongodb://localhost:27017/"
mongodb_database = "twitter_db_1"
mongodb_collection = "tweets_collection_1"

# Kafka settings
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'twitter_topic'
kafka_group_id = 'twitter_consumer_group'

# Tweepy API authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_bootstrap_servers, group_id=kafka_group_id, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Checking for the existence of the file
try:
    all_tweets = pd.read_json("/Users/kavyasnair/Documents/SRH Heidelberg/Applied Data Science and Analytics/Sem2/Data Engineering 2/Project/all_tweets.json", orient='records')
except:
    all_tweets = pd.DataFrame(columns=['id'])

# Function to scrape data from Twitter, save to MongoDB, and send to Kafka
def data_scraping(all_tweets, keywords):
    tweet_information = []
    tweet_ids = []
    existing_tweet_ids = all_tweets['id'].tolist()  # Adding the existing tweet IDs to a list
    print("Fetching data from Twitter")

    for keyword in keywords:  # Iterating through all the keywords
        keyword_tweet_details = api.search_tweets(keyword, count=1000)
        for keyword_tweet_detail in keyword_tweet_details:  # Iterating through all the tweets related to a keyword
            id = keyword_tweet_detail.id
            if id not in existing_tweet_ids and id not in tweet_ids:  # Checking if the obtained new tweet ID is in existing IDs and latest tweets appended
                print(id)
                status = api.get_status(id, tweet_mode='extended')  # Fetching the status
                tweet_information.append(status._json)
                tweet_ids.append(id)
                producer.send(kafka_topic, status._json)  # Send tweet to Kafka topic
            else:
                print(id, "exists")

    producer.flush()  # Flush Kafka producer

    tweet_df = pd.DataFrame(tweet_information)

    # Save tweets to MongoDB
    client = MongoClient(mongodb_uri)
    db = client[mongodb_database]
    collection = db[mongodb_collection]
    collection.insert_many(tweet_information)
    print("Tweets saved to MongoDB")

    return tweet_df


# Keywords to search for
keywords = ["#JNJ", "johnsonandjohnson"]

# Scrape data, save to MongoDB, and retrieve as DataFrame
tweet_df = data_scraping(all_tweets, keywords)

# Print the DataFrame
print(tweet_df.head())

# checking the existance of file and doing assigning or append operation accordingly
try:
    final_df = all_tweets.append(tweet_df, ignore_index=True)
except:
    final_df = tweet_df   


final_df.to_json(r"/Users/kavyasnair/Documents/SRH Heidelberg/Applied Data Science and Analytics/Sem2/Data Engineering 2/Project/all_tweets.json",orient='records')    # saving data to json file 

file_path = "/Users/kavyasnair/Documents/SRH Heidelberg/Applied Data Science and Analytics/Sem2/Data Engineering 2/Project/all_tweets.json"
df2 = pd.read_json(file_path)

def count_dataframe(df):
    count = len(df.index)
    return count

num_of_tweets = count_dataframe(df2)
print(f"No of tweets : {num_of_tweets}")


# # Consume tweets from Kafka
# for message in consumer:
#     tweet = message.value
#     print("Received tweet from Kafka:", tweet)
#     # Do further processing or analysis with the received tweet

# Consume tweets from Kafka
consumer.subscribe(topics=[kafka_topic])

try:
    while True:
        records = consumer.poll(timeout_ms=1000,max_records= 10)
        print("Iam here")
        for _, messages in records.items():
            print("Iam inside 1st for")
            for message in messages:
                print("Iam inside 2nd for")
                tweet = message.value
                print("Received tweet from Kafka:", tweet)
                # Do further processing or analysis with the received tweet
except KeyboardInterrupt:
    pass

# Close Kafka consumer
consumer.close()
