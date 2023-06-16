from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient()
db = client.nasdaq_database

# Define the collection names
collection_names = ['avbls_collection', 'blchs_collection', 'cptra_collection', 'diff_collection',
                    'hrate_collection', 'mkpru_collection', 'mwnus_collection', 'trvou_collection']

# Create a dictionary to store the DataFrames
dataframes = {}

# Iterate over the collection names
for collection_name in collection_names:
    # Get the collection
    collection = db[collection_name]
    
    # Fetch data from the collection
    data = collection.find()

    # Access the details within the dataset column
    dataset_details = data[0]['dataset']

    # Convert the dataset details to a DataFrame
    df_dataset = pd.DataFrame([dataset_details])
    
    # Extract the data column from df_dataset
    data_column = df_dataset['data']

    # Create a new DataFrame with the extracted data
    df_data = pd.DataFrame(data_column)
    # Print the DataFrame
    print(f"Data from : {collection_name}")
    print(df_data)

    # Create empty lists to store date and value
    dates = []
    values = []

    # Iterate over each row in the DataFrame
    for row in df_data.itertuples(index=False):
        # Iterate over each entry in the data column
        for entry in row.data:
            # Extract date and value from the entry
            date, value = entry
            
            # Append date and value to the respective lists
            dates.append(date)
            values.append(value)

    # Create a new DataFrame with date and value columns
    df_extracted_data = pd.DataFrame({'Date': dates, 'Value': values})

    # Print the extracted data DataFrame
    print("\n")
    print("Extracted Data:")
    print(df_extracted_data)

    print("\n Data Types of Columns in df_extracted_data:")
    print(df_extracted_data.dtypes)

    # Convert the date column to date data type
    df_extracted_data['Date'] = pd.to_datetime(df_extracted_data['Date'])


    # Save the extracted data to the dictionary
    dataframes[collection_name] = df_extracted_data
    
    # Print the dataset details
    print("Data from", collection_name)
    print(df_extracted_data)
    print("------------------------------")

    #Save the DataFrame to an Excel file
    excel_filename = f"{collection_name}_data.xlsx"
    df_extracted_data.to_excel(excel_filename, index=False)
    print(f"Saved {collection_name} DataFrame to {excel_filename}")

# Access the individual DataFrames
avls_df = dataframes['avbls_collection']
blchs_df = dataframes['blchs_collection']
cptra_df = dataframes['cptra_collection']
diff_df = dataframes['diff_collection']
hrate_df = dataframes['hrate_collection']
mkpru_df = dataframes['mkpru_collection']
mwnus_df = dataframes['mwnus_collection']
trvou_df = dataframes['trvou_collection']

#################################################################################################################
# from pymongo import MongoClient
# import pandas as pd

# # Connect to MongoDB
# client = MongoClient()
# db = client.nasdaq_database

# # Define the collection names
# collection_names = ['avbls_collection', 'blchs_collection', 'cptra_collection', 'diff_collection',
#                     'hrate_collection', 'mkpru_collection', 'mwnus_collection', 'trvou_collection']

# # Create a dictionary to store the DataFrames
# dataframes = {}

# # Iterate over the collection names
# for collection_name in collection_names:
#     # Get the collection
#     collection = db[collection_name]
    
#     # Fetch data from the collection
#     data = collection.find()

#     # Access the details within the dataset column
#     dataset_details = data[0]['dataset']

#     # Convert the dataset details to a DataFrame
#     df_dataset = pd.DataFrame([dataset_details])
    
#     # Extract the data column from df_dataset
#     data_column = df_dataset['data']

#     # Create a new DataFrame with the extracted data
#     df_data = pd.DataFrame(data_column)
    
#     # Create empty lists to store date and value
#     dates = []
#     values = []

#     # Iterate over each row in the DataFrame
#     for row in df_data.itertuples(index=False):
#         # Iterate over each entry in the data column
#         for entry in row.data:
#             # Extract date and value from the entry
#             date, value = entry
            
#             # Append date and value to the respective lists
#             dates.append(date)
#             values.append(value)

#     # Create a new DataFrame with date and value columns
#     df_extracted_data = pd.DataFrame({'Date': dates, f'{collection_name}_Value': values})

#     # Convert the date column to date data type
#     df_extracted_data['Date'] = pd.to_datetime(df_extracted_data['Date'])

#     # Save the extracted data to the dictionary
#     dataframes[collection_name] = df_extracted_data
    
#     # Print the dataset details
#     print("Data from", collection_name)
#     print(df_extracted_data)
#     print("------------------------------")

# # Merge the DataFrames based on 'Date'
# merged_df = pd.concat(dataframes.values()).sort_values('Date').reset_index(drop=True)

# # Print the merged DataFrame
# print("Merged DataFrame:")
# print(merged_df)

# # Save the merged DataFrame to a CSV file
# merged_df.to_csv('merged_data.csv', index=False)