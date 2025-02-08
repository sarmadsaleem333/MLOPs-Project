from pymongo import MongoClient

# Your MongoDB Atlas connection string
CONNECTION_URL = "mongodb+srv://sarmadsaleem333:4FDygQmrMY9ZZkra@cluster0.yfdm9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

DATABASE_NAME = "Proj1"
COLLECTION_NAME = "Proj1-Data"


# Connect to MongoDB Atlas
import pandas as pd
import pymongo

df = pd.read_csv('C:/Users/sarma/Desktop/MLOPs-Project/notebook/data.csv')

print(df.head())
data = df.to_dict(orient='records')

client = MongoClient(CONNECTION_URL)

data_base = client[DATABASE_NAME]
collection = data_base[COLLECTION_NAME]
print("Connected successfully!!!")

rec = collection.insert_many(data)

print("Data inserted with record ids",)
