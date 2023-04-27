import csv
import gzip
import json
import pymongo
from pymongo import MongoClient

# Establish a connection to a MongoDB database
client = pymongo.MongoClient(f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test")
database_name = "amazon_reviews12"
db = client[database_name]
collection_name = "pc1"
collection = db[collection_name]

# Download and extract the dataset
url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_PC_v1_00.tsv.gz"
filename = "amazon_reviews_us_PC_v1_00.tsv.gz"
with gzip.open(filename, "rt", encoding = "utf-8") as f:
  reader = csv.DictReader(f, delimiter = "\t")
  for i, row in enumerate(reader):
    collection.insert_one(row)
    if i == 1010:
      break

# Close the MongoDB database connection
client.close()
