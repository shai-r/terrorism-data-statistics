import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(verbose=True)

client = MongoClient(os.environ['MONGODB_URL'])
db = client[os.environ['TERRORISM_DATA']]
terrorism_collection = db.get_collection('terrorism_events')