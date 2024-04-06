from pymongo import MongoClient

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["your_database"]
insights_collection = db["analytics"]

def store_insights(insights):
    insights_collection.insert_one(insights)