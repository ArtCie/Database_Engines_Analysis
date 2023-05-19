from databases.database import Database
from databases.utils import measure_time
from pymongo import MongoClient
from uuid import uuid4
import random

class MongodbManager(Database):
    def connect(self):
        client = MongoClient('localhost', 27017)
        return None, client.local.products

    @measure_time
    def clear_database(self, collection) -> None:
        collection.remove({})

    @measure_time
    def insert_database(self, collection, data) -> None:
        insert_data = []
        for i in range(1000):
            for index, record in enumerate(data):
                insert_data.append({
                    "_id": str(uuid4()),
                    "name": record.name,
                    "price": float(record.price),
                    "rating": float(record.rating),
                    "rating_count": int(record.rating_count),
                    "timestamp": record.timestamp,
                    "test": random.random()
                })
        collection.insert_many(insert_data)

    @measure_time
    def update_database(self, collection) -> None:
        myquery = {"name": "(2 Pack) LG Velvet Screen Protector Tempered Glass, [Leave Space for Case] 3D Curved Edge High Definition Anti Scratch 9H Hardness Bubble-Free Case Friendly Screen Protector for LG Velvet /LG Velvet 5G UW 6.8"}
        newvalues = {"$set": {"name": "Mydelko Fa"}}

        x = collection.update_many(myquery, newvalues)
        print(x.modified_count, "documents updated.")

