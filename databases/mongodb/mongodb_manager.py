from databases.database import Database
from databases.utils import measure_time, write_results_to_file
from pymongo import MongoClient
from uuid import uuid4
from dateutil import parser
import random


class MongodbManager(Database):
    def connect(self):
        client = MongoClient('localhost', 27017)
        return None, client.local.products

    @measure_time
    def clear_database(self, collection) -> None:
        collection.delete_many({})

    @measure_time
    def insert_database(self, collection, data) -> None:
        insert_data = []
        collection.drop()
        for i in range(1000):
            print(f"{i} - mongodb")
            for index, record in enumerate(data):
                insert_data.append({
                    "_id": str(uuid4()),
                    "name": record.name,
                    "price": float(record.price),
                    "rating": float(record.rating),
                    "rating_count": int(record.rating_count),
                    "timestamp": record.timestamp
                })
            collection.insert_many(insert_data)

    def update_database(self, collection, args) -> None:
        query = args[0][1]
        if not query:
            query = {
                "name": "(2 Pack) LG Velvet Screen Protector Tempered Glass, [Leave Space for Case] 3D Curved Edge High Definition Anti Scratch 9H Hardness Bubble-Free Case Friendly Screen Protector for LG Velvet /LG Velvet 5G UW 6.8"}
            update_values = {"$set": {"name": "Mydelko Fa"}}
        else:
            query, update_values = self._parse_query(query)
        self._update_collection(collection, query, update_values)

    def _parse_query(self, query):
        set_values, query_raw = query.split(" WHERE ")
        set_values = set_values[4:]
        update_values = {"$set": self._build_dict_values(set_values)}
        query = self._build_query(query_raw)
        return query, update_values

    def _build_dict_values(self, set_values, split_condition=", "):
        result = {}
        set_values = set_values.replace("'", "").replace('"', "")
        for value in set_values.split(split_condition):
            parameter_name, parameter_value = value.split("=")
            parameter_value_parsed = self._parse_value(parameter_name, parameter_value)
            result[parameter_name] = parameter_value_parsed
        return result

    @staticmethod
    def _parse_value(param_name, param_value):
        try:
            if param_name in ['rating', 'price']:
                return float(param_value)
            if param_name == 'rating_count':
                return int(param_value)
            if param_name == 'timestamp':
                timestamp = parser.parse(param_value)
                return timestamp.timestamp
            return param_value
        except Exception as e:
            print(f"MongoDB update Exception -> Bad query! {str(e)}")

    def _build_query(self, query_raw):
        if 'OR' not in query_raw:
            return self._build_dict_values(query_raw, split_condition=" AND ")
        return self._build_or_query(query_raw)

    def _build_or_query(self, query_raw):
        result = {"$or": []}
        for condition in query_raw.split(" OR "):
            result["$or"].append(self._build_dict_values(condition, split_condition=" AND "))
        return result

    @measure_time
    def _update_collection(self, collection, query, update_values):
        try:
            x = collection.update_many(query, update_values)
            print(f"MongoDB update Success: Rows affected -> {x.modified_count}")
        except Exception as e:
            print(f"MongoDB update Exception: {str(e)}")

    def select_from_database(self, collection, args) -> None:
        query = args[0][1]
        catalog = args[0][2]
        query = self._build_query(query) if query else {}
        res = self._select_records(collection, query)
        write_results_to_file(catalog, "MongoDB", res)

    @measure_time
    def _select_records(self, collection, query):
        try:
            cursor = collection.find(query)
            res = [document for document in cursor]
            print(f"MongoDB Select Success! Rows fetched -> {len(res)}")
            return res
        except Exception as e:
            print(f"MongoDB Select Exception! {str(e)}")