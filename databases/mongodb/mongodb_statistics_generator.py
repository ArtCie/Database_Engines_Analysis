from random import random
from time import time
from uuid import uuid4

from databases.mongodb.mongodb_manager import MongodbManager

from databases.postgresql.postgresql_statistics_generator import PostgresqlStatisticsGenerator


class MongoDbStatisticsGenerator(MongodbManager):
    def __init__(self, postgresql_statistics_manager: PostgresqlStatisticsGenerator):
        super().__init__()
        self.postgresql_statistics_manager = postgresql_statistics_manager

    def generate_statistics(self, collection, data):
        records_sizes = [25, 100, 1000, 10000, 100000, 1000000]
        for db_size in records_sizes:
            print(f"MongoDB Processing database size: {db_size}")
            self._generate_data(collection, data, db_size)

    def _generate_data(self, collection, data, db_size):
        self.clear_database(collection)
        self._insert_data(collection, data, db_size)
        count_time = self._get_count_time(collection)
        median_time = self._get_median_time(collection)
        average_time = self._get_average_time(collection)
        select_time = self._get_select_time(collection)
        update_time = self._get_update_time(collection)
        aggregation_time = self._get_aggregation_time(collection)
        sort_time = self._get_sort_time(collection)

        connection, cursor = self.postgresql_statistics_manager.connect()
        self.postgresql_statistics_manager.insert_result(db_size, count_time, median_time, average_time, select_time,
                                                         update_time, aggregation_time, sort_time, cursor, db_type="mongo")

    @staticmethod
    def _get_count_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Count -> {collection.fetchall()}")
        return time() - start_time

    @staticmethod
    def _get_median_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Median -> {collection.fetchall()}")
        return time() - start_time

    @staticmethod
    def _get_average_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Average -> {collection.fetchall()}")
        return time() - start_time

    @staticmethod
    def _get_select_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Median -> {collection.rowcount}")
        return time() - start_time

    @staticmethod
    def _get_update_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Update -> {collection.rowcount}")
        return time() - start_time

    @staticmethod
    def _get_aggregation_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Aggregation -> {collection.rowcount}")
        return time() - start_time

    @staticmethod
    def _get_sort_time(collection):
        start_time = time()
        # TODO
        # print(f"MongoDB Sort -> {collection.rowcount}")
        return time() - start_time

    @staticmethod
    def _insert_data(collection, data, db_size) -> None:
        insert_data = []
        counter = 0
        for i in range(1000):
            for index, record in enumerate(data):
                insert_data.append({
                    "_id": str(uuid4()) + str(random()),
                    "name": record.name,
                    "price": float(record.price),
                    "rating": float(record.rating),
                    "rating_count": int(record.rating_count),
                    "timestamp": record.timestamp,
                    "random": random()
                })
                counter += 1
                if counter == db_size:
                    collection.insert_many(insert_data)
                    return
        collection.insert_many(insert_data)
