from time import time
from decimal import Decimal
from uuid import uuid4

from databases.dynamodb.dynamodb_manager import DynamodbManager

from databases.postgresql.postgresql_statistics_generator import PostgresqlStatisticsGenerator


class DynamodbStatisticsGenerator(DynamodbManager):
    def __init__(self, postgresql_statistics_manager: PostgresqlStatisticsGenerator):
        super().__init__()
        self.postgresql_statistics_manager = postgresql_statistics_manager

    def generate_statistics(self, client, data):
        records_sizes = [25, 100, 1000, 10000, 100000, 1000000]
        for db_size in records_sizes:
            self._generate_data(client, data, db_size)

    def _generate_data(self, client, data, db_size):
        self.clear_database(client)
        self._insert_data(client, db_size, data)
        table = client.Table('products')
        count_time = self._get_count_time(table)
        median_time = self._get_median_time(table)
        average_time = self._get_average_time(table)
        select_time = self._get_select_time(table)
        update_time = self._get_update_time(table)
        aggregation_time = self._get_aggregation_time(table)
        sort_time = self._get_sort_time(table)

        connection, cursor = self.postgresql_statistics_manager.connect()
        self.postgresql_statistics_manager.insert_result(db_size, count_time, median_time, average_time, select_time,
                                                         update_time, aggregation_time, sort_time, cursor,
                                                         db_type="dynamo")

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
    def _insert_data(client, db_size, data):
        request_items = {'products': []}
        for index, record in enumerate(data):
            request_items['products'].append({
                'PutRequest': {
                    'Item': {
                        "_id": str(uuid4()),
                        "name": record.name,
                        "price": Decimal(record.price),
                        "rating": Decimal(record.rating),
                        "rating_count": int(record.rating_count),
                        "timestamp": str(record.timestamp)
                    }
                }
            })
            if len(request_items['products']) == 25:
                client.batch_write_item(RequestItems=request_items)
                request_items['products'] = []
            if index == db_size:
                client.batch_write_item(RequestItems=request_items)
                break