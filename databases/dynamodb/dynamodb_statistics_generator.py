from time import time
from decimal import Decimal
from uuid import uuid4
from statistics import median, mean
from collections import defaultdict

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
    def _get_count_time(table):
        start_time = time()
        count = table.scan(Select='COUNT')['Count']
        print(f"DynamoDB Count -> {count}")
        return time() - start_time

    @staticmethod
    def _get_median_time(table):
        start_time = time()
        response = table.scan()
        items = response['Items']
        rating_counts = [item['rating_count'] for item in items]
        median_rating = median(rating_counts)
        print(f"DynamoDB Median -> {median_rating}")
        return time() - start_time

    @staticmethod
    def _get_average_time(table):
        start_time = time()
        response = table.scan()
        items = response['Items']
        rating_counts = [item['rating_count'] for item in items]
        average_rating = mean(rating_counts)
        print(f"DynamoDB Mean -> {average_rating}")
        return time() - start_time

    @staticmethod
    def _get_select_time(table):
        start_time = time()
        response = table.scan()
        items = response['Items']
        count = len(items)
        print(f"DynamoDB Count -> {count}")
        return time() - start_time

    @staticmethod
    def _get_update_time(table):
        start_time = time()
        response = table.scan()
        items = response['Items']
        for item in items:
            table.update_item(
                Key={'_id': item['_id']},
                UpdateExpression='SET #rating = :rating',
                ExpressionAttributeNames={'#rating': 'rating'},
                ExpressionAttributeValues={':rating': Decimal(2)}
            )
        count = len(items)
        print(f"DynamoDB Update -> {count}")
        return time() - start_time

    @staticmethod
    def _get_aggregation_time(table):
        start_time = time()
        response = table.scan()
        items = response['Items']
        aggregation_results = defaultdict(float)
        for item in items:
            rating_count = item['rating_count']
            price = item['price']
            aggregation_results[rating_count] += float(price)
        count = len(aggregation_results)
        print(f"DynamoDB Aggregation -> {count}")
        return time() - start_time

    @staticmethod
    def _get_sort_time(table):
        start_time = time()
        response = table.scan()
        items = response['Items']
        sorted_items = sorted(items, key=lambda x: x['timestamp'], reverse=True)
        count = len(sorted_items)
        print(f"DynamoDB Sort -> {count}")
        return time() - start_time

    @staticmethod
    def _insert_data(client, db_size, data):
        request_items = {'products': []}
        counter = 0
        for i in range(1000):
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
                counter += 1
                if len(request_items['products']) == 25:
                    client.batch_write_item(RequestItems=request_items)
                    request_items['products'] = []
                if counter == db_size:
                    if len(request_items['products']) > 0:
                        client.batch_write_item(RequestItems=request_items)
                    return