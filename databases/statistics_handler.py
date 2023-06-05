from databases.dynamodb.dynamodb_statistics_generator import DynamodbStatisticsGenerator
from databases.postgresql.postgresql_statistics_generator import PostgresqlStatisticsGenerator
from databases.mongodb.mongodb_statistics_generator import MongoDbStatisticsGenerator
from data_collector.products.product_repository import ProductRepository

from databases.postgresql.postgresql_manager import PostgresqlManager

import threading


class StatisticsHandler:
    def __init__(self, population_repository: ProductRepository):
        self.population_repository = population_repository

        self.statistics_generators = [
            # PostgresqlStatisticsGenerator(),
            MongoDbStatisticsGenerator(PostgresqlStatisticsGenerator()),
            # DynamodbStatisticsGenerator(PostgresqlStatisticsGenerator())
        ]

    @staticmethod
    def execute_threads_generator(f):
        def wrapper(*args):
            thread_list = []
            for statistic_generator in args[0].statistics_generators:
                connection, cursor = statistic_generator.connect()
                t = f(args[0], cursor, statistic_generator)
                thread_list.append(t)
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()

        return wrapper

    @execute_threads_generator
    def generate_statistics(self, connection, statistic_generator):
        return threading.Thread(target=statistic_generator.generate_statistics,
                                args=(connection, self.population_repository.fetch_products()))