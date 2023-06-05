from databases.postgresql.postgresql_manager import PostgresqlManager
from databases.mongodb.mongodb_manager import MongodbManager
from databases.dynamodb.dynamodb_manager import DynamodbManager
from data_collector.products.product_repository import ProductRepository

import threading


class DatabaseHandler:
    def __init__(self, population_repository: ProductRepository):
        self.population_repository = population_repository
        self.databases = [
            PostgresqlManager(),
            MongodbManager(),
            DynamodbManager()
        ]

    @staticmethod
    def execute_threads(f):
        def wrapper(*args):
            thread_list = []
            for database in args[0].databases:
                connection, cursor = database.connect()
                t = f(args[0], cursor, database, args)
                thread_list.append(t)
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()
        return wrapper

    @execute_threads
    def clear_databases(self, connection, database, *args):
        return threading.Thread(target=database.clear_database, args=(connection,))

    @execute_threads
    def populate_databases(self, connection, database, *args):
        return threading.Thread(target=database.insert_database, args=(connection, self.population_repository.fetch_products()))

    @execute_threads
    def update_databases(self, connection, database, *args):
        return threading.Thread(target=database.update_database, args=(connection, args,))

    @execute_threads
    def select_from_databases(self, connection, database, *args):
        return threading.Thread(target=database.select_from_database, args=(connection, args,))
