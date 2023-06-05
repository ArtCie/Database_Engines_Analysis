from time import time

from databases.postgresql.postgresql_manager import PostgresqlManager


class PostgresqlStatisticsGenerator(PostgresqlManager):
    def __init__(self):
        super().__init__()

    def generate_statistics(self, cursor, data):
        records_sizes = [25, 100, 1000, 10000, 100000, 1000000]
        for db_size in records_sizes:
            print(f"PostgreSQL Processing database size: {db_size}")
            self._generate_data(cursor, data, db_size)

    def _generate_data(self, cursor, data, db_size):
        self.clear_database(cursor)
        self._insert_data(cursor, data, db_size)
        count_time = self._get_count_time(cursor)
        median_time = self._get_median_time(cursor)
        average_time = self._get_average_time(cursor)
        select_time = self._get_select_time(cursor)
        update_time = self._get_update_time(cursor)
        aggregation_time = self._get_aggregation_time(cursor)
        sort_time = self._get_sort_time(cursor)
        self.insert_result(db_size, count_time, median_time, average_time, select_time, update_time, aggregation_time, sort_time, cursor, db_type="pg")

    @staticmethod
    def _get_count_time(cursor):
        start_time = time()
        query = """SELECT COUNT(*) FROM products"""
        cursor.execute(query)
        print(f"PostgreSQL Count -> {cursor.fetchall()}")
        return time() - start_time

    @staticmethod
    def _get_median_time(cursor):
        start_time = time()
        query = """SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY rating_count) FROM products"""
        cursor.execute(query)
        print(f"PostgreSQL Median -> {cursor.fetchall()}")
        return time() - start_time

    @staticmethod
    def _get_average_time(cursor):
        start_time = time()
        query = """SELECT avg(rating_count) FROM products"""
        cursor.execute(query)
        print(f"PostgreSQL Average -> {cursor.fetchall()}")
        return time() - start_time

    @staticmethod
    def _get_select_time(cursor):
        start_time = time()
        query = """SELECT * FROM products"""
        cursor.execute(query)
        print(f"PostgreSQL Select -> {cursor.rowcount}")
        return time() - start_time

    @staticmethod
    def _get_update_time(cursor):
        start_time = time()
        query = """UPDATE products set rating = 2.0"""
        cursor.execute(query)
        print(f"PostgreSQL Update -> {cursor.rowcount}")
        return time() - start_time

    @staticmethod
    def _get_aggregation_time(cursor):
        start_time = time()
        query = """SELECT sum(price), rating_count FROM products group by rating_count"""
        cursor.execute(query)
        print(f"PostgreSQL Aggregation -> {cursor.rowcount}")
        return time() - start_time

    @staticmethod
    def _get_sort_time(cursor):
        start_time = time()
        query = """SELECT * FROM products ORDER BY timestamp DESC"""
        cursor.execute(query)
        print(f"PostgreSQL Sort -> {cursor.rowcount}")
        return time() - start_time

    @staticmethod
    def _insert_data(cursor, data, db_size) -> None:
        counter = 0
        for i in range(1000):
            query = "INSERT INTO PRODUCTS(name, price, rating, rating_count, timestamp) VALUES "
            for record in data:
                query += f"('{record.name}', {record.price}, {record.rating}, {record.rating_count}, '{record.timestamp}'), "
                counter += 1
                if counter == db_size:
                    cursor.execute(query[:-2])
                    return
            cursor.execute(query[:-2])

    def insert_result(self, db_size, count_time, median_time, average_time, select_time, update_time, aggregation_time, sort_time, cursor, db_type):
        data = {
            "db_size": db_size,
            "count_time": count_time,
            "median_time": median_time,
            "average_time": average_time,
            "select_time": select_time,
            "update_time": update_time,
            "aggregation_time": aggregation_time,
            "sort_time": sort_time,
            "db_type": db_type
        }
        query = """
            INSERT INTO public."statistics"
            (
                db_size, 
                count_time, 
                median_time, 
                average_time, 
                select_time, 
                update_time, 
                aggregation_time, 
                sort_time, 
                db_type
            )
            VALUES
            (
                %(db_size)s, 
                %(count_time)s, 
                %(median_time)s, 
                %(average_time)s, 
                %(select_time)s, 
                %(update_time)s, 
                %(aggregation_time)s, 
                %(sort_time)s, 
                %(db_type)s
            )
        """
        cursor.execute(query, data)