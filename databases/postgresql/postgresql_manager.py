from databases.database import Database
from databases.utils import measure_time, write_results_to_file
import psycopg2
from psycopg2 import extras


class PostgresqlManager(Database):
    def connect(self):
        conn = psycopg2.connect(database="postgres",
                                host="localhost",
                                user="artur",
                                password="",
                                port="5432")
        conn.autocommit = True
        return conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    @staticmethod
    def close_connection(connection):
        connection.close()

    @measure_time
    def clear_database(self, cursor) -> None:
        query = "DELETE FROM products";
        cursor.execute(query)

    @measure_time
    def insert_database(self, cursor, data) -> None:
        # for i in range(1000):
        query = "INSERT INTO PRODUCTS(name, price, rating, rating_count, timestamp) VALUES "
        for record in data:
            query += f"('{record.name}', {record.price}, {record.rating}, {record.rating_count}, '{record.timestamp}'), "
        cursor.execute(query[:-2])

    def update_database(self, cursor, query) -> None:
        query = query[0][1]
        if not query:
            query = "UPDATE products set name = 'Mydelko FA' WHERE name = '(2 Pack) LG Velvet Screen Protector Tempered Glass, [Leave Space for Case] 3D Curved Edge High Definition Anti Scratch 9H Hardness Bubble-Free Case Friendly Screen Protector for LG Velvet /LG Velvet 5G UW 6.8'"
        else:
            query = f"UPDATE products {query}"
        self.update_rows(cursor, query)

    @measure_time
    def update_rows(self, cursor, query):
        try:
            cursor.execute(query)
            print(f"PostgreSQL Update Success! Rows affected -> {cursor.rowcount}")
        except Exception as e:
            print(f"PostgreSQL Update Exception! {str(e)}")

    def select_from_database(self, cursor, args) -> None:
        where_expression = args[0][1]
        catalog = args[0][2]
        query = "SELECT * from products"
        if where_expression:
            query += f" WHERE {where_expression}"
        result = self.select_rows(cursor, query)
        write_results_to_file(catalog, file_name="PostgreSQL", data=result)

    @measure_time
    def select_rows(self, cursor, query):
        try:
            cursor.execute(query)
            print(f"PostgreSQL Select Success! Rows fetched -> {cursor.rowcount}")
            return cursor.fetchall()
        except Exception as e:
            print(f"PostgreSQL Select Exception! {str(e)}")