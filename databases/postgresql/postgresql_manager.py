from databases.database import Database
from databases.utils import measure_time
import psycopg2


class PostgresqlManager(Database):
    def connect(self):
        conn = psycopg2.connect(database="postgres",
                                host="localhost",
                                user="artur",
                                password="",
                                port="5432")
        conn.autocommit = True
        return conn, conn.cursor()

    @staticmethod
    def close_connection(connection):
        connection.close()

    @measure_time
    def clear_database(self, cursor) -> None:
        query = "DELETE FROM products";
        cursor.execute(query)

    @measure_time
    def insert_database(self, cursor, data) -> None:
        for i in range(1000):
            query = "INSERT INTO PRODUCTS(name, price, rating, rating_count, timestamp) VALUES "
            for record in data:
                query += f"('{record.name}', {record.price}, {record.rating}, {record.rating_count}, '{record.timestamp}'), "
            cursor.execute(query[:-2])

    @measure_time
    def update_database(self, cursor) -> None:
        query = "UPDATE products set name = 'Mydelko FA' WHERE name = '(2 Pack) LG Velvet Screen Protector Tempered Glass, [Leave Space for Case] 3D Curved Edge High Definition Anti Scratch 9H Hardness Bubble-Free Case Friendly Screen Protector for LG Velvet /LG Velvet 5G UW 6.8'"
        cursor.execute(query)
