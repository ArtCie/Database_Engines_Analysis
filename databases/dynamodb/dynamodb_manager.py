from databases.database import Database
from databases.utils import measure_time


class DynamodbManager(Database):
    def connect(self):
        pass

    @measure_time
    def clear_database(self, connection) -> None:
        pass

    @measure_time
    def insert_database(self, connection, data) -> None:
        pass

    @measure_time
    def update_database(self, connection) -> None:
        pass
