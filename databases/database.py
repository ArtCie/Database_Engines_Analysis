from abc import ABC, abstractmethod


class Database(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def clear_database(self, connection) -> None:
        pass

    @abstractmethod
    def insert_database(self, connection, data) -> None:
        pass

    @abstractmethod
    def update_database(self, connection, query) -> None:
        pass

    @abstractmethod
    def select_from_database(self, connection, query) -> None:
        pass
