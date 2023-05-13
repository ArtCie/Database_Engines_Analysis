from typing import Optional
from datetime import datetime, timedelta
import random


class Product:
    name: Optional[str]
    price: Optional[str]
    rating: Optional[str]
    rating_count: Optional[str]

    def __init__(self, name, price, rating, rating_count, timestamp=None) -> None:
        self.name = name
        self.price = price
        self.rating = rating
        self.rating_count = rating_count
        self.timestamp = self.generate_random_timestamp() if not timestamp else timestamp

    @staticmethod
    def generate_random_timestamp():
        start = datetime(1980, 1, 1, 00, 00, 00)
        years = 2023 - 1980 + 1
        end = start + timedelta(days=365 * years)
        return start + (end - start) * random.random()

    def __repr__(self):
        return f"Name = {self.name} " \
               f"Price = {self.price} " \
               f"Rating = {self.rating} " \
               f"Rating count = {self.rating_count} " \
               f"Timestamp = {self.timestamp}" \
               f""
