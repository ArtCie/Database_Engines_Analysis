from typing import List
from data_collector.products.product import Product


class ProductRepository:
    _product_repository: List[Product]

    def __init__(self):
        self._product_repository = []

    def add_product(self, product: Product):
        self._product_repository.append(product)

    def fetch_products(self):
        return self._product_repository
