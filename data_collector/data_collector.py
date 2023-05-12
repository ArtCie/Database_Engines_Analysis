from data_collector.scrapper import Scraper
from data_collector.products.product_repository import Product, ProductRepository


class DataCollector:
    def __init__(self, product_repository: ProductRepository, scraper: Scraper, page_num: int):
        self.scrapper = scraper
        self.product_repository = product_repository
        self.PAGE_NUM = page_num

    def get_data(self) -> None:
        for i in range(self.PAGE_NUM):
            print(f"Processing page number: {i + 1}")
            products = self.scrapper.fetch_data(i + 1)
            for product in products:
                self.product_repository.add_product(Product(product["name"], product["price"],
                                                            product["rating"], product["rating_count"]))