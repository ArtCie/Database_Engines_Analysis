import csv
from dateutil.parser import parse

from data_collector.scrapper import Scraper
from data_collector.products.product_repository import Product, ProductRepository


class DataCollector:
    def __init__(self, product_repository: ProductRepository, scraper: Scraper, page_num: int):
        self.scrapper = scraper
        self.product_repository = product_repository
        self.PAGE_NUM = page_num

    def get_data(self) -> None:
        for i in range(350, 400):
            print(f"Processing page number: {i + 1}")
            products = self.scrapper.fetch_data(i + 1)
            for product in products:
                self.product_repository.add_product(Product(product["name"].replace('"', ''), product["price"][1:],
                                                            product["rating"], product["rating_count"].replace(",", '')))
        # for testing purposes
        with open('products.csv', 'a') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['name', 'price', 'rating', 'rating_count', 'timestamp'])

            for product in self.product_repository.fetch_products():
                csv_writer.writerow([product.name, product.price, product.rating, product.rating_count, product.timestamp])

    def get_test_data(self):
        with open("products.csv", 'r') as file:
            csvreader = csv.DictReader(file)
            for row in csvreader:
                self.product_repository.add_product(Product(row['name'], row['price'], row['rating'],
                                                            row['rating_count'], parse(row['timestamp'])))
