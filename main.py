from data_collector.data_collector import DataCollector
from data_collector.scrapper import Scraper
from data_collector.products.product_repository import ProductRepository


if __name__ == '__main__':
    scrapper = Scraper()
    product_repository = ProductRepository()
    data_collector = DataCollector(product_repository, scrapper, 20)
    data_collector.get_data()
    print(f"Data len: {len(data_collector.product_repository.fetch_products())}")