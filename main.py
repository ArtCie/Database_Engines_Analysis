from data_collector.data_collector import DataCollector
from data_collector.scrapper import Scraper
from data_collector.products.product_repository import ProductRepository
from databases.database_handler import DatabaseHandler
from databases.statistics_handler import StatisticsHandler
from interface.interface import InterfaceMain


if __name__ == '__main__':
    scrapper = Scraper()
    product_repository = ProductRepository()
    data_collector = DataCollector(product_repository, scrapper, 10)

    # data_collector.get_data()
    data_collector.get_test_data()
    database_handler = DatabaseHandler(product_repository)
    statistics_handler = StatisticsHandler(product_repository)
    InterfaceMain(database_handler, statistics_handler).draw()
