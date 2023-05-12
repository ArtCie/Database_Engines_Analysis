from bs4 import BeautifulSoup
import requests


class Scraper:
    def __init__(self):
        self.HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
                        "Accept-Encoding": "gzip, deflate",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1",
                        "Connection": "close", "Upgrade-Insecure-Requests": "1"}

    def fetch_data(self, page_number):
        r = requests.get(
            f'https://www.amazon.com/s?i=electronics-intl-ship&bbn=16225009011&rh=n%3A16225009011%2Cn%3A2811119011&page={page_number}&ref=sr_pg_{page_number}',
            headers=self.HEADERS)
        content = r.content
        soup = BeautifulSoup(content, features="lxml")

        result = []
        for elem in soup.findAll('div', attrs={'class': 'a-section a-spacing-small puis-padding-left-small puis-padding-right-small'}):
            result.append(self.parse_output(elem))
        return result

    def parse_output(self, elem):
        name = elem.find('span', attrs={'class': 'a-size-base-plus a-color-base a-text-normal'}).get_text()
        price = self.get_price(elem)
        rating = self.get_rating(elem)
        rating_count = self.get_rating_count(elem)
        return {
            "name": name,
            "rating": rating,
            "price": price,
            "rating_count": rating_count,
        }

    @staticmethod
    def get_price(elem):
        price = elem.find('span', attrs={'class': 'a-offscreen'})
        return price.get_text() if price else None

    @staticmethod
    def get_rating(elem):
        rating = elem.find('span', attrs={"aria-label": True})
        result = None
        if rating and rating.get('aria-label'):
            result = rating['aria-label'].split(" ")[0]
        return result

    @staticmethod
    def get_rating_count(elem):
        rating_count = elem.find('span', attrs={'class': 'a-size-base s-underline-text'})
        return rating_count.get_text() if rating_count else None
