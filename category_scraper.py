import os
import sys
import csv
import math
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from bs4 import BeautifulSoup
from selenium import webdriver
from dotenv import load_dotenv
from selenium.webdriver import Remote
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection

load_dotenv()

BASE_URL = "https://www.tesco.com"


def get_categories() -> List[str]:
    categories = [
        "/christmas/all",
        "/fresh-food/all",
        "/bakery/all",
        "/frozen-food/all",
        "/treats-and-snacks/all",
        "/food-cupboard/all",
        "/drinks/all",
        "/baby-and-toddler/all",
        "/health-and-beauty/all",
        "/pets/all",
        "/household/all",
        "/home-and-ents/all",
    ]

    return [
        f"https://www.tesco.com/groceries/en-GB/shop{category}"
        for category in categories
    ]


class CategoryScraper:
    _categories: List[str]
    _sbr_webdriver_connection: ChromiumRemoteConnection

    def __init__(
        self,
        categories: List[str],
        sbr_webdriver_connection: ChromiumRemoteConnection,
    ):
        self._categories = categories
        self._sbr_webdriver_connection = sbr_webdriver_connection

    def get_products_by_category(self, category: str) -> List[str]:
        products: List[str] = []

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        
        logging.info(category)

        try:
            with Remote(
                self._sbr_webdriver_connection, options=chrome_options
            ) as driver:
                driver.get(f"{category}?page=1")
                html = driver.page_source
                page = BeautifulSoup(html, "html5lib")

                last_page_number = int(
                    page.find_all("li", class_="pagination-btn-holder")[
                        -2
                    ].span.get_text()
                )

            for page_no in range(0, last_page_number):
                with Remote(
                    self._sbr_webdriver_connection, options=chrome_options
                ) as driver:
                    driver.get(f"{category}?page={page_no + 1}")
                    html = driver.page_source
                    page = BeautifulSoup(html, "html5lib")
                    elements = page.find_all("a", class_="product-image-wrapper")
                    products += [
                        f"{BASE_URL}/{element['href']}" for element in elements
                    ]

        except Exception as e:
            logging.info(f"Exception: {str(e)}")
        finally:
            return products

    def run(self):
        for category in self._categories:
            product_links = self.get_products_by_category(category)
            csv_file_name = "Tesco_Products_Links.csv"
            with open(csv_file_name, "a", newline="") as csv_file:
                fieldnames = ["Link"]
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                if csv_file.tell() == 0:
                    writer.writeheader()
                for product in product_links:
                    writer.writerow({"Link": product})


def run_category_scraper():
    csv_file_name = "Tesco_Products_Links.csv"
    if os.path.exists(csv_file_name):
        os.remove(csv_file_name)

    logging.basicConfig(
        format="[%(asctime)s] %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    SBR_WEBDRIVER = f"http://{os.getenv(f'SBR_WEBDRIVER_AUTH1')}@95.217.141.220:9515"

    processes: List[mp.Process] = []

    try:
        logging.info("Tesco category scraper running...")

        process_count = 4
        categories = get_categories()[:4]
        unit = math.floor(len(categories) / process_count)

        try:
            sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, "goog", "chrome")
        except Exception as e:
            logging.error(f"Scraping Browser connection failed")
            raise e

        processes = [
            mp.Process(
                target=CategoryScraper(
                    categories[unit * i : unit * (i + 1)], sbr_connection
                ).run
            )
            for i in range(process_count)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join()

        logging.info("Tesco category scraper finished")

    except KeyboardInterrupt:
        logging.info("Quitting...")
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")
    finally:
        for process in processes:
            process.terminate()


if __name__ == "__main__":
    run_category_scraper()
