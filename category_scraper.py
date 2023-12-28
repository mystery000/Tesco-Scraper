import os
import sys
import csv
import math
import time
import random
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from bs4 import BeautifulSoup
from selenium import webdriver
from config import SELENIUM_SERVERS
from selenium.webdriver import Remote
from selenium.webdriver.firefox.remote_connection import FirefoxRemoteConnection

BASE_URL = "https://www.tesco.com"

def get_categories(sbr_connection: FirefoxRemoteConnection) -> List[str]:
    categories: List[str] = []
    firefox_options = webdriver.FirefoxOptions()
    firefox_options.add_argument("--start-maximized")
    try:
        with Remote(sbr_connection, options=firefox_options) as driver:
            driver.get(f"https://www.tesco.com/groceries/?icid=dchp_groceriesshopgroceries")
            html = driver.page_source
            page = BeautifulSoup(html, "html5lib")
            categories = [f"{BASE_URL}{category.a['href'].replace('?', '/all?', 1)}" for category in page.find_all('li', class_="menu__item--superdepartment")]
    except:
        pass
    
    return categories
    
class CategoryScraper:
    _categories: List[str]
    _sbr_webdriver_connection: FirefoxRemoteConnection

    def __init__(
        self,
        categories: List[str],
        sbr_webdriver_connection: FirefoxRemoteConnection,
    ):
        self._categories = categories
        self._sbr_webdriver_connection = sbr_webdriver_connection

    def get_products_by_category(self, category: str) -> List[str]:
        products: List[str] = []

        firefox_options = webdriver.FirefoxOptions()
        firefox_options.add_argument("--start-maximized")
        
        logging.info(category)

        try:
            with Remote(self._sbr_webdriver_connection, options=firefox_options) as driver:
                driver.get(f"{category}&page=1")
                html = driver.page_source
                page = BeautifulSoup(html, "html5lib")

                last_page_number = int(
                    page.find_all("li", class_="pagination-btn-holder")[
                        -2
                    ].span.get_text()
                )
                
            for page_no in range(0, last_page_number):
                try:
                    time.sleep(random.choice([0,2, 0.25, 0.3]))
                    with Remote(self._sbr_webdriver_connection, options=firefox_options) as driver:
                        driver.get(f"{category}&page={page_no + 1}")
                        html = driver.page_source
                        page = BeautifulSoup(html, "html5lib")
                        elements = page.select('.product-list--list-item .product-image-wrapper')
                        products += [
                            f"{BASE_URL}{element['href']}" for element in elements
                        ]
                except:
                    continue

        except Exception as e:
            logging.info(f"Exception: {str(e)}")
            
        finally:
            return products

    def run(self):
        for category in self._categories:
            product_links = self.get_products_by_category(category)
            csv_file_name = "tesco_product_links.csv"
            with open(csv_file_name, "a", newline="") as csv_file:
                fieldnames = ["Link"]
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                if csv_file.tell() == 0:
                    writer.writeheader()
                for product in product_links:
                    writer.writerow({"Link": product})
                    

def run_category_scraper():
    csv_file_name = "tesco_product_links.csv"
    if os.path.exists(csv_file_name):
        os.remove(csv_file_name)

    logging.basicConfig(
        format="[%(asctime)s] %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    processes: List[mp.Process] = []

    try:
        logging.info("Tesco category scraper running...")

        sbr_connections = [FirefoxRemoteConnection(SELENIUM_SERVER, "mozilla", "firefox") for SELENIUM_SERVER in SELENIUM_SERVERS]

        process_count = len(SELENIUM_SERVERS) * 2 # Assign two browser sessions per Grid server
        categories = get_categories(sbr_connections[0])
        unit = math.floor(len(categories) / process_count)

        processes = [
            mp.Process(target=CategoryScraper(categories[unit * i : ], sbr_connections[i % len(SELENIUM_SERVERS)]).run)
            if i == process_count - 1
            else mp.Process(target=CategoryScraper(categories[unit * i : unit * (i + 1)], sbr_connections[i % len(SELENIUM_SERVERS)]).run)
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
        for process in processes: process.terminate()


if __name__ == "__main__":
    run_category_scraper()
