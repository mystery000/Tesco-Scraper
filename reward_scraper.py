import sys
import math
import time
import random
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import Remote
from selenium.webdriver.common.by import By
from product_scraper import get_product_details
from selenium.webdriver.support.wait import WebDriverWait
from product_scraper import get_product_page_links_from_csv
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.remote_connection import FirefoxRemoteConnection

BASE_URL = "https://www.tesco.com"

class RewardCategoryScraper:
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
            current_url = ""
            
            with Remote(self._sbr_webdriver_connection, options=firefox_options) as driver:
                driver.get(category)
                html = driver.page_source
                page = BeautifulSoup(html, "html5lib")
                
                try:
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "pagination-btn-holder")))
                except:
                    return products
                
                last_page_number = int(
                    page.find_all("li", class_="pagination-btn-holder")[
                        -2
                    ].span.get_text()
                )
                
                elements = page.select('.product-list--list-item .product-image-wrapper')
                
                products += [
                    f"{BASE_URL}{element['href']}" for element in elements
                ]
                
                if last_page_number == 1: return products
                
                current_url = driver.current_url
                
            for page_no in range(1, last_page_number):
                time.sleep(random.choice([0,2, 0.25, 0.3]))
                try:
                    with Remote(self._sbr_webdriver_connection, options=firefox_options) as driver:
                        driver.get(f"{current_url}?page={page_no + 1}")
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "pagination-btn-holder")))
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
        
    def get_all_products_by_categories(self) -> List[str]:
        products: List[str] = []
        for category in self._categories:
            products.extend(self.get_products_by_category(category))
        return products


def run_reward_scraper():

    logging.basicConfig(
        format="[%(asctime)s] %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    processes: List[mp.Process] = []

    try:
        logging.info("Tesco Reward scraper running...")
        
        SELENIUM_GRID_IP_ADDRESSES = [
            "65.21.77.95:9515",
            "65.21.123.13:9515",
            "65.21.194.207:9515",
        ]
        
        sbr_connections = [FirefoxRemoteConnection(f"http://{IP}", "mozilla", "firefox") for IP in SELENIUM_GRID_IP_ADDRESSES]

        process_count = 6
        reward_links = get_product_page_links_from_csv("tesco_reward.csv")
        product_page_links = RewardCategoryScraper(reward_links, sbr_connections[0]).get_all_products_by_categories()
        
        unit = math.floor(len(product_page_links) / process_count)
        
        processes = [
            mp.Process(target=get_product_details, args=[product_page_links[unit * i : ], sbr_connections[i % len(SELENIUM_GRID_IP_ADDRESSES)]])
            if i == process_count - 1
            else mp.Process(target=get_product_details, args=[product_page_links[unit * i : unit * (i + 1)], sbr_connections[i % len(SELENIUM_GRID_IP_ADDRESSES)]])
            for i in range(process_count)
        ]

        for process in processes:
            process.start()
            
        for process in processes:
            process.join()

    except KeyboardInterrupt:
        logging.info("Quitting...")
        
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")
        
    finally:
        for process in processes: process.terminate()
        logging.info("Tesco Reward Scraper Finished")

if __name__ == "__main__":
    run_reward_scraper()
