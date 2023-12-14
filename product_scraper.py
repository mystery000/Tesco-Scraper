import os
import csv
import sys
import math
import pandas
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from dotenv import load_dotenv
from selenium.webdriver import Remote
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection

load_dotenv()

def get_product_page_links() -> List[str]:
    csv_file_name = "Tesco_Products_Links.csv"
    links: List[str] = []
    try:
        if os.path.exists(csv_file_name):
            csv_file_name = "Tesco_Products_Links.csv"
            products = pandas.read_csv(csv_file_name)
            products.drop_duplicates(subset="Link", inplace=True)
            links.extend(products["Link"].values.tolist())
    except pandas.errors.EmptyDataError as e:
        logging.error(f"Error: {str(e)}")
    finally:
        return links


def get_product_details(links: List[str], sbr_connection: ChromiumRemoteConnection):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    try:
        for link in links:
            with Remote(sbr_connection, options=chrome_options) as driver:
                driver.get(link)
                html = driver.page_source
                page = BeautifulSoup(html, "html5lib")
                
                with open("tesco_products.csv", 'a', newline='') as csv_file:
                    fieldnames = [
                    'title', 
                    'description',
                    'unit_price',
                    'product_url',
                    'image_url',
                    'last_updated' ]
                    
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    
                    if csv_file.tell() == 0:
                        writer.writeheader()

                    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    
                    title_element = page.find('section', {'name': 'title'})
                    title = title_element.h1.get_text() if title_element and title_element.h1 else ''
                    
                    unit_price = title_element.find('p').get_text() if title_element and title_element.find('p') else ''
                    
                    image_element = page.find('section', {'name': 'image'})
                    image_url = image_element.img['src'] if image_element and image_element.img else ''
                    
                    desc_element = page.find('div', {'id': 'accordion-panel-product-description'})
                    description = desc_element.get_text() if desc_element else ''
                    
                    logging.info({
                        'title': title, 
                        'description': description,
                        'unit_price': unit_price,
                        'product_url': link,
                        'image_url': image_url,
                        'last_updated': now,
                        })
                    
                    writer.writerow({
                        'title': title, 
                        'description': description,
                        'unit_price': unit_price,
                        'product_url': link,
                        'image_url': image_url,
                        'last_updated': now,
                        })

    except Exception as e:
        logging.info(f"Exception: {str(e)}")


def run_product_scraper():

    logging.basicConfig(
        format="[%(asctime)s] %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    processes: List[mp.Process] = []

    try:
        logging.info("Tesco product scraper running...")

        csv_file_name = "tesco_products.csv"
        if os.path.exists(csv_file_name):
            os.remove(csv_file_name)

        process_count = 3
        product_page_links = get_product_page_links()
        unit = math.floor(len(product_page_links) / process_count)
        
        SBR_WEBDRIVER = f"http://{os.getenv(f'SBR_WEBDRIVER_AUTH1')}@95.217.141.220:9515"
        
        try:
            sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, "goog", "chrome")
        except Exception as e:
            logging.error(f"Scraping Browser connection failed")
            raise e

        processes = [
            mp.Process(
                target=get_product_details,
                args=[product_page_links[unit * i : unit * (i + 1)], sbr_connection],
            )
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
        for process in processes:
            process.terminate()
        logging.info("Tesco product scraper finished")


if __name__ == "__main__":
    run_product_scraper()
