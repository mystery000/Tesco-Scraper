import os
import csv
import sys
import time
import math
import pandas
import random
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import Remote
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection

def get_product_page_links() -> List[str]:
    csv_file_name = "tesco_product_links.csv"
    links: List[str] = []
    
    try:
        if os.path.exists(csv_file_name):
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
    
    for link in links:
        time.sleep(random.choice([0.15, 0.2, 0.22, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]))
        try:
            with Remote(sbr_connection, options=chrome_options) as driver:
                driver.get(link)
                html = driver.page_source
                page = BeautifulSoup(html, "html5lib")
                
                with open("tesco_products.csv", 'a', newline='') as csv_file:
                    fieldnames = [
                    'source',
                    'title', 
                    'description',
                    'item_price',
                    'unit_price',
                    'offer_price',
                    'offer_from',
                    'offer_to',
                    'average_rating',
                    'review_count',
                    'categories',
                    'tags',
                    'nutrition',
                    'product_url',
                    'image_url',
                    'last_updated' ]
                    
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    
                    if csv_file.tell() == 0:
                        writer.writeheader()

                    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    
                    source = "Tesco"
                    
                    parent = page.find('div', class_="template-wrapper")
                    
                    title_element = parent.find('section', {'name': 'title'})
                    title = title_element.h1.get_text() if title_element and title_element.h1 else None
                    
                    try:
                        price_element = title_element.find('div', {'data-auto': 'pdp-buy-box'})
                        price = price_element.find_all('p')
                        item_price = price[0].get_text() if price[0] else None
                        unit_price = price[1].get_text() if price[1] else None
                    except:
                        item_price = unit_price = None
                    
                    try:
                        offer_element = title_element.find('div', {'data-testid': 'value-bar'})
                        clubcard = offer_element.find_all('span')[-1].get_text(strip=True)
                        offer_message = offer_element.find("p").get_text(strip=True)
                        from_idx = offer_message.index("from")
                        until_idx = offer_message.index("until")
                        offer_price = clubcard.replace("Clubcard Price", "").strip()
                        offer_from = offer_message[from_idx + len("from") : until_idx].strip()
                        offer_to = offer_message[until_idx + len("until"):].strip()
                    except:
                        offer_price = offer_from = offer_to = None
                        
                    try:
                        rating_element = title_element.find('a')
                        average_rating = rating_element.find_all('span')[0].get_text() if len(rating_element.find_all('span')) else None
                        review_count = int(rating_element.find_all('span')[1].get_text()[1:-1]) if len(rating_element.find_all('span')) else None
                    except:
                        average_rating = 0
                        review_count = 0
                    
                    breadcrumbs_element = parent.find("div", {"data-auto": "pdp-breadcrumbs"})
                    categories = " / ".join([category.get_text(strip=True) for category in breadcrumbs_element.find_all("li")][1:] if breadcrumbs_element else [])
                    
                    tag_element = parent.find('div', class_="styled__DietaryTagsContainer-mfe-pdp__sc-1wwtd31-0")
                    tags = ",".join([span.get_text() for span in tag_element.find_all('span')] if tag_element else [])
                    
                    try:
                        image_element = parent.find('section', {'name': 'image'})
                        image_url = image_element.img['src'] if image_element and image_element.img else None
                    except:
                        image_url = None
                    
                    desc_element = parent.find('div', {'id': 'accordion-panel-product-description'})
                    description = desc_element.get_text() if desc_element else None
                    
                    nutritions = { "values": [] }
                    
                    try:
                      nutritional_information = parent.find("div", {"id" : "accordion-panel-nutritional-information"})
                      nutrition_titles = [title.get_text(strip=True) for title in nutritional_information.thead.find_all("th")]
                      nutrition_rows = nutritional_information.tbody.find_all("tr")

                      for _id, nutrition_title in enumerate(nutrition_titles):
                        if _id == 0 : continue
                        nutrition = { "unit" : nutrition_title }
                        for row in nutrition_rows:
                            nutrition_cells = list(row.children)
                            typical_value = nutrition_cells[0].get_text(strip=True)
                            if typical_value.lower().find("which") >= 0: continue
                            nutrition[typical_value] = nutrition_cells[_id].get_text(strip=True)
                        
                        nutritions["values"].append(nutrition)
                    except:
                      nutritions = { "values": [] }
                      
                    logging.info({
                        'source': source,
                        'title': title, 
                        'description': description,
                        'item_price': item_price,
                        'unit_price': unit_price,
                        'offer_price': offer_price,
                        'offer_from': offer_from,
                        'offer_to': offer_to,
                        'average_rating': average_rating,
                        'review_count': review_count,
                        'categories': categories,
                        'tags': tags,
                        'nutrition': nutritions,
                        'product_url': link,
                        'image_url': image_url,
                        'last_updated': now,
                        })
                    
                    writer.writerow({
                        'source': source,
                        'title': title, 
                        'description': description,
                        'item_price': item_price,
                        'unit_price': unit_price,
                        'offer_price': offer_price,
                        'offer_from': offer_from,
                        'offer_to': offer_to,
                        'average_rating': average_rating,
                        'review_count': review_count,
                        'categories': categories,
                        'tags': tags,
                        'nutrition': nutritions,
                        'product_url': link,
                        'image_url': image_url,
                        'last_updated': now,
                        })
                    
        except Exception as e:
            logging.info(f"Exception: {str(e)} at {link}")
            

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

        process_count = 6
        product_page_links = get_product_page_links()
        unit = math.floor(len(product_page_links) / process_count)
        
        SELENIUM_GRID_IP_ADDRESSES = [
            "65.21.77.95:9515",
            "65.21.123.13:9515",
            "65.21.194.207:9515",
        ]
        
        sbr_connections = [ChromiumRemoteConnection(f"http://{IP}", "goog", "chrome") for IP in SELENIUM_GRID_IP_ADDRESSES]

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
        logging.info("Tesco product scraper finished")


if __name__ == "__main__":
    run_product_scraper()
