import sys
import math
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from category_scraper import CategoryScraper   
from product_scraper import get_product_details
from product_scraper import get_product_page_links_from_csv
from selenium.webdriver.firefox.remote_connection import FirefoxRemoteConnection

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
        product_page_links = CategoryScraper(reward_links, sbr_connections[0]).get_all_products_by_categories()
        
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
