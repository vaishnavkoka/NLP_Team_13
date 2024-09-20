# Add progress and error logging for better visibility

import os
import re
import pandas as pd
import aiohttp
import asyncio
import time
import logging
import sys
import uuid  # To generate new UUIDs
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import aiofiles
from urllib.parse import urljoin

# Set up directory as before
directory = sys.argv[0].split('.')[-2].replace('\\','')
path = directory
for i in range(1, 10000):
    path = directory + '-' + str(i)
    if not os.path.exists(path):
        os.mkdir(path)
        print(path)
        break
os.chdir(path)
os.mkdir('urls')
# Setup logging for scraper progress and errors
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler('error_log.log')
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

scraper_logger = logging.getLogger('scraper_logger')
scraper_handler = logging.FileHandler('scraper.log')
scraper_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
scraper_handler.setFormatter(scraper_formatter)
scraper_logger.addHandler(scraper_handler)
scraper_logger.setLevel(logging.INFO)

# global variables
updated_urls = pd.DataFrame(columns=["UUID", "Domain", "URL"])  # Initialize DataFrame to track new and old URLs
total_data_size = 0  # Initialize total data size
max_data_size = 150 * 1024 * 1024 * 1024  # 150 GB data to scrap limit
all_urls = set()  # all urls which are scrapped till now
all_urls_lock = asyncio.Lock()  # lock for all url
errors = []  # list of error used to write to error files
errors_lock = asyncio.Lock()  # error lock

# Global queues and session
url_queue = asyncio.Queue(maxsize=500000)
session = None

# Function to check if content is primarily Hindi
def is_hindi(text):
    hindi_chars = re.findall(r'[\u0900-\u097F]', text)
    return len(hindi_chars) / len(text) if len(text) > 0 else 0

# Function to get all <p> tag content and clean text
def get_clean_text(soup):
    body = soup.find('body')
    if body is None:
        return ""
    text = ""
    # Remove contents of <script> and <style> tags
    for script_or_style in body.find_all(['script', 'style']):
        script_or_style.decompose()

    # Extract text from meaningful tags
    for tag in body.find_all(True):
        temp = tag.get_text(separator=' ', strip=True) + ' '
        if is_hindi(temp) > 0.5:
            text += temp

    text = re.sub(r'\s+', ' ', text)
    hindi_percentage = is_hindi(text)
    return text if hindi_percentage >= 0.4 else ""

def get_soup(content):
    # Check if the content seems to be XML
    if content.strip().startswith('<?xml') or content.strip().startswith('<'):
        try:
            # Try parsing as XML
            return BeautifulSoup(content, 'lxml-xml')
        except Exception as e:
            print(f"Error parsing as XML: {e}")
    # Fallback to HTML parser
    return BeautifulSoup(content, 'lxml')

# Function to get all URLs on the page
def get_all_urls(soup, base_url):
    urls = set()
    links = soup.find_all('a', href=True)
    for link in links:
        url = link['href']
        # Convert relative URLs to absolute URLs
        absolute_url = urljoin(base_url, url)
        # Ensure the URL starts with http or https
        if absolute_url.startswith(('http://', 'https://')):
            urls.add(absolute_url)
    return urls

# Updated scrape_page to generate new UUID for each new URL
async def scrape_page():
    global total_data_size
    global all_urls
    global errors
    global session
    global updated_urls

    if total_data_size >= max_data_size:
        scraper_logger.info("Reached max data size. Stopping...")
        url_queue.task_done()
        return

    while True:
        try:
            url, domain = await asyncio.wait_for(url_queue.get(), timeout=60)
        except asyncio.TimeoutError:
            error_logger.warning('Scrape page timeout error.')
            break
        async with all_urls_lock:
            if url in all_urls:
                url_queue.task_done()
                continue
            all_urls.add(url)            

        try:
            async with session.get(url, ssl=False, timeout=30) as response:
                if response.status != 200:
                    error_logger.warning(f"Failed to retrieve {url}. Status code: {response.status}")
                    url_queue.task_done()
                    continue

                content = await response.text()
                soup = get_soup(content)

                # Get clean Hindi content
                text = get_clean_text(soup)
                hindi_percentage = is_hindi(text)

                if len(text) > 200:
                    # Generate a new UUID
                    uuid_ = str(uuid.uuid4())
                    file_name = f"{uuid_}.txt"

                    # Save the scraped content with the UUID filename
                    with open(file_name, 'w', encoding='utf-8') as f:
                        f.write(text)
                    with open('urls/updated_urls.csv','a') as f:
                        f.write(','.join([uuid_,domain,url]))

                    data_size = os.path.getsize(file_name)
                    total_data_size += data_size
                    scraper_logger.info(f"Stored {file_name}, size: {data_size} bytes")
                    scraper_logger.info(f"Total data size so far: {total_data_size / (1024 * 1024)} MB")

                    # put urls in url_queue
                    new_urls = list(get_all_urls(soup, domain))
                    async with all_urls_lock:
                        for new_url in new_urls:
                            if new_url not in all_urls:
                                await url_queue.put((new_url, domain))

        except (aiohttp.ClientError and Exception) as e:
            error_logger.error(e)

        url_queue.task_done()

async def main(urls):
    global session

    start_time = time.time()

    # Limit concurrent requests and manage connection pooling
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as sess:
        session = sess

        # Add initial URLs to the queue
        for domain, url in urls:
            await url_queue.put((url, domain))

        tasks = set()
        max_workers = 1000

        # Function to calculate the number of workers
        def get_worker_count(queue_size):
            return min(max_workers, max(1, queue_size // 2))

        # Start initial workers
        while not url_queue.empty() and len(tasks) < get_worker_count(url_queue.qsize()):
            task = asyncio.ensure_future(scrape_page())
            tasks.add(task)

        while not url_queue.empty() or any(task for task in tasks if not task.done()):
            current_queue_size = url_queue.qsize()
            current_worker_count = get_worker_count(current_queue_size)

            # Adjust the number of workers if needed
            if len(tasks) < current_worker_count and not url_queue.empty():
                task = asyncio.ensure_future(scrape_page())
                tasks.add(task)

            done, pending = await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED)
            tasks = pending

            # Log the current queue size and the number of active tasks
            scraper_logger.info(f"Queue size: {current_queue_size}, Active tasks: {len(tasks)}")

            # Check if the total data size limit has been reached
            if total_data_size >= max_data_size:
                scraper_logger.info(f"Data size limit reached: {total_data_size / (1024 * 1024)} MB. Stopping...")
                break

        # Cancel remaining tasks
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    elapsed_time = time.time() - start_time
    scraper_logger.info(f"Scraping completed in {elapsed_time / 60:.2f} minutes.")


# Run the async main function with the list of initial URLs
if __name__ == "__main__":
    df = pd.read_csv('../input_domain_url.csv', header=None, names=['Domain','URL']).drop_duplicates(subset='URL', keep='first')
    urls = df.values.tolist()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(urls))

