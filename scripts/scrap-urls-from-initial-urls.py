import os
import re
import aiohttp
import asyncio
import time
import pandas as pd
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

# Initialize a set to keep track of stored URLs and a queue for batch writing
stored_urls = set()
stored_urls_lock = asyncio.Lock()
batch_urls = asyncio.Queue()
batch_size = 1000

# Global queue and session
url_queue = asyncio.Queue()
session = None

# Function to check if content is primarily Hindi
def is_hindi(text):
    hindi_chars = re.findall(r'[\u0900-\u097F]', text)
    return len(hindi_chars) / len(text) if len(text) > 0 else 0

# Function to get all <p> tag content and clean text
def get_clean_text(soup):
    body = soup.find('body')
    if not body:
        return ""

    # Remove <script> and <style> tags
    for script_or_style in body(['script', 'style']):
        script_or_style.decompose()

    # Extract text from all remaining tags
    text = body.get_text(separator=' ', strip=True)
    
    # Clean up extra spaces and newlines
    text = re.sub(r'\s+', ' ', text)

    hindi_percentage = is_hindi(text)
    return text if hindi_percentage >= 0.1 else ""

def get_soup(content):
    # Check if the content seems to be XML
    if content.strip().startswith('<?xml') or content.strip().startswith('<'):
        try:
            # Try parsing as XML
            return BeautifulSoup(content, 'lxml-xml')
        except Exception:
            pass
    # Fallback to HTML parser
    return BeautifulSoup(content, 'lxml')

def is_same_domain(url, base_domain):
    parsed_url = urlparse(url)
    parsed_base_domain = urlparse(base_domain).netloc or base_domain
    return parsed_url.netloc.endswith(parsed_base_domain)

# Function to get all URLs on the page
def get_all_urls(soup, base_domain):
    urls = set()
    base_url = f"http://{base_domain}"
    
    # Extract all <a> tags with href attribute
    for link in soup.find_all('a', href=True):
        href = link['href']
        # Construct full URL
        full_url = urljoin(base_url, href)
        # Check if the URL is valid and ensure it is from the same domain
        if full_url.startswith('http') and is_same_domain(full_url, base_url):
            urls.add(full_url)

    return urls

# Asynchronous function to scrape a page
async def scrape_page():
    global session
    global stored_urls
    global batch_urls

    if url_queue.empty():
        return

    while True:
        try:
            url, domain = await asyncio.wait_for(url_queue.get(), timeout=60)  # Wait for URL with a timeout
        except asyncio.TimeoutError:
            break

        async with stored_urls_lock:
            if url in stored_urls:
                url_queue.task_done()
                continue
            stored_urls.add(url)
            if len(batch_urls._queue) < batch_size:
                await batch_urls.put(url)

        try:
            async with session.get(url, ssl=False, timeout=30) as response:
                if response.status != 200:
                    url_queue.task_done()
                    continue

                content = await response.text()
                soup = get_soup(content)

                # Get clean Hindi content
                text = get_clean_text(soup)
                hindi_percentage = is_hindi(text)

                if hindi_percentage > 0.1:
                    # Extract all URLs from the same domain and scrape them recursively
                    new_urls = list(get_all_urls(soup, domain))
                    for new_url in new_urls:
                        if new_url not in stored_urls:
                            await url_queue.put((new_url, domain))

        except (aiohttp.ClientError, Exception):
            pass

        url_queue.task_done()

    # Write remaining URLs to the batch queue if the queue is not empty
    async with stored_urls_lock:
        if not batch_urls.empty():
            await batch_urls.put(None)  # Sentinel value to indicate end of processing

# Background task for writing URLs to file
async def write_batches():
    async with aiofiles.open("stored_urls_more.txt", 'a', encoding='utf-8') as file:
        while True:
            batch = await batch_urls.get()
            if batch is None:
                break
            await file.write(batch + "\n")

async def main(urls):
    global session

    start_time = time.time()

    # Limit concurrent requests and manage connection pooling
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as sess:
        session = sess

        # Add initial URLs to the queue
        for url, domain in urls:
            await url_queue.put((url, domain))

        # Start scraping tasks
        tasks = []
        max_workers = 1000
        while not url_queue.empty() and len(tasks) < max_workers:
            tasks.append(asyncio.ensure_future(scrape_page()))

        # Start the background writing task
        writer_task = asyncio.ensure_future(write_batches())

        # Dynamically adjust worker count based on queue size
        while not url_queue.empty() or any(task for task in tasks if not task.done()):
            if len(tasks) < max_workers and not url_queue.empty():
                tasks.append(asyncio.ensure_future(scrape_page()))

            done, pending = await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED)
            tasks = list(pending)

        # Cancel remaining tasks
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        await writer_task

    print("Scraping completed.")
    end_time = time.time()
    print('Total time in seconds: ', end_time - start_time)


# df = pd.read_csv('urls.txt', header=None, names=['URL', 'Domain']).drop_duplicates(subset='URL', keep='first')
# urls = df.values.tolist()
files=['./hindi_urls.txt','./hindi_urls_more.txt','./hindi_urls_more-2.txt']
urls = set()
for file in files:
    with open(file,'r') as f:
        for line in f:
            urls.add(line.strip())

print(len(urls))
urls_with_domain = []
for url in urls:
    if 'http' in url[:5]:
        domain_words = urlparse(url).netloc.split('.')
        domain = domain_words[-2]+'.'+domain_words[-1]
        urls_with_domain.append([url,domain])

# Custom event loop for Python 3.6 compatibility
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(urls_with_domain))
