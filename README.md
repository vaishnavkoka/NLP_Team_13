# NLP_Team_13: Data Preparation and De-duplication

## Overview
NLP_Team_13 is a project focused on web scraping, data preparation, and de-duplication of Hindi text content. The scripts in this repository automate the process of collecting URLs based on a list of Hindi keywords, scraping web pages, and efficiently deduplicating the collected text files.

## Steps to Execute

### 1. Get Initial Hindi URLs with Selenium
**Script**: `NLP_Team_13/scripts/get-initial-hindi-urls-with-selenium.py`

- This script searches for Hindi keywords (provided in the script) using a headless browser controlled by Selenium.
- For each keyword, it crawls 15 pages to extract URLs.
- All URLs are saved to a specified output file.

### 2. Scrape URLs from Initial URLs
**Script**: `NLP_Team_13/scripts/scrap-urls-from-initial-urls.py`

- This script takes the output from the previous step and scrapes additional URLs using the `requests` module.
- It processes the HTML responses and extracts all unique URLs.
- The script uses 1000 workers by default (this number can be adjusted) and a pool of 50 TCP connections for faster scraping.
- It stores all the unique URLs in a text file.

### 3. Scrape Web Pages for Hindi Content
**Script**: `NLP_Team_13/scripts/scraping.py`

- This step is similar to the previous one but focuses on downloading and storing web pages that meet a predefined threshold of Hindi content.
- The script uses 1000 workers by default (this number can be adjusted) and a pool of 50 TCP connections for faster scraping.
- The scraped files are saved to disk.

### 4. Deduplicate the Dataset
**Script**: `NLP_Team_13/scripts/deduplication.py`

- It takes the path of the folder containing the scraped files and generates a `unique_files.txt`, which lists the absolute paths of all unique files after deduplication.
- This script takes a few seconds (~10 seconds for ~11 GB data i.e. ~14000 files), so wait for it to finish even if you don't see any activity.

## Usage
1. **Setup: Run NLP_Team_13/scripts/requirements.txt to install the required dependencies.**
2. Supported python version is 3.8 or above `you may need to make changes to run on older versions`
3. Run the scripts sequentially in the order mentioned above.
4. Adjust the number of workers or connection pools if needed for better performance.

## Output
- **Initial URLs**: Extracted URLs for each keyword.
- **Unique URLs**: Additional URLs scraped from the initial list.
- **Scraped Files**: Text files stored locally, filtered by Hindi content.
- **Deduplicated Files**: Paths of all unique files post-deduplication.

![image](https://github.com/user-attachments/assets/43a2750f-999e-4268-852f-900ff65c5881)
