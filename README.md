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
- The script uses 200 workers by default (this number can be adjusted) and a pool of 50 TCP connections for faster scraping.
- It stores all the unique URLs in a text file.

### 3. Scrape Web Pages for Hindi Content
**Script**: `NLP_Team_13/scripts/scraping.py`

- This step is similar to the previous one but focuses on downloading and storing web pages that meet a predefined threshold of Hindi content.
- The script uses 200 workers by default (this number can be adjusted) and a pool of 50 TCP connections for faster scraping.
- The scraped files are saved to disk.

### 4. Data Cleaning
**Script**: `badwords_remover.py`

- It takes input file of bad words and sequentially eliminates bad words (abuse, pornographic, present in data files.
- As most of our data got scrapped from good sources, we didn't suffer any significant reduction in data due to it.
The initial dataset size before cleaning was 249.800 GB; after cleanup, it came down to 249.440 GB, which shows a reduction of 360MB of data
- ![image](https://github.com/user-attachments/assets/df9c726b-1619-4949-9402-88f646870f61)


### 5. Deduplicate the Dataset
**Script**: `NLP_Team_13/scripts/deduplication.py (MinHash + MinHashLSH)`

- It takes the path of the folder containing the scraped files and generates a `unique_files.txt,` which lists the absolute paths of all unique files after deduplication.
- This script takes some time to complete (~100 minutes for ~250 GB data, i.e., ~775000 files), so wait for it to finish; there are progress bars added so that we can track the progress of this work.
- ![image](https://github.com/user-attachments/assets/76b53016-39ab-466e-9155-d33543acf187)

### 6. Get Data Sources
**Script**: `get_data_sources.py`

- It takes the path of the URL CSV file, which contains file name, domain, URL
- It also takes the path of the folder where data is stored (it can be raw data or after preprocessing because the file name is not changed)
- It creates 2 files
  - CSV file: it contains 3 columns, domain, no. of articles, and total size scraped from this domain.
  - PDF file: it is the same as a CSV file, just in the form of a table for readability
  - Both PDF and CSV are created at the same location of the URL file
- ![image](https://github.com/user-attachments/assets/e49ad47e-ae5c-4690-b294-210084d19e57)


## Usage
1. **Setup: Run NLP_Team_13/scripts/requirements.txt to install the required dependencies.**
2. Supported Python version is 3.8 or above `you may need to make changes to run on older versions.`
3. Run the scripts sequentially in the order mentioned above.
4. Adjust the number of workers or connection pools if needed for better performance.

## Output
- **Initial URLs**: Extracted URLs for each keyword.
- **Unique URLs**: Additional URLs scraped from the initial list.
- **Scraped Files**: Text files stored locally, filtered by Hindi content.
- **Deduplicated Files**: Paths of all unique files post-deduplication.

![image](https://github.com/user-attachments/assets/15b23d0c-3c65-4cb6-83ba-5fe54fa34217)


## Acknowledgement
- Contributions:
  - **Isha Jain** collected data from existing datasets **|** code for data scraping pipeline **|** proposed parallelization in the data scrapping
  - **Keshav Krishna** collected bad words **|** ideate the initial data scraping methodology **|** code for data-cleanup pipeline.
  - **Vaishnav Koka** collected data from existing datasets **|** documented code flow in the readme file **|** code for deduplication pipeline for exact document match and SimHash.
  - **Yash Sahu** collected bad words **|** code for data-cleanup pipeline **|** code for SHA256 deduplication
  - **Ramanand** documented code flow in the readme file **|** code for data scraping pipeline **|** Source-Data mapping *(result.pdf)* **|** MinHash deduplication pipeline.
- Our team was able to parallelize the data scraping part and add comments *(only some parts)* to the code with the help of ChatGPT

