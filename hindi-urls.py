from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from urllib.parse import quote

# List of Hindi keywords
keywords = [
    "हिंदी लघु कहानियाँ", "हिंदी बाल साहित्य", "हिंदी लोककथाएँ", "हिंदी साहित्य समीक्षा", "हिंदी रहस्य उपन्यास",
    "हिंदी विज्ञान कथाएँ", "हिंदी ऐतिहासिक कहानियाँ", "हिंदी राजनीति समाचार", "हिंदी आर्थिक समाचार", "हिंदी खेल समाचार",
    "हिंदी मौसम समाचार", "हिंदी ताज़ा खबरें", "हिंदी स्थानीय समाचार", "हिंदी अंतरराष्ट्रीय समाचार", "हिंदी सामान्य ज्ञान",
    "हिंदी विज्ञान प्रश्नोत्तरी", "हिंदी गणित प्रश्नोत्तरी", "हिंदी ऐतिहासिक तथ्य", "हिंदी सामाजिक अध्ययन", "हिंदी शिक्षा जगत",
    "हिंदी शैक्षिक लेख", "हिंदी कविता संग्रह", "हिंदी ग़ज़ल", "हिंदी दोहे", "हिंदी शेरो शायरी", "हिंदी भजन",
    "हिंदी सुभाषित", "हिंदी प्रेम शायरी", "हिंदी फिल्म समीक्षा", "हिंदी टीवी धारावाहिक समीक्षा", "हिंदी संगीत समीक्षा",
    "हिंदी वेब सीरीज़ समीक्षा", "हिंदी सिनेमा समाचार", "हिंदी मनोरंजन जगत", "हिंदी योग और ध्यान", "हिंदी फिटनेस टिप्स",
    "हिंदी घरेलू नुस्खे", "हिंदी आयुर्वेदिक उपचार", "हिंदी मानसिक स्वास्थ्य", "हिंदी खानपान और पोषण", "हिंदी जीवनशैली",
    "हिंदी पर्यटन स्थल", "हिंदी धार्मिक यात्रा", "हिंदी संस्कृति और सभ्यता", "हिंदी परंपराएँ और त्योहार", "हिंदी ऐतिहासिक स्थल",
    "हिंदी यात्रा अनुभव", "हिंदी प्रौद्योगिकी समाचार", "हिंदी विज्ञान लेख", "हिंदी नवीनतम गैजेट्स", "हिंदी आर्टिफिशियल इंटेलिजेंस",
    "हिंदी रोबोटिक्स", "हिंदी इंटरनेट और सोशल मीडिया", "हिंदी हास्य", "हिंदी प्रेरणादायक कहानियाँ", "हिंदी सुविचार",
    "हिंदी शैक्षिक लेख", "हिंदी धार्मिक लेख", "हिंदी प्रेरक उद्धरण", "हिंदी प्रेरणात्मक लेख"
]

# File to store URLs
output_file = 'hindi_urls_selenium.txt'

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)
chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration

# Setup WebDriver
driver = webdriver.Chrome()

def fetch_urls_for_keyword(keyword):
    all_urls = set()
    encoded_keyword = quote(keyword)
    print(keyword)
    for page in range(15):  # Loop through 15 pages
        start = page * 10
        url = f"https://www.bing.com/search?q={encoded_keyword}&first={start}"
        print(f"Fetching URL: {url}")
        try:
            driver.get(url)
        except Exception as e:
            print(e)
            continue
        time.sleep(2)
        try:
            # Wait for results to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'li.b_algo'))
            )
            
            # Find all result anchors
            anchors = driver.find_elements(By.XPATH, "//h2/a")
            for a in anchors:
                href = a.get_attribute('href')
                if href and href.startswith('http'):
                    print(href)
                    all_urls.add(href)
                    
            # Wait for a short time to avoid hitting the server too quickly
            time.sleep(2)
        except Exception as e:
            print(f"An error occurred: {e}")
    
    return all_urls

# Main function to handle keyword search and saving results
def main():
    all_urls = set()
    
    for keyword in keywords:
        print(f"Processing keyword: {keyword}")
        urls = fetch_urls_for_keyword(keyword)
        all_urls.update(urls)
        print(f"Number of URLs found for keyword '{keyword}': {len(urls)}")
    
    # Save URLs to the file
    with open(output_file, 'a', encoding='utf-8') as f:
        for url in all_urls:
            f.write(url + '\n')
    
    print(f"Scraping complete. URLs stored in '{output_file}'.")

if __name__ == "__main__":
    main()
    driver.quit()
