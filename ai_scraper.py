import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import time
import json
from datetime import datetime
from selenium_stealth import stealth

# Create logs directory if it doesn't exist
log_directory = 'logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.join(log_directory, 'ai_scraper.log'),
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class NewsSearcher:
    def __init__(self):
        """Initialize Chrome driver with stealth settings"""
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        
        # Apply stealth settings
        stealth(self.driver,
               languages=["en-US", "en"],
               vendor="Google Inc.",
               platform="Win32",
               webgl_vendor="Intel Inc.",
               renderer="Intel Iris OpenGL Engine",
               fix_hairline=True,
        )
        
        logging.info("Browser initialized with stealth settings")

    def scroll_and_load(self, scroll_increment=500, scroll_pause=1):
        """Scroll the page to load more content"""
        try:
            # Get initial scroll height
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_count = 0
            max_scrolls = 1  # Increased number of scrolls

            while scroll_count < max_scrolls:
                scroll_count += 1
                logging.info(f"Starting scroll iteration {scroll_count}")
                
                # Scroll down in smaller increments
                for i in range(0, last_height, scroll_increment):
                    self.driver.execute_script(f"window.scrollTo(0, {i});")
                    time.sleep(scroll_pause)  # Increased pause time
                    logging.debug(f"Scrolled to position {i}")

                # Wait for content to load
                time.sleep(scroll_pause)
                
                # Calculate new scroll height
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                logging.info(f"New scroll height: {new_height}, Previous height: {last_height}")
                
                if new_height == last_height:
                    logging.info("Reached end of scrollable content")
                    break
                    
                last_height = new_height
                logging.info(f"Completed scroll iteration {scroll_count}")

        except Exception as e:
            logging.error(f"Error during scrolling: {str(e)}")

    def scrape_news(self):
        """Scrape news articles directly using Selenium"""
        try:
            # Navigate to Google News
            self.driver.get("https://news.google.com/")
            logging.info("Opened Google News")

            # Wait for page to load
            time.sleep(2)

            # Find and click on "Top stories" link
            top_stories = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//*[text()='Top stories']"))
            )
            logging.info("Found Top stories link")
            top_stories.click()
            logging.info("Clicked Top stories")

            # Wait for initial content to load
            time.sleep(2)
            
            # Perform scrolling before scraping
            logging.info("Starting page scroll")
            self.scroll_and_load()
            logging.info("Completed page scroll")
            
            # Find the main container
            container = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "PIlOad"))
            )
            logging.info("Found main container with class PIlOad")

            # Find all elements with links within the container
            articles = container.find_elements(By.TAG_NAME, "article")
            logging.info(f"Found {len(articles)} article elements")
            
            news_data = []

            # Process all articles
            for index, article in enumerate(articles, 1):
                try:
                    # Find all links in the article
                    links = article.find_elements(By.TAG_NAME, "a")
                    logging.debug(f"Processing article {index}, found {len(links)} links")

                    if links:
                        # Get the main link (usually the first one with text)
                        for link in links:
                            title = link.text.strip()
                            href = link.get_attribute('href')
                            
                            # Find source element within the article
                            try:
                                source_element = article.find_element(By.CLASS_NAME, "vr1PYe")
                                source = source_element.text.strip()
                                logging.debug(f"Found source: {source}")
                            except Exception as se:
                                logging.debug(f"No source element found for article {index}: {str(se)}")
                                source = None
                            
                            # Find time element within the article
                            try:
                                time_element = article.find_element(By.CLASS_NAME, "hvbAAd")
                                datetime_value = time_element.get_attribute('datetime')
                                time_ago = time_element.text
                                logging.debug(f"Found time: {time_ago} ({datetime_value})")
                            except Exception as te:
                                logging.debug(f"No time element found for article {index}: {str(te)}")
                                datetime_value = None
                                time_ago = None
                            
                            if title and href:  # Only add if both title and link exist
                                news_data.append({
                                    'title': title,
                                    'link': href,
                                    'source': source,
                                    'datetime': datetime_value,
                                    'time_ago': time_ago
                                })
                                logging.info(f"Article {index}: {title} ({source}, {time_ago})")
                                break  # Take only the first valid link
                except Exception as e:
                    logging.error(f"Error processing article {index}: {str(e)}")
                    continue

            logging.info(f"Successfully processed {len(news_data)} articles")
            return news_data

        except Exception as e:
            logging.error(f"An error occurred while scraping: {str(e)}")
            return []

    def close(self):
        """Close the browser"""
        self.driver.quit()
        logging.info("Browser closed")

def parse_datetime(dt_str):
    """Convert ISO datetime string to datetime object for sorting"""
    if not dt_str:
        return datetime.min  # Handle None values
    try:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return datetime.min

def main():
    """Main function to execute the scraping process"""
    searcher = NewsSearcher()
    try:
        news_articles = searcher.scrape_news()
        
        if news_articles:
            # Sort articles by datetime (most recent first)
            sorted_articles = sorted(news_articles, 
                                  key=lambda x: parse_datetime(x.get('datetime')), 
                                  reverse=True)
            
            # Print to console
            print("\nScraped News Articles (Sorted by Date):")
            for i, article in enumerate(sorted_articles, 1):
                print(f"\n{i}. {article['title']}")
                print(f"   Link: {article['link']}")
                if article['time_ago']:
                    print(f"   Time: {article['time_ago']} ({article['datetime']})")

            # Save to JSON file with timestamp in a more human-readable format
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"news_articles_{timestamp}.json"

            # Define the output path
            output_path = f"data/scraped_data/{filename}"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(sorted_articles, f, indent=4, ensure_ascii=False)
            
            logging.info(f"Successfully saved {len(sorted_articles)} sorted articles to {filename}")
        else:
            logging.warning("No articles were scraped")
            
    except Exception as e:
        logging.error(f"Main execution error: {str(e)}")
    finally:
        searcher.close()

if __name__ == "__main__":
    main()
