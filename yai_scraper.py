import os
import json
import time
import logging
import glob
import re
import traceback
from datetime import datetime, timedelta
from selector_utils import sanitize_selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_stealth import stealth

# Import the scraper debugger
try:
    from scraper_debug import ScraperDebugger
    DEBUGGER_AVAILABLE = True
except ImportError:
    DEBUGGER_AVAILABLE = False
    logging.warning("ScraperDebugger not available. Some features will be disabled.")

# Create logs directory if it doesn't exist
log_directory = 'logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.join(log_directory, 'yai_scraper.log'),
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class YahooNewsScraper:
    def __init__(self, headless=True, enable_debug=False):
        # Store configuration
        self.headless = headless
        self.enable_debug = enable_debug
        
        # Initialize the Chrome options
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Initialize the Chrome driver
        self.driver = webdriver.Chrome(options=options)
        
        # Apply stealth settings
        stealth(self.driver,
               languages=["en-US", "en"],
               vendor="Apple Inc.",
               platform="Macintosh",
               webgl_vendor="Apple Inc.",
               renderer="Apple M1 OpenGL Engine",
               fix_hairline=True,
        )
        
        # Create directories for data if they don't exist
        os.makedirs('data/scraped_data', exist_ok=True)
        
        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create a file handler
        os.makedirs('logs', exist_ok=True)
        handler = logging.FileHandler('logs/yai_scraper.log')
        handler.setLevel(logging.INFO)
        
        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)
        
        # Load dynamic selectors if available
        self.selectors = self.load_dynamic_selectors()
        
        self.logger.info("YahooNewsScraper initialized")
        
    def load_dynamic_selectors(self):
        """Load the latest dynamic selectors from debug files"""
        default_selectors = {
            'article_containers': [
                {'selector': 'li.js-stream-content', 'item_selector': None},
                {'selector': 'ul.My\\(0\\) > li', 'item_selector': None},
                {'selector': 'div[data-test-locator="mega"]', 'item_selector': 'li'},
                {'selector': 'div.js-stream-content', 'item_selector': 'div.Pos\\(r\\)'},
                {'selector': 'ul.My\\(0\\) Ov\\(h\\) P\\(0\\) Wow\\(bw\\)', 'item_selector': 'li'}
            ],
            'title_selectors': [
                'a.js-content-viewer',
                'a.mega-item-header-link',
                'h3 > a',
                'h3 a',
                'h4 a',
                'a[data-test-locator*="headline"]',
                'a[class*="title"]',
                'a[class*="headline"]'
            ],
            'source_selectors': [
                'div.C\\(#959595\\)',
                'div.C\\(#4d4d4d\\)',
                'div.C\\(#959595\\) > span:first-child',
                'div[class*="C\\(#959595\\)"] > span',
                'div[class*="caas-attr"] span',
                'div[class*="provider"] span',
                'span[class*="provider"]',
                'div[class*="meta"] span'
            ],
            'time_selectors': [
                'div.C\\(#959595\\) > span:last-child',
                'div.C\\(#4d4d4d\\) > span:last-child',
                'span.fc-4th',
                'div[class*="C\\(#959595\\)"] > span:nth-child(2)',
                'span[class*="time"]',
                'time'
            ]
        }
        
        try:
            # Ensure debug directories exist
            os.makedirs('debug/selectors', exist_ok=True)
            self.logger.info("Debug directories created/verified")
                
            selector_files = glob.glob('debug/selectors/yahoo_finance_selectors_*.json')
            if not selector_files:
                self.logger.info("No Yahoo Finance selector files found, using default selectors")
                return default_selectors
                
            latest_file = max(selector_files)
            self.logger.info(f"Loading selectors from {latest_file}")
            
            with open(latest_file, 'r', encoding='utf-8') as f:
                dynamic_selectors = json.load(f)
            
            # Validate the loaded selectors
            if not isinstance(dynamic_selectors, dict):
                self.logger.error("Invalid selector format: not a dictionary")
                return default_selectors
                
            for key in default_selectors:
                if key not in dynamic_selectors or not dynamic_selectors[key]:
                    self.logger.warning(f"Missing or empty key '{key}' in dynamic selectors, using defaults")
                    dynamic_selectors[key] = default_selectors[key]
                    
            self.logger.info(f"Loaded dynamic selectors with {sum(len(dynamic_selectors[k]) for k in dynamic_selectors)} total selector patterns")
            return dynamic_selectors
            
        except Exception as e:
            self.logger.error(f"Error loading dynamic selectors: {str(e)}")
            self.logger.info("Using default selectors")
            return default_selectors

    def scroll_and_load(self, scroll_increment=1000, scroll_pause=4):
        """Scroll the page in increments until no new content is loaded."""
        try:
            # Initial wait for page load
            time.sleep(5)  # Increased initial wait
            
            # Get initial scroll height
            last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            scroll_count = 0
            max_scrolls = 5  # Increased max scrolls

            while scroll_count < max_scrolls:
                scroll_count += 1
                self.logger.info(f"Scroll iteration {scroll_count}")
                
                # Scroll to bottom of page
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(scroll_pause)
                
                # Calculate new scroll height after content load
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                self.logger.info(f"New height: {new_height}, Previous height: {last_height}")

                if new_height == last_height:
                    # Add extra wait before deciding no new content
                    time.sleep(2)
                    final_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                    if final_height == new_height:
                        self.logger.info("No new content loaded")
                        break
                last_height = new_height
                
                # Additional wait for dynamic content
                time.sleep(2)

        except Exception as e:
            self.logger.error(f"Error during scrolling: {str(e)}")

    def accept_cookies(self):
        """Handle cookie consent popup"""
        try:
            # Wait for cookie banner and find accept button
            wait = WebDriverWait(self.driver, 10)
            accept_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class*='btn secondary accept-all ']"))
            )
            self.logger.info("Found cookie accept button")
            
            # Click the accept button
            accept_button.click()
            self.logger.info("Clicked accept cookies button")
            
            # Wait for banner to disappear
            time.sleep(1)
            
        except TimeoutException:
            self.logger.info("No cookie banner found or already accepted")
        except Exception as e:
            self.logger.error(f"Error handling cookie consent: {str(e)}")

    def scrape_news(self, retry_count=0):
        """Scrape news articles from Yahoo Finance using a direct approach"""
        # Prevent infinite recursion
        if retry_count >= 2:
            self.logger.warning("Maximum retry count reached, using default selectors directly")
            # Force use of default selectors on last attempt
            self.selectors = self.load_dynamic_selectors()
            
        try:
            self.logger.info(f"Starting to scrape Yahoo Finance news (attempt {retry_count + 1})")
            self.driver.get("https://finance.yahoo.com/topic/latest-news/")
            self.logger.info("Loaded Yahoo Finance latest news page")
            
            # Wait for page to load
            time.sleep(5)
            
            # Handle cookie consent if present
            try:
                cookie_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='agree']"))
                )
                cookie_button.click()
                self.logger.info("Accepted cookie consent")
                time.sleep(2)
            except TimeoutException:
                self.logger.info("No cookie banner found or already accepted")
                
            # Scroll down to load more content - increased iterations and wait time
            prev_height = 0
            same_height_count = 0
            max_same_height = 2  # Stop if height doesn't change for this many iterations
            
            for i in range(8):  # Increased from 5 to 8 iterations
                self.logger.info(f"Scroll iteration {i+1}")
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(5)  # Increased from 4 to 5 seconds
                
                # Check if page height increased
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                self.logger.info(f"New height: {new_height}, Previous height: {prev_height}")
                
                # More robust height comparison
                if new_height == prev_height:
                    same_height_count += 1
                    if same_height_count >= max_same_height:
                        self.logger.info(f"Height unchanged for {max_same_height} iterations, stopping scrolling")
                        break
                else:
                    same_height_count = 0
                    
                prev_height = new_height
                
            # Final wait to ensure all content is loaded
            self.logger.info("Waiting for final content to load...")
            time.sleep(3)
            
            # DIRECT APPROACH: Find all news article links
            self.logger.info("Using direct approach to find news articles")
            news_articles = []
            
            # Find all links on the page
            all_links = self.driver.find_elements(By.TAG_NAME, 'a')
            self.logger.info(f"Found {len(all_links)} total links on the page")
            
            # Filter for news article links
            news_links = []
            seen_urls = set()  # Track unique URLs to avoid duplicates
            seen_titles = set()  # Track unique titles to avoid duplicates
            
            for link in all_links:
                try:
                    href = link.get_attribute('href')
                    if not href or 'finance.yahoo.com/news/' not in href:
                        continue
                        
                    # Skip if we've already seen this URL
                    if href in seen_urls:
                        continue
                    
                    # Get the title from the link text or aria-label
                    title = link.text.strip()
                    if not title:
                        title = link.get_attribute('aria-label') or ''
                        
                    # Only include links with actual text content
                    if title and len(title) > 10:
                        # Skip if we've already seen this title
                        if title in seen_titles:
                            continue
                            
                        news_links.append((link, href, title))
                        seen_urls.add(href)
                        seen_titles.add(title)
                except Exception as e:
                    self.logger.debug(f"Error processing link: {str(e)}")
                    continue
            
            self.logger.info(f"Found {len(news_links)} unique news article links")
            
            # Process the news links
            for index, (link, href, title) in enumerate(news_links, 1):
                try:
                    # Get source and time information
                    source = "Yahoo Finance"  # Default source
                    time_ago = ""
                    
                    # Try to find source/time near the link
                    try:
                        # Look for parent elements that might contain metadata
                        parent = link.find_element(By.XPATH, '..')
                        
                        # Look for spans within the parent
                        spans = parent.find_elements(By.TAG_NAME, 'span')
                        for span in spans:
                            span_text = span.text.strip()
                            if span_text:
                                # Check if it looks like a time string
                                if any(time_marker in span_text.lower() for time_marker in ['ago', 'min', 'hour', 'day', 'sec']):
                                    time_ago = span_text
                                # Otherwise it might be a source
                                elif len(span_text) < 30 and not any(char.isdigit() for char in span_text):
                                    source = span_text
                    except Exception as e:
                        self.logger.debug(f"Error finding metadata for article {index}: {str(e)}")
                    
                    # Add to results
                    article_data = {
                        'title': title,
                        'href': href,
                        'source': source,
                        'time': time_ago
                    }
                    
                    self.logger.info(f"Article {index}: {title[:50]}... | {source} | {time_ago}")
                    news_articles.append(article_data)
                    
                except Exception as e:
                    self.logger.error(f"Error processing article link {index}: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully processed {len(news_articles)} articles")
            return news_articles
            
        except Exception as e:
            self.logger.error(f"An error occurred while scraping: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # If we have retry attempts left and debug is enabled, try to update selectors
            if retry_count < 2 and self.enable_debug:
                self.logger.info("Attempting to update selectors using debugger")
                try:
                    from scraper_debug import ScraperDebugger
                    debugger = ScraperDebugger(headless=self.headless)
                    selector_file = debugger.analyze_yahoo_finance()
                    debugger.close()
                    
                    if selector_file:
                        self.logger.info(f"Updated selectors saved to {selector_file}")
                        # Reload selectors
                        self.selectors = self.load_dynamic_selectors()
                        # Retry scraping
                        return self.scrape_news(retry_count + 1)
                except Exception as e:
                    self.logger.error(f"Error updating selectors: {str(e)}")
            
            return []

    def close(self):
        """Close the browser"""
        self.driver.quit()
        self.logger.info("Browser closed")

def convert_time_ago_to_datetime(time_ago):
    """Convert relative time to ISO datetime format"""
    try:
        current_time = datetime.utcnow()
        
        # Extract number and unit from time_ago string
        if not time_ago or 'ago' not in time_ago:
            return current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            
        parts = time_ago.lower().split()
        if len(parts) < 2:
            return current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            
        value = int(parts[0])
        unit = parts[1]

        # Convert relative time to timedelta
        if 'minute' in unit:
            delta = timedelta(minutes=value)
        elif 'hour' in unit:
            delta = timedelta(hours=value)
        elif 'day' in unit:
            delta = timedelta(days=value)
        elif 'week' in unit:
            delta = timedelta(weeks=value)
        elif 'month' in unit:
            delta = timedelta(days=value * 30)  # Approximate
        elif 'year' in unit:
            delta = timedelta(days=value * 365)  # Approximate
        else:
            return current_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Calculate the actual datetime
        article_datetime = current_time - delta
        return article_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        logging.error(f"Error converting time_ago: {str(e)}")
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def update_selectors():
    """Run the scraper debugger to update selectors"""
    if not DEBUGGER_AVAILABLE:
        print("ScraperDebugger not available. Cannot update selectors.")
        return False
    
    try:
        print("Running scraper debugger to update selectors...")
        debugger = ScraperDebugger(headless=True)
        yahoo_selector_file = debugger.analyze_yahoo_finance()
        debugger.close()
        
        if yahoo_selector_file:
            print(f"Successfully updated selectors: {yahoo_selector_file}")
            return True
        else:
            print("Failed to update selectors")
            return False
    except Exception as e:
        print(f"Error updating selectors: {str(e)}")
        return False

def main():
    """Main function to run the scraper"""
    # Check if we should update selectors first
    import argparse
    parser = argparse.ArgumentParser(description='Yahoo Finance News Scraper')
    parser.add_argument('--update-selectors', action='store_true', help='Update selectors before scraping')
    args = parser.parse_args()
    
    if args.update_selectors:
        update_selectors()
    
    scraper = YahooNewsScraper(headless=True)
    try:
        articles = scraper.scrape_news()
        print(f"Scraped {len(articles)} articles from Yahoo Finance")
        
        # Save the articles to a file
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"data/scraped_data/yahoo_finance_articles_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(articles, f, indent=4)
        
        print(f"Saved articles to {filename}")
        
        # Print the first 5 articles
        if articles:
            print("\nSample of scraped articles:")
            for i, article in enumerate(articles[:5], 1):
                print(f"{i}. {article['title']}")
                print(f"   Source: {article['source']}")
                print(f"   Time: {article['time_ago']}")
                print(f"   Link: {article['link']}\n")
        else:
            logging.warning("No articles were scraped")
            
    except Exception as e:
        logging.error(f"Main execution error: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
