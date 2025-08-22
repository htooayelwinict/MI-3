#!/usr/bin/env python3
"""
MI-3 News Scraper - Debug Utility
This utility helps analyze website structures and extract dynamic selectors for scrapers.
"""

import os
import sys
import json
import logging
import argparse
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
from selenium_stealth import stealth
from selector_utils import sanitize_selector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ScraperDebugger:
    def __init__(self, headless=True):
        """Initialize the debugger with a Selenium driver"""
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
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
        
        logger.info("Debug browser initialized with stealth settings")
        
        # Create directories for debug output
        os.makedirs('debug', exist_ok=True)
        os.makedirs('debug/html', exist_ok=True)
        os.makedirs('debug/selectors', exist_ok=True)
        
        logger.info("Debug directories created/verified")

    def accept_cookies(self):
        """Handle cookie consent popup"""
        try:
            # Wait for cookie banner and find accept button
            wait = WebDriverWait(self.driver, 10)
            # Try multiple selectors for cookie accept buttons
            selectors = [
                "button[class*='btn secondary accept-all ']",
                "button[class*='accept']",
                "button[id*='accept']",
                "a[class*='accept']"
            ]
            
            for selector in selectors:
                try:
                    accept_button = wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"Found cookie accept button with selector: {selector}")
                    accept_button.click()
                    logger.info("Clicked accept cookies button")
                    time.sleep(1)
                    return True
                except TimeoutException:
                    continue
            
            logger.info("No cookie banner found or already accepted")
            return False
            
        except Exception as e:
            logger.error(f"Error handling cookie consent: {str(e)}")
            return False

    def scroll_page(self, scroll_count=5, scroll_pause=2):
        """Scroll the page to load dynamic content"""
        try:
            # Initial wait for page load
            time.sleep(3)
            
            # Get initial scroll height
            last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            
            for i in range(scroll_count):
                logger.info(f"Scroll iteration {i+1}")
                
                # Scroll to bottom of page
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(scroll_pause)
                
                # Calculate new scroll height after content load
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                logger.info(f"New height: {new_height}, Previous height: {last_height}")

                if new_height == last_height:
                    # Add extra wait before deciding no new content
                    time.sleep(2)
                    final_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                    if final_height == new_height:
                        logger.info("No new content loaded")
                        break
                last_height = new_height
                
                # Additional wait for dynamic content
                time.sleep(1)

        except Exception as e:
            logger.error(f"Error during scrolling: {str(e)}")

    def save_page_source(self, url, name):
        """Save the page source to a file"""
        try:
            self.driver.get(url)
            logger.info(f"Opened URL: {url}")
            
            # Handle cookie consent
            self.accept_cookies()
            
            # Scroll to load dynamic content
            self.scroll_page()
            
            # Save the page source
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"debug/html/{name}_{timestamp}.html"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            
            logger.info(f"Saved page source to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving page source: {str(e)}")
            return None

    def analyze_yahoo_finance(self):
        """Analyze Yahoo Finance structure and extract selectors"""
        # Ensure debug directories exist
        os.makedirs('debug/selectors', exist_ok=True)
        os.makedirs('debug/html', exist_ok=True)
        
        logger.info("Analyzing Yahoo Finance structure")
        url = "https://finance.yahoo.com/topic/latest-news/"
        html_file = self.save_page_source(url, "yahoo_finance")
        
        if not html_file:
            logger.error("Failed to save Yahoo Finance page source")
            return None
        
        # Parse the HTML
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find potential article containers
        selectors = {
            'article_containers': [],
            'title_selectors': [],
            'link_selectors': [],
            'source_selectors': [],
            'time_selectors': []
        }
        
        # Look for common container patterns
        container_candidates = []
        
        # Find UL elements with multiple LI children (common pattern for article lists)
        for ul in soup.find_all('ul'):
            if len(ul.find_all('li')) > 3:
                container_candidates.append(ul)
                # Get classes and construct a selector
                if ul.get('class'):
                    class_selector = f"ul.{' .'.join(ul.get('class'))}"
                    # Sanitize the selector
                    safe_selector = sanitize_selector(class_selector, logger)
                    selectors['article_containers'].append({
                        'selector': safe_selector,
                        'item_selector': 'li',
                        'count': len(ul.find_all('li'))
                    })
        
        # Find DIV elements that might be containers
        for div in soup.find_all('div', id=True):
            if len(div.find_all('a', href=True)) > 5:
                container_candidates.append(div)
                # Sanitize the selector
                selector = f"div#{div['id']}"
                safe_selector = sanitize_selector(selector, logger)
                selectors['article_containers'].append({
                    'selector': safe_selector,
                    'item_selector': 'a[href]',
                    'count': len(div.find_all('a', href=True))
                })
        
        # Find common patterns for titles, links, sources, and times
        for container in container_candidates:
            # Look for title patterns
            for a in container.find_all('a', href=True):
                if a.text.strip() and len(a.text.strip()) > 20:
                    # This might be a title
                    parent_classes = []
                    if a.parent and a.parent.get('class'):
                        parent_classes = a.parent.get('class')
                    
                    if a.get('class'):
                        selector = f"a.{' .'.join(a.get('class'))}"
                        safe_selector = sanitize_selector(selector, logger)
                        selectors['title_selectors'].append({
                            'selector': safe_selector,
                            'sample': a.text.strip()[:50]
                        })
                    elif parent_classes:
                        selector = f"{a.parent.name}.{' .'.join(parent_classes)} > a"
                        safe_selector = sanitize_selector(selector, logger)
                        selectors['title_selectors'].append({
                            'selector': safe_selector,
                            'sample': a.text.strip()[:50]
                        })
            
            # Look for source and time patterns
            for span in container.find_all('span'):
                text = span.text.strip()
                if text:
                    if any(source in text.lower() for source in ['yahoo', 'reuters', 'bloomberg', 'cnbc']):
                        # This might be a source
                        if span.get('class'):
                            selectors['source_selectors'].append({
                                'selector': f"span.{' .'.join(span.get('class'))}",
                                'sample': text
                            })
                    elif any(time_indicator in text.lower() for time_indicator in ['ago', 'min', 'hour', 'day']):
                        # This might be a timestamp
                        if span.get('class'):
                            selectors['time_selectors'].append({
                                'selector': f"span.{' .'.join(span.get('class'))}",
                                'sample': text
                            })
        
        # Sanitize and validate all selectors before saving
        for key in selectors:
            if not selectors[key]:
                logger.warning(f"No {key} found, using empty list")
            else:
                logger.info(f"Found {len(selectors[key])} {key}")
                
                # Sanitize each selector
                sanitized_items = []
                for item in selectors[key]:
                    if isinstance(item, dict) and 'selector' in item:
                        # For dictionary items with 'selector' key
                        safe_selector = sanitize_selector(item['selector'], logger)
                        item['selector'] = safe_selector
                        sanitized_items.append(item)
                    elif isinstance(item, str):
                        # For string items
                        safe_selector = sanitize_selector(item, logger)
                        sanitized_items.append(safe_selector)
                    else:
                        logger.warning(f"Skipping invalid selector item: {item}")
                
                selectors[key] = sanitized_items
                logger.info(f"Sanitized {len(sanitized_items)} {key}")
        
        # Ensure debug directories exist
        os.makedirs('debug/selectors', exist_ok=True)
        
        # Save the selectors
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        selector_file = f"debug/selectors/yahoo_finance_selectors_{timestamp}.json"
        
        # Log the selectors being saved
        logger.info(f"Saving selectors to {selector_file}")
        for key in selectors:
            logger.debug(f"{key}: {selectors[key]}")
            
        with open(selector_file, 'w', encoding='utf-8') as f:
            json.dump(selectors, f, indent=4)
        
        logger.info(f"Saved Yahoo Finance selectors to {selector_file}")
        return selector_file

    def analyze_google_news(self):
        """Analyze Google News structure and extract selectors"""
        url = "https://news.google.com/"
        html_file = self.save_page_source(url, "google_news")
        
        if not html_file:
            logger.error("Failed to save Google News page source")
            return None
        
        # Parse the HTML
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find potential article containers
        selectors = {
            'article_containers': [],
            'title_selectors': [],
            'link_selectors': [],
            'source_selectors': [],
            'time_selectors': []
        }
        
        # Look for common container patterns
        container_candidates = []
        
        # Find article elements
        for article in soup.find_all('article'):
            container_candidates.append(article)
            if article.get('class'):
                selectors['article_containers'].append({
                    'selector': f"article.{' .'.join(article.get('class'))}",
                    'item_selector': None,
                    'count': 1
                })
        
        # Find DIV elements that might be containers
        for div in soup.find_all('div', class_=True):
            if len(div.find_all('a', href=True)) > 3:
                container_candidates.append(div)
                selectors['article_containers'].append({
                    'selector': f"div.{' .'.join(div.get('class'))}",
                    'item_selector': 'a[href]',
                    'count': len(div.find_all('a', href=True))
                })
        
        # Find common patterns for titles, links, sources, and times
        for container in container_candidates:
            # Look for title patterns
            for a in container.find_all('a', href=True):
                if a.text.strip() and len(a.text.strip()) > 20:
                    # This might be a title
                    if a.get('class'):
                        selectors['title_selectors'].append({
                            'selector': f"a.{' .'.join(a.get('class'))}",
                            'sample': a.text.strip()[:50]
                        })
                    else:
                        # Try to get parent with class
                        parent = a.parent
                        if parent and parent.get('class'):
                            selectors['title_selectors'].append({
                                'selector': f"{parent.name}.{' .'.join(parent.get('class'))} > a",
                                'sample': a.text.strip()[:50]
                            })
            
            # Look for source and time patterns
            for span in container.find_all('span'):
                text = span.text.strip()
                if text:
                    if any(time_indicator in text.lower() for time_indicator in ['ago', 'min', 'hour', 'day']):
                        # This might be a timestamp
                        if span.get('class'):
                            selectors['time_selectors'].append({
                                'selector': f"span.{' .'.join(span.get('class'))}",
                                'sample': text
                            })
                    else:
                        # This might be a source
                        if span.get('class'):
                            selectors['source_selectors'].append({
                                'selector': f"span.{' .'.join(span.get('class'))}",
                                'sample': text
                            })
        
        # Log the selectors being saved  
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        selector_file = f"debug/selectors/google_news_selectors_{timestamp}.json"
        logger.info(f"Saving selectors to {selector_file}")
        for key in selectors:
            logger.debug(f"{key}: {selectors[key]}")
            
        # Save the selectors
        
        with open(selector_file, 'w', encoding='utf-8') as f:
            json.dump(selectors, f, indent=4)
        
        logger.info(f"Saved selectors to {selector_file}")
        return selector_file

    def close(self):
        """Close the browser"""
        self.driver.quit()
        logger.info("Debug browser closed")

def main():
    """Main function to run the debugger"""
    parser = argparse.ArgumentParser(description='MI-3 News Scraper Debugger')
    parser.add_argument('--yahoo', action='store_true', help='Debug Yahoo Finance')
    parser.add_argument('--google', action='store_true', help='Debug Google News')
    parser.add_argument('--all', action='store_true', help='Debug all sites')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    
    args = parser.parse_args()
    
    # Default to headless mode
    headless = True if args.headless else False
    
    debugger = ScraperDebugger(headless=headless)
    
    try:
        # Determine which sites to debug
        debug_yahoo = args.yahoo or args.all
        debug_google = args.google or args.all
        
        # If no specific site is selected, debug all
        if not (debug_yahoo or debug_google):
            debug_yahoo = debug_google = True
            logger.info("No specific site selected, debugging all")
        
        results = {}
        
        # Debug selected sites
        if debug_yahoo:
            logger.info("Debugging Yahoo Finance")
            yahoo_selector_file = debugger.analyze_yahoo_finance()
            if yahoo_selector_file:
                results['yahoo'] = yahoo_selector_file
        
        if debug_google:
            logger.info("Debugging Google News")
            google_selector_file = debugger.analyze_google_news()
            if google_selector_file:
                results['google'] = google_selector_file
        
        # Print summary
        print("\n=== Debugging Summary ===")
        if results:
            print(f"Successfully analyzed {len(results)} sites:")
            for site, selector_file in results.items():
                print(f"- {site.capitalize()}: saved selectors to {selector_file}")
        else:
            print("No sites were successfully analyzed.")
        
    finally:
        debugger.close()

if __name__ == "__main__":
    main()
