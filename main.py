#!/usr/bin/env python3
"""
MI-3 News Scraper - Main Entry Point
This script serves as a unified entry point for running the news scrapers.
"""

__version__ = "1.0.0"

import os
import sys
import logging
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

# Import scraper modules
from yai_scraper import YahooNewsScraper
from ai_scraper import NewsSearcher

# Import sentiment processing module
try:
    from thllm_processor import process_and_save_data
    SENTIMENT_PROCESSOR_AVAILABLE = True
except ImportError:
    SENTIMENT_PROCESSOR_AVAILABLE = False

# Import debug utility if available
try:
    from scraper_debug import ScraperDebugger
    DEBUGGER_AVAILABLE = True
except ImportError:
    DEBUGGER_AVAILABLE = False

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_file_logger(name: str) -> None:
    """Set up a file logger for the main script"""
    log_directory = 'logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    file_handler = logging.FileHandler(os.path.join(log_directory, f'{name}.log'), mode='w')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

def save_results(data: List[Dict[str, Any]], source: str) -> str:
    """Save scraped data to a JSON file and return the filename"""
    if not data:
        logger.warning("No data to save from %s , source")
        return None
    
    # Create timestamp for filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{source}_{timestamp}.json"
    
    # Define the output path
    output_path = f"data/scraped_data/{filename}"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save data to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    logger.info(f"Successfully saved {len(data)} articles to {filename}")
    return filename

def run_yahoo_scraper(update_selectors=False, enable_debug=False) -> Optional[str]:
    """Run the Yahoo Finance News scraper"""
    logger.info("Starting Yahoo Finance News scraper")
    
    # Update selectors if requested and available
    if update_selectors and DEBUGGER_AVAILABLE:
        logger.info("Updating Yahoo Finance selectors")
        try:
            debugger = ScraperDebugger(headless=True)
            debugger.analyze_yahoo_finance()
            debugger.close()
            logger.info("Yahoo Finance selectors updated successfully")

        except Exception as e:
            logger.error(f"Error updating selectors: {str(e)}")
    
    # Enable debug mode if updating selectors or explicitly requested
    debug_mode = update_selectors or enable_debug
    scraper = YahooNewsScraper(headless=True, enable_debug=debug_mode)
    
    try:
        news_articles = scraper.scrape_news()
        filename = save_results(news_articles, "yai_scraper")
        
        # Print results to console
        if news_articles:
            print(f"\nScraped {len(news_articles)} Yahoo Finance News Articles:")
            for i, article in enumerate(news_articles[:5], 1):  # Show first 5 articles
                print(f"\n{i}. {article['title']}")
                print(f"   Link: {article['link']}")
                if article.get('source') and article.get('time_ago'):
                    print(f"   Source: {article['source']}, Time: {article['time_ago']}")
            
            if len(news_articles) > 5:
                print(f"\n... and {len(news_articles) - 5} more articles")
        
        return filename
    except Exception as e:
        logger.error(f"Error running Yahoo Finance scraper: {str(e)}")
        return None
    finally:
        scraper.close()

def run_google_scraper(update_selectors=False) -> Optional[str]:
    """Run the Google News scraper"""
    logger.info("Starting Google News scraper")
    
    # Update selectors if requested and available
    if update_selectors and DEBUGGER_AVAILABLE:
        logger.info("Updating Google News selectors")
        try:
            debugger = ScraperDebugger(headless=True)
            debugger.analyze_google_news()
            debugger.close()
            logger.info("Google News selectors updated successfully")
        except Exception as e:
            logger.error(f"Error updating selectors: {str(e)}")
    
    scraper = NewsSearcher()
    
    try:
        news_articles = scraper.scrape_news()
        
        # Sort articles by datetime if available
        try:
            from ai_scraper import parse_datetime
            sorted_articles = sorted(
                news_articles, 
                key=lambda x: parse_datetime(x.get('datetime')), 
                reverse=True
            )
        except Exception as e:
            logger.warning(f"Could not sort articles: {str(e)}")
            sorted_articles = news_articles
        
        filename = save_results(sorted_articles, "news_articles")
        
        # Print results to console
        if sorted_articles:
            print(f"\nScraped {len(sorted_articles)} Google News Articles:")
            for i, article in enumerate(sorted_articles[:5], 1):  # Show first 5 articles
                print(f"\n{i}. {article['title']}")
                print(f"   Link: {article['link']}")
                if article.get('time_ago'):
                    print(f"   Time: {article['time_ago']} ({article.get('datetime', 'N/A')})")
            
            if len(sorted_articles) > 5:
                print(f"\n... and {len(sorted_articles) - 5} more articles")
        
        return filename
    except Exception as e:
        logger.error(f"Error running Google News scraper: {str(e)}")
        return None
    finally:
        scraper.close()

def run_debug_utility():
    """Run the debug utility to analyze website structures and update selectors"""
    if not DEBUGGER_AVAILABLE:
        logger.error("Debug utility not available. Please ensure scraper_debug.py is in the same directory.")
        print("\nError: Debug utility not available.")
        print("Please ensure scraper_debug.py is in the same directory.")
        return False
    
    try:
        print("\n=== Running Scraper Debug Utility ===")
        print("This will analyze website structures and update selectors for scrapers.")
        
        debugger = ScraperDebugger(headless=False)
        
        # Analyze Yahoo Finance
        print("\nAnalyzing Yahoo Finance...")
        yahoo_selector_file = debugger.analyze_yahoo_finance()
        
        # Analyze Google News
        print("\nAnalyzing Google News...")
        google_selector_file = debugger.analyze_google_news()
        
        debugger.close()
        
        print("\n=== Debug Analysis Complete ===")
        if yahoo_selector_file:
            print(f"Yahoo Finance selectors saved to: {yahoo_selector_file}")
        if google_selector_file:
            print(f"Google News selectors saved to: {google_selector_file}")
        
        return True
    except Exception as e:
        logger.error(f"Error running debug utility: {str(e)}")
        print(f"\nError running debug utility: {str(e)}")
        return False

def main():
    """Main function to parse arguments and run scrapers"""
    parser = argparse.ArgumentParser(description='MI-3 News Scraper', prog='mi3-scraper')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--yahoo', action='store_true', help='Run Yahoo Finance News scraper')
    parser.add_argument('--google', action='store_true', help='Run Google News scraper')
    parser.add_argument('--all', action='store_true', help='Run all scrapers')
    parser.add_argument('--debug', action='store_true', help='Run debug utility to analyze website structures')
    parser.add_argument('--update-selectors', action='store_true', help='Update selectors before scraping')
    parser.add_argument('--enable-debug', action='store_true', help='Enable debug mode to auto-update selectors on errors')
    parser.add_argument('--process-sentiment', action='store_true', help='Run sentiment analysis and deduplication on scraped data')
    
    args = parser.parse_args()
    
    # Set up logging
    setup_file_logger('main')
    logger.info("Starting MI-3 News Scraper")
    
    # Run debug utility if requested
    if args.debug:
        if run_debug_utility():
            logger.info("Debug utility completed successfully")
        else:
            logger.error("Debug utility failed")
        return 0
    
    # Track results
    results = {}
    
    # Determine which scrapers to run
    run_yahoo = args.yahoo or args.all
    run_google = args.google or args.all
    update_selectors = args.update_selectors
    enable_debug = args.enable_debug
    process_sentiment = args.process_sentiment
    
    # If no specific scraper is selected, run all
    if not (run_yahoo or run_google):
        run_yahoo = run_google = True
        logger.info("No specific scraper selected, running all")
    
    # Auto-enable debug mode when using --all to handle selector issues automatically
    if args.all:
        enable_debug = True
        logger.info("Auto-enabled debug mode for comprehensive scraping")
    
    # Run selected scrapers
    if run_yahoo:
        yahoo_file = run_yahoo_scraper(update_selectors, enable_debug)
        if yahoo_file:
            results['yahoo'] = yahoo_file
    
    if run_google:
        google_file = run_google_scraper(update_selectors)
        if google_file:
            results['google'] = google_file
    
    # Run sentiment processing if requested
    sentiment_output = None
    if process_sentiment and results:
        if SENTIMENT_PROCESSOR_AVAILABLE:
            try:
                logger.info("Starting sentiment analysis and deduplication")
                sentiment_output = process_and_save_data()
                logger.info("Sentiment processing completed successfully")
            except Exception as e:
                logger.error(f"Error during sentiment processing: {str(e)}")
                print(f"Error during sentiment processing: {str(e)}")
        else:
            logger.error("Sentiment processing requested but dependencies not available")
            print("ERROR: Sentiment processing requested but torch/transformers not installed.")
            print("Please install dependencies: pip install torch transformers")
    elif process_sentiment and not results:
        logger.warning("Sentiment processing requested but no data was scraped")
        print("WARNING: Sentiment processing requested but no data was scraped.")

    # Print summary
    print("\n=== Scraping Summary ===")
    if results:
        print(f"Successfully scraped data from {len(results)} sources:")
        for source, filename in results.items():
            print(f"- {source.capitalize()}: saved to data/scraped_data/{filename}")
        
        if sentiment_output:
            print(f"\n=== Sentiment Processing Summary ===")
            print(f"- Processed data saved to: {sentiment_output}")
    else:
        print("No data was successfully scraped.")
    
    logger.info("MI-3 News Scraper completed")
    return 0

if __name__ == "__main__":
    sys.exit(main())
