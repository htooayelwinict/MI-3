#!/usr/bin/env python3
"""
MI-3 News Scraper - Configuration Module
Centralized configuration management for the scraper.
"""

import os
from pathlib import Path

# Version information
VERSION = "1.0.0"
APP_NAME = "MI-3 News Scraper"

# Directory structure
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SCRAPED_DATA_DIR = DATA_DIR / "scraped_data"
PROCESSED_DATA_DIR = DATA_DIR / "processed_data"
LOGS_DIR = BASE_DIR / "logs"
DEBUG_DIR = BASE_DIR / "debug"
DEBUG_HTML_DIR = DEBUG_DIR / "html"
DEBUG_SELECTORS_DIR = DEBUG_DIR / "selectors"

# Scraper settings
YAHOO_FINANCE_URL = "https://finance.yahoo.com/topic/latest-news/"
GOOGLE_NEWS_URL = "https://news.google.com/"

# Chrome/Selenium settings
CHROME_OPTIONS = [
    "--headless",
    "--no-sandbox", 
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-plugins"
]

# Sentiment analysis settings
SENTIMENT_MODEL_NAME = "fuchenru/Trading-Hero-LLM"
SENTIMENT_MAX_LENGTH = 128

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"

# File retention settings (for cleanup)
MAX_DEBUG_FILES = 5
MAX_SCRAPED_FILES = 10

def ensure_directories():
    """Create necessary directories if they don't exist"""
    for directory in [DATA_DIR, SCRAPED_DATA_DIR, PROCESSED_DATA_DIR, 
                     LOGS_DIR, DEBUG_DIR, DEBUG_HTML_DIR, DEBUG_SELECTORS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

def get_chrome_options():
    """Get Chrome options for Selenium"""
    return CHROME_OPTIONS.copy()

def get_log_config():
    """Get logging configuration"""
    return {
        'level': LOG_LEVEL,
        'format': LOG_FORMAT,
        'log_dir': LOGS_DIR
    }