#!/usr/bin/env python3
"""
MI-3 News Scraper - Selector Utilities
This module provides utility functions for handling CSS selectors.
"""

import re
import logging

logger = logging.getLogger(__name__)

def sanitize_selector(selector, logger=None):
    """
    Sanitize CSS selectors to ensure they are valid
    
    Args:
        selector (str): The CSS selector to sanitize
        logger (logging.Logger, optional): Logger for error reporting
        
    Returns:
        str: A sanitized CSS selector
    """
    if not selector:
        return "*"
        
    # If selector is a dict with 'selector' key, extract the selector string
    if isinstance(selector, dict) and 'selector' in selector:
        if logger:
            logger.debug(f"Converting selector dict to string: {selector}")
        selector = selector.get('selector', '*')
        
    # Ensure selector is a string
    if not isinstance(selector, str):
        if logger:
            logger.error(f"Invalid selector type: {type(selector)}, expected string")
        return "*"
        
    try:
        # Handle complex selectors with multiple parts
        if ',' in selector:
            parts = selector.split(',')
            sanitized_parts = [sanitize_selector(part.strip(), logger) for part in parts]
            return ', '.join(sanitized_parts)
        
        # Handle Yahoo Finance specific escaped parentheses in class names
        selector = selector.replace('\\(', '\(').replace('\\)', '\)')
        
        # Handle class selectors with spaces (common in dynamically generated selectors)
        if '.' in selector and ' ' in selector:
            # Replace spaces in class names with dots
            selector = re.sub(r'\.([^\s]+)\s+\.', r'.\1.', selector)
        
        # Fix invalid characters in class names
        if '.' in selector:
            # Replace invalid characters in class names with escaped versions
            selector = re.sub(r'\.([^\s\.#\[\]\(\):]+)', lambda m: f'.{re.escape(m.group(1))}', selector)
            
        # Escape special characters in attribute selectors
        if '[' in selector and ']' in selector:
            # Already properly formatted attribute selectors should be left alone
            if not re.search(r'\[.*=.*\]', selector):
                selector = re.sub(r'\[(.*?)\]', lambda m: f'[{m.group(1).replace(" ", "_")}]', selector)
        
        # Final validation - if selector contains invalid characters, return safe fallback
        if re.search(r'[^\w\s\-_\.:,\[\]\(\)=\^\$\*~\|\\>+]', selector):
            if logger:
                logger.warning(f"Selector contains potentially invalid characters: {selector}")
            # Try to clean it up one more time
            selector = re.sub(r'[^\w\s\-_\.:,\[\]\(\)=\^\$\*~\|\\>+]', '', selector)
                
        return selector
    except Exception as e:
        if logger:
            logger.error(f"Error sanitizing selector '{selector}': {str(e)}")
        # Return a safe fallback
        return "*"
