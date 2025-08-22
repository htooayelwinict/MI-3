#!/usr/bin/env python3
"""
MI-3 News Scraper - Sentiment Analysis and Deduplication Processor
This module provides sentiment analysis and deduplication functionality for scraped news data.
"""

import json
import os
import logging
import glob
import sys
from datetime import datetime

# Try to import ML dependencies
try:
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
    ML_DEPENDENCIES_AVAILABLE = True
except ImportError:
    ML_DEPENDENCIES_AVAILABLE = False

# Get logger for this module
logger = logging.getLogger(__name__)

# Global variables for model and pipeline
model = None
tokenizer = None
nlp = None

def load_model():
    """Load the sentiment analysis model and tokenizer"""
    global model, tokenizer, nlp
    
    if nlp is not None:
        return  # Already loaded
    
    try:
        logger.info("Loading sentiment analysis model and tokenizer...")
        model_name = "fuchenru/Trading-Hero-LLM"
        model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=3)
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer, max_length=128, truncation=True)
        logger.info("Model and tokenizer loaded successfully")
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        logger.warning("Model loading failed - sentiment analysis will return 'neutral' for all predictions")
        nlp = None

def predict_sentiment(input_text):
    """Perform sentiment prediction on input text"""
    global nlp
    
    if nlp is None:
        logger.warning("Model not loaded, attempting to load now...")
        load_model()
        
    if nlp is None:
        logger.warning("Model still not available, returning neutral sentiment")
        return 'neutral'
    
    try:
        logger.debug(f"Predicting sentiment for text: {input_text[:100]}...")
        
        # Use pipeline directly for prediction
        result = nlp(input_text)[0]
        logger.debug(f"Raw prediction result: {result}")
        
        predicted_label = int(result['label'].split('_')[1])
        logger.debug(f"Extracted label number: {predicted_label}")
        
        # Map the predicted label
        label_map = {0: 'neutral', 1: 'positive', 2: 'negative'}
        predicted_sentiment = label_map[predicted_label]
        logger.debug(f"Mapped sentiment: {predicted_sentiment}")
        
        return predicted_sentiment
        
    except Exception as e:
        logger.error(f"Error in sentiment prediction: {str(e)}")
        return 'neutral'  # default fallback

def read_scraped_data(directory='data/scraped_data'):
    """Read all JSON files from the scraped data directory"""
    all_data = []
    logger.info(f"Reading data from directory: {directory}")
    
    if not os.path.exists(directory):
        logger.warning(f"Directory {directory} does not exist")
        return all_data
    
    json_files = glob.glob(os.path.join(directory, '*.json'))
    logger.info(f"Found {len(json_files)} JSON files")
    
    for file_path in json_files:
        try:
            logger.debug(f"Processing file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                    logger.debug(f"Read {len(data)} entries from {file_path}")
                else:
                    logger.warning(f"File {file_path} does not contain a list - skipping")
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
    
    logger.info(f"Total entries read: {len(all_data)}")
    return all_data

def remove_duplicates(data):
    """Remove duplicates while keeping the latest entry for each title"""
    try:
        logger.info(f"Starting duplicate removal for {len(data)} entries")
        title_dict = {}
        
        # Process entries - use various timestamp fields that might exist
        def get_timestamp(entry):
            # Try different timestamp field names that might exist in scraped data
            for field in ['timestamp', 'datetime', 'time_ago', 'scraped_at']:
                if field in entry and entry[field]:
                    return entry[field]
            return ''
        
        # Sort by timestamp if available
        try:
            sorted_data = sorted(data, key=get_timestamp, reverse=True)
            logger.debug("Data sorted by timestamp")
        except Exception as e:
            logger.warning(f"Could not sort by timestamp: {str(e)}, proceeding without sorting")
            sorted_data = data
        
        # Keep only the latest entry for each title
        for entry in sorted_data:
            title = entry.get('title')
            if title and title not in title_dict:
                title_dict[title] = entry
        
        # Convert back to list
        unique_data = list(title_dict.values())
        
        duplicates_removed = len(data) - len(unique_data)
        logger.info(f"Removed {duplicates_removed} duplicates")
        logger.info(f"Remaining entries: {len(unique_data)}")
        
        return unique_data
    except Exception as e:
        logger.error(f"Error removing duplicates: {str(e)}")
        return data

def process_and_save_data():
    """Process scraped data and save to processed_data.json"""
    try:
        logger.info("Starting data processing")
        
        # Ensure output directory exists
        output_dir = 'data/processed_data'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'processed_data.json')
        logger.info(f"Output will be saved to: {output_file}")

        # Load existing processed data first
        existing_data = []
        existing_titles = set()
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_titles = {entry['title'] for entry in existing_data if 'title' in entry}
                logger.info(f"Loaded {len(existing_data)} existing entries")
            except json.JSONDecodeError:
                logger.warning("Existing file corrupted. Creating new file.")

        # Read and filter new data
        scraped_data = read_scraped_data()
        if not scraped_data:
            logger.warning("No scraped data found to process")
            print("No scraped data found to process")
            return
        
        new_entries = [entry for entry in scraped_data 
                      if entry.get('title') and entry['title'] not in existing_titles]
        logger.info(f"Found {len(new_entries)} new entries to process")

        if not new_entries:
            logger.info("No new entries to process")
            print("No new entries to process")
            return

        # Load the sentiment analysis model
        load_model()

        # Process new entries
        new_processed_data = []
        for i, entry in enumerate(new_entries, 1):
            logger.debug(f"Processing entry {i}/{len(new_entries)}")
            sentiment = predict_sentiment(entry.get('title', ''))
            
            # Extract timestamp from various possible fields
            timestamp = entry.get('datetime') or entry.get('timestamp') or datetime.now().isoformat()
            
            processed_entry = {
                "title": entry['title'],
                "sentiment": sentiment,
                "timestamp": timestamp,
                "link": entry.get('link', entry.get('href', '')),
                "source": entry.get('source', 'Unknown')
            }
            new_processed_data.append(processed_entry)
            logger.debug(f"Processed: '{entry['title'][:50]}...' -> {sentiment}")

        # Combine and clean data
        combined_data = existing_data + new_processed_data
        logger.info(f"Combined data size: {len(combined_data)}")
        
        final_data = remove_duplicates(combined_data)

        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=4, ensure_ascii=False)

        # Log final results
        logger.info(f"Successfully saved data to: {output_file}")
        logger.info(f"Added {len(new_processed_data)} new entries")
        logger.info(f"Removed {len(combined_data) - len(final_data)} duplicates")
        logger.info(f"Total entries in database: {len(final_data)}")

        # Print to console
        print(f"\n✅ Sentiment analysis and deduplication completed!")
        print(f"📄 Processed data saved to: {output_file}")
        print(f"➕ Added {len(new_processed_data)} new entries")
        print(f"🔄 Removed {len(combined_data) - len(final_data)} duplicates")
        print(f"📊 Total entries in database: {len(final_data)}")
        
        return output_file

    except Exception as e:
        error_msg = f"Error in process_and_save_data: {str(e)}"
        logger.error(error_msg)
        print(f"ERROR: {error_msg}")
        raise

if __name__ == "__main__":
    # Set up logging for standalone execution
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join('logs', 'thllm_processor.log')),
            logging.StreamHandler()
        ]
    )
    
    try:
        logger.info("Starting THLLM processing")
        print("Starting sentiment analysis and deduplication processing...")
        process_and_save_data()
        logger.info("Processing completed successfully")
        print("Processing completed successfully")
    except Exception as e:
        logger.error(f"Program failed: {str(e)}")
        print(f"Program failed: {str(e)}")
        sys.exit(1)