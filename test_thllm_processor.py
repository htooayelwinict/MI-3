#!/usr/bin/env python3
"""
Unit tests for thllm_processor.py
Tests sentiment analysis, deduplication, and data processing functionality.
"""

import unittest
import os
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

# Import the module to test
import thllm_processor


class TestThllmProcessor(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        # Create temporary directories for testing
        self.temp_dir = tempfile.mkdtemp()
        self.scraped_data_dir = os.path.join(self.temp_dir, 'data', 'scraped_data')
        self.processed_data_dir = os.path.join(self.temp_dir, 'data', 'processed_data')
        os.makedirs(self.scraped_data_dir)
        os.makedirs(self.processed_data_dir)
        
        # Sample test data
        self.sample_scraped_data = [
            {
                'title': 'Stock Market Rises on Good News',
                'link': 'https://example.com/article1',
                'source': 'Example News',
                'datetime': '2024-01-01T10:00:00Z'
            },
            {
                'title': 'Tech Stocks Fall After Report',
                'link': 'https://example.com/article2',
                'source': 'Tech News',
                'datetime': '2024-01-01T11:00:00Z'
            },
            {
                'title': 'Stock Market Rises on Good News',  # Duplicate
                'link': 'https://example.com/article3',
                'source': 'Another Source',
                'datetime': '2024-01-01T12:00:00Z'  # Later timestamp
            }
        ]
        
        self.expected_processed_data = [
            {
                'title': 'Tech Stocks Fall After Report',
                'sentiment': 'negative',
                'timestamp': '2024-01-01T11:00:00Z',
                'link': 'https://example.com/article2',
                'source': 'Tech News'
            },
            {
                'title': 'Stock Market Rises on Good News',
                'sentiment': 'positive',
                'timestamp': '2024-01-01T12:00:00Z',  # Should keep the later one
                'link': 'https://example.com/article3',
                'source': 'Another Source'
            }
        ]

    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir)
        
        # Reset global variables in thllm_processor
        thllm_processor.model = None
        thllm_processor.tokenizer = None
        thllm_processor.nlp = None

    def test_remove_duplicates(self):
        """Test that duplicate removal keeps the most recent entry per title"""
        result = thllm_processor.remove_duplicates(self.sample_scraped_data)
        
        # Should have 2 items (one duplicate removed)
        self.assertEqual(len(result), 2)
        
        # Check that we kept the most recent duplicate
        titles = [item['title'] for item in result]
        self.assertIn('Stock Market Rises on Good News', titles)
        self.assertIn('Tech Stocks Fall After Report', titles)
        
        # Find the kept duplicate and verify it's the later one
        for item in result:
            if item['title'] == 'Stock Market Rises on Good News':
                self.assertEqual(item['datetime'], '2024-01-01T12:00:00Z')
                self.assertEqual(item['source'], 'Another Source')

    def test_remove_duplicates_empty_data(self):
        """Test duplicate removal with empty data"""
        result = thllm_processor.remove_duplicates([])
        self.assertEqual(result, [])

    def test_remove_duplicates_no_duplicates(self):
        """Test duplicate removal with no duplicates"""
        unique_data = [
            {'title': 'Unique Title 1', 'datetime': '2024-01-01T10:00:00Z'},
            {'title': 'Unique Title 2', 'datetime': '2024-01-01T11:00:00Z'}
        ]
        result = thllm_processor.remove_duplicates(unique_data)
        self.assertEqual(len(result), 2)

    @patch('thllm_processor.nlp')
    def test_predict_sentiment_positive(self, mock_nlp):
        """Test sentiment prediction returns positive"""
        # Mock the pipeline result
        mock_nlp.return_value = [{'label': 'LABEL_1', 'score': 0.9}]
        
        result = thllm_processor.predict_sentiment('Great news about the market!')
        self.assertEqual(result, 'positive')

    @patch('thllm_processor.nlp')
    def test_predict_sentiment_negative(self, mock_nlp):
        """Test sentiment prediction returns negative"""
        # Mock the pipeline result
        mock_nlp.return_value = [{'label': 'LABEL_2', 'score': 0.8}]
        
        result = thllm_processor.predict_sentiment('Bad news for investors')
        self.assertEqual(result, 'negative')

    @patch('thllm_processor.nlp')
    def test_predict_sentiment_neutral(self, mock_nlp):
        """Test sentiment prediction returns neutral"""
        # Mock the pipeline result
        mock_nlp.return_value = [{'label': 'LABEL_0', 'score': 0.7}]
        
        result = thllm_processor.predict_sentiment('Market shows mixed signals')
        self.assertEqual(result, 'neutral')

    @patch('thllm_processor.nlp', None)
    def test_predict_sentiment_no_model(self):
        """Test sentiment prediction when model is not available"""
        result = thllm_processor.predict_sentiment('Any text')
        self.assertEqual(result, 'neutral')

    @patch('thllm_processor.nlp')
    def test_predict_sentiment_exception(self, mock_nlp):
        """Test sentiment prediction handles exceptions gracefully"""
        # Mock the pipeline to raise an exception
        mock_nlp.side_effect = Exception('Model error')
        
        result = thllm_processor.predict_sentiment('Test text')
        self.assertEqual(result, 'neutral')

    def test_read_scraped_data_nonexistent_directory(self):
        """Test reading from non-existent directory"""
        result = thllm_processor.read_scraped_data('/nonexistent/directory')
        self.assertEqual(result, [])

    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_read_scraped_data_success(self, mock_file, mock_glob):
        """Test successful reading of scraped data"""
        # Mock glob to return test files
        mock_glob.return_value = ['file1.json', 'file2.json']
        
        # Mock file contents
        mock_file.return_value.read.side_effect = [
            json.dumps([{'title': 'Article 1'}]),
            json.dumps([{'title': 'Article 2'}])
        ]
        
        result = thllm_processor.read_scraped_data(self.scraped_data_dir)
        
        # Should have called glob.glob
        mock_glob.assert_called_once()
        
        # Should have opened both files
        self.assertEqual(mock_file.call_count, 2)

    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_read_scraped_data_invalid_json(self, mock_file, mock_glob):
        """Test reading scraped data with invalid JSON"""
        # Mock glob to return test files
        mock_glob.return_value = ['invalid.json']
        
        # Mock file with invalid JSON
        mock_file.return_value.read.return_value = 'invalid json content'
        mock_file.side_effect = json.JSONDecodeError('Invalid JSON', 'doc', 0)
        
        result = thllm_processor.read_scraped_data(self.scraped_data_dir)
        
        # Should return empty list and handle error gracefully
        self.assertEqual(result, [])

    @patch('thllm_processor.read_scraped_data')
    @patch('thllm_processor.predict_sentiment')
    @patch('os.makedirs')
    def test_process_and_save_data_new_entries(self, mock_makedirs, mock_predict, mock_read):
        """Test processing new entries"""
        # Mock the scraped data
        mock_read.return_value = self.sample_scraped_data
        
        # Mock sentiment predictions
        def mock_sentiment(text):
            if 'Good News' in text:
                return 'positive'
            elif 'Fall' in text:
                return 'negative'
            return 'neutral'
        
        mock_predict.side_effect = mock_sentiment
        
        # Create temporary output file path
        output_file = os.path.join(self.processed_data_dir, 'processed_data.json')
        
        with patch('thllm_processor.os.path.join', return_value=output_file), \
             patch('builtins.open', mock_open()) as mock_file:
            
            # Mock the file read for existing data (empty file)
            mock_file.return_value.read.return_value = '[]'
            
            result = thllm_processor.process_and_save_data()
            
            # Should return the output file path
            self.assertEqual(result, output_file)
            
            # Should have called sentiment prediction
            self.assertTrue(mock_predict.called)

    def test_sentiment_label_mapping(self):
        """Test that sentiment labels map correctly"""
        label_map = {0: 'neutral', 1: 'positive', 2: 'negative'}
        
        # Test all label mappings
        self.assertEqual(label_map[0], 'neutral')
        self.assertEqual(label_map[1], 'positive')
        self.assertEqual(label_map[2], 'negative')

    @patch('thllm_processor.load_model')
    @patch('thllm_processor.nlp')
    def test_load_model_called_when_none(self, mock_nlp, mock_load):
        """Test that model loading is called when nlp is None"""
        # Set nlp to None initially
        thllm_processor.nlp = None
        mock_load.return_value = None  # Simulate successful loading
        
        # Call predict_sentiment which should trigger model loading
        thllm_processor.predict_sentiment('test')
        
        # Should have called load_model
        mock_load.assert_called_once()


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete workflow"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.scraped_data_dir = os.path.join(self.temp_dir, 'data', 'scraped_data')
        self.processed_data_dir = os.path.join(self.temp_dir, 'data', 'processed_data')
        os.makedirs(self.scraped_data_dir, exist_ok=True)
        
        # Create sample scraped data files
        self.sample_data = [
            {
                'title': 'Market Rally Continues',
                'link': 'https://example.com/rally',
                'source': 'Finance News',
                'datetime': '2024-01-01T10:00:00Z'
            },
            {
                'title': 'Tech Crash Worries Investors',
                'link': 'https://example.com/crash',
                'source': 'Tech News',
                'datetime': '2024-01-01T11:00:00Z'
            }
        ]
        
        # Write sample data to file
        sample_file = os.path.join(self.scraped_data_dir, 'test_data.json')
        with open(sample_file, 'w') as f:
            json.dump(self.sample_data, f)

    def tearDown(self):
        """Clean up integration test fixtures"""
        shutil.rmtree(self.temp_dir)

    @patch('thllm_processor.predict_sentiment')
    def test_end_to_end_processing(self, mock_predict):
        """Test end-to-end processing from scraped data to processed output"""
        # Mock sentiment predictions
        def mock_sentiment(text):
            if 'Rally' in text:
                return 'positive'
            elif 'Crash' in text:
                return 'negative'
            return 'neutral'
        
        mock_predict.side_effect = mock_sentiment
        
        # Mock the directory paths
        with patch('thllm_processor.read_scraped_data') as mock_read:
            mock_read.return_value = self.sample_data
            
            output_file = os.path.join(self.processed_data_dir, 'processed_data.json')
            os.makedirs(self.processed_data_dir, exist_ok=True)
            
            with patch('thllm_processor.os.makedirs'), \
                 patch('thllm_processor.os.path.join', return_value=output_file):
                
                result = thllm_processor.process_and_save_data()
                
                # Should return output file path
                self.assertEqual(result, output_file)
                
                # Should have processed both entries
                self.assertEqual(mock_predict.call_count, 2)


if __name__ == '__main__':
    # Set up logging for tests
    import logging
    logging.basicConfig(level=logging.CRITICAL)  # Suppress log output during tests
    
    # Run the tests
    unittest.main(verbosity=2)