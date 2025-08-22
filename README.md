# MI-3 News Scraper

A production-ready news scraping and sentiment analysis system for financial news from Yahoo Finance and Google News.

## ✨ Features

- **Multi-Source Scraping**: Yahoo Finance and Google News
- **Sentiment Analysis**: AI-powered sentiment classification (positive/neutral/negative)
- **Deduplication**: Intelligent duplicate removal with timestamp-based preference
- **Stealth Scraping**: Anti-detection measures for reliable data collection
- **Modular Architecture**: Clean, maintainable codebase
- **Production Ready**: Comprehensive logging, error handling, and configuration management

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Chrome/Chromium browser
- Internet connection

### Installation

1. **Clone and setup:**
```bash
git clone <repository>
cd MI-3
pip install -r requirements-base.txt  # Basic scraping only
# OR
pip install -r requirement.txt        # Full functionality with ML
```

2. **Basic scraping (no ML dependencies):**
```bash
python main.py --all
```

3. **With sentiment analysis:**
```bash
python main.py --all --process-sentiment
```

## 📖 Usage

### Command Line Interface

```bash
# Run all scrapers
python main.py --all

# Run specific scrapers
python main.py --yahoo
python main.py --google

# Add sentiment analysis
python main.py --all --process-sentiment

# Debug and update selectors
python main.py --debug
python main.py --update-selectors

# Show version
python main.py --version
```

### Standalone Components

```bash
# Sentiment processing only
python thllm_processor.py

# Individual scrapers
python yai_scraper.py
python ai_scraper.py
```

## 📁 Project Structure

```
MI-3/
├── main.py                 # Main entry point
├── config.py               # Centralized configuration
├── thllm_processor.py      # Sentiment analysis & deduplication
├── yai_scraper.py          # Yahoo Finance scraper
├── ai_scraper.py           # Google News scraper
├── scraper_debug.py        # Debug utilities
├── selector_utils.py       # CSS selector utilities
├── test_thllm_processor.py # Unit tests
├── data/
│   ├── scraped_data/       # Raw scraped JSON files
│   └── processed_data/     # Sentiment-analyzed data
├── logs/                   # Application logs
├── debug/                  # Debug HTML & selectors
└── requirements*.txt       # Dependencies
```

## 🔧 Configuration

### Environment Options

- **Basic Scraping**: `requirements-base.txt` (~50MB)
- **Full ML Support**: `requirement.txt` (~600MB, includes PyTorch)

### Key Settings

- **Model**: fuchenru/Trading-Hero-LLM for financial sentiment
- **Browser**: Headless Chrome with stealth mode
- **Output**: Timestamped JSON files with structured data
- **Logging**: Multi-level logging with file rotation

## 📊 Output Format

### Scraped Data
```json
{
  "title": "Market News Headline",
  "link": "https://source.com/article",
  "source": "News Source",
  "datetime": "2024-01-01T10:00:00Z"
}
```

### Processed Data (with sentiment)
```json
{
  "title": "Market News Headline",
  "sentiment": "positive|neutral|negative",
  "timestamp": "2024-01-01T10:00:00Z",
  "link": "https://source.com/article",
  "source": "News Source"
}
```

## 🛡️ Production Features

- **Error Recovery**: Graceful handling of network issues and site changes
- **Anti-Detection**: Selenium stealth mode with randomized headers
- **Resource Management**: Automatic model loading with fallback options
- **Data Integrity**: Duplicate detection and incremental processing
- **Monitoring**: Comprehensive logging and error reporting

## 🧪 Testing

```bash
# Run unit tests
python -m unittest test_thllm_processor.py

# Test basic functionality
python -c "import thllm_processor; print('✓ Import successful')"

# Verify CLI
python main.py --help
```

## 🔍 Troubleshooting

### Common Issues

1. **Chrome/ChromeDriver**: Selenium manager handles driver installation automatically
2. **ML Dependencies**: Use `requirements-base.txt` for scraping-only functionality
3. **Memory Usage**: Sentiment analysis requires ~2GB RAM for model loading
4. **Rate Limiting**: Built-in delays and stealth mode reduce detection risk

### Logs Location

- `logs/main.log` - Main application logs
- `logs/yai_scraper.log` - Yahoo scraper logs  
- `logs/ai_scraper.log` - Google scraper logs
- `logs/thllm_processor.log` - Sentiment analysis logs

## 📈 Performance

- **Scraping Speed**: ~50-100 articles/minute
- **Sentiment Analysis**: ~10-20 articles/second
- **Memory Usage**: 
  - Base: ~100MB
  - With ML: ~2GB (model loading)
- **Storage**: ~1MB per 1000 articles

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Run tests: `python -m unittest discover`
4. Submit pull request

## 📄 License

This project is for educational and research purposes. Ensure compliance with website terms of service and rate limiting guidelines.

---

**Version**: 1.0.0 | **Status**: Production Ready