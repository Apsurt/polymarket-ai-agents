import requests
import logging
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.base import BaseCollector
from app.core.config import settings
from workers.data_validator_worker import validate_and_store_raw_event

logger = logging.getLogger(__name__)

class NewsCollector(BaseCollector):
    CALLS = 100
    PERIOD = 86400

    def __init__(self, category: str = "general", news_source_name: str = "news_api_org"):
        super().__init__(source_name=news_source_name, category=category)
        self.api_key = settings.NEWS_API_KEY
        category_mapping = {
            "political": "politics", "economic": "business", "sports": "sports",
            "technology": "technology", "health": "health", "science": "science",
            "entertainment": "entertainment"
        }
        api_category = category_mapping.get(category, "general")
        self.endpoint = "https://newsapi.org/v2/top-headlines"
        self.params = {'country': 'us', 'category': api_category, 'pageSize': 20, 'apiKey': self.api_key}
        if not self.api_key or self.api_key == "your_news_api_key_here":
            logger.warning("No valid NEWS_API_KEY found, using mock data")
            self.use_mock_data = True
        else:
            self.use_mock_data = False

    def _fetch_data(self) -> list[dict]:
        if self.use_mock_data:
            return self._get_mock_data()
        try:
            self.logger.info(f"Fetching news for category: {self.category}")
            response = requests.get(self.endpoint, params=self.params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('status') != 'ok':
                self.logger.error(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                return []
            articles = data.get("articles", [])
            self.logger.info(f"Fetched {len(articles)} articles for category: {self.category}")
            filtered_articles = [
                article for article in articles
                if article.get('title') != '[Removed]' and article.get('description') != '[Removed]'
            ]
            return filtered_articles
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {self.source_name} ({self.category}): {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error fetching news: {e}")
            return []

    def _get_mock_data(self) -> list[dict]:
        mock_articles = [
            {"source": {"id": "mock-news", "name": "Mock News"}, "author": "Test Author",
             "title": f"Breaking: Mock {self.category.title()} News Story",
             "description": f"This is a mock {self.category} news article for testing the data pipeline.",
             "url": "https://example.com/mock-news-1", "urlToImage": "https://via.placeholder.com/400x200",
             "publishedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
             "content": f"Mock content for {self.category} news article. This helps test the collector without API keys."},
            {"source": {"id": "mock-urgent", "name": "Mock Urgent News"}, "author": "Urgent Reporter",
             "title": f"Urgent: Important {self.category.title()} Development",
             "description": f"An urgent development in {self.category} that should trigger breaking news detection.",
             "url": "https://example.com/mock-news-2", "urlToImage": "https://via.placeholder.com/400x200",
             "publishedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
             "content": f"Urgent breaking news content for {self.category}. This should be flagged as high priority."}
        ]
        self.logger.info(f"Generated {len(mock_articles)} mock articles for category: {self.category}")
        return mock_articles

    def _standardize_data(self, article: dict) -> dict | None:
        title = article.get("title", "").strip()
        description = article.get("description", "").strip()
        if not title or not description or title == "[Removed]": return None
        return {
            "source": self.source_name, "event_type": "news_article",
            "category": self.category,
            "content": {"title": title, "description": description, "url": article.get("url"),
                        "author": article.get("author"), "publishedAt": article.get("publishedAt"),
                        "source_name": article.get("source", {}).get("name"),
                        "image_url": article.get("urlToImage"),
                        "full_content": article.get("content", "")[:500]},
            "metadata": {"fetch_timestamp": time.time(), "article_url": article.get("url"),
                         "published_at": article.get("publishedAt"),
                         "api_source_name": article.get("source", {}).get("name"),
                         "collector_version": "1.0"},
            "relevance_score": None,
        }

    def collect_and_store(self):
        """Collects, standardizes, and directly stores news articles."""
        try:
            self.logger.info(f"Starting collection for {self.source_name}, category: {self.category}")
            raw_items = self._fetch_data()

            if not raw_items:
                self.logger.info(f"No new data found for {self.source_name}, category: {self.category}")
                return 0

            processed_count = 0
            for item in raw_items:
                try:
                    standardized_item = self._standardize_data(item)
                    if standardized_item:
                        validate_and_store_raw_event(standardized_item)
                        processed_count += 1
                except Exception as e:
                    self.logger.error(f"Error standardizing or storing item from {self.source_name}: {e}")

            if processed_count > 0:
                 self.logger.info(f"Successfully collected and stored {processed_count} items from {self.source_name}")
            else:
                self.logger.info(f"No valid items to store from {self.source_name}")

            return processed_count

        except Exception as e:
            self.logger.error(f"Error in collect_and_store method for {self.source_name}: {e}")
            return 0

def run_all_categories():
    categories = ["political"]#, "economic", "sports", "technology", "health"]
    for category in categories:
        try:
            logger.info(f"Running news collector for category: {category}")
            collector = NewsCollector(category=category)
            results_count = collector.collect_and_store()
            logger.info(f"Collected and stored {results_count} items for {category}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error running collector for {category}: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_all_categories()
