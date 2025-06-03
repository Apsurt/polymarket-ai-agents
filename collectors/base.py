import time
import logging
import sys
import os
from abc import ABC, abstractmethod
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, RateLimitException

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.core.config import settings # Corrected import if needed
from app.core.db import execute_query

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseCollector(ABC):
    CALLS = 5
    PERIOD = 60

    def __init__(self, source_name: str, category: str):
        self.source_name = source_name
        self.category = category
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_data(self) -> list[dict]:
        """
        Core logic to fetch data from the source.
        Must be implemented by subclasses. # [cite: 50]
        Should return a list of data items (dictionaries).
        Rate limiting is applied here. # [cite: 50]
        """
        pass

    def _standardize_data(self, item: dict) -> dict:
        return {
            "source": self.source_name,
            "event_type": "generic_event",
            "category": self.category, # [cite: 51]
            "content": item,
            "metadata": {"fetch_timestamp": time.time()},
            "relevance_score": None,
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def collect(self) -> list[dict]:
        """
        Orchestrates the data collection process. # [cite: 52]
        Rate limiting is handled by the decorator on _fetch_data. # [cite: 52, 53]
        Returns a list of standardized data items.
        """
        try:
            self.logger.info(f"Fetching data for source: {self.source_name}, category: {self.category}")
            raw_items = self._fetch_data()

            if not raw_items:
                self.logger.info(f"No new data found for {self.source_name}, category: {self.category}")
                return []

            standardized_items = []
            for item in raw_items:
                try:
                    standardized_item = self._standardize_data(item)
                    if standardized_item: # Ensure item is not None
                        standardized_items.append(standardized_item)
                except Exception as e:
                    self.logger.error(f"Error standardizing item from {self.source_name}: {item}. Error: {e}")

            if standardized_items:
                self.logger.info(f"Successfully collected {len(standardized_items)} items from {self.source_name}, category: {self.category}")

            return standardized_items

        except RateLimitException as rle:
            self.logger.warning(f"Rate limit exceeded for {self.source_name}. Waiting for {rle.period_remaining} seconds.") # [cite: 56]
            time.sleep(rle.period_remaining)
            raise
        except Exception as e:
            self.logger.error(f"Unhandled error collecting data for {self.source_name}, category: {self.category}. Error: {e}", exc_info=True)
            raise

    def update_source_reliability(self, events_processed_count: int, successful_prediction_count: int = 0, accuracy: float = 0.0):
        """
        Updates the source_reliability table.
        This might be called after a batch processing or based on downstream analysis feedback. # [cite: 62, 63]
        """
        query = """
        INSERT INTO source_reliability (source, category, total_events_processed, successful_predictions, accuracy_score, last_updated)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (source, category) DO UPDATE SET
            total_events_processed = source_reliability.total_events_processed + EXCLUDED.total_events_processed,
            successful_predictions = source_reliability.successful_predictions + EXCLUDED.successful_predictions,
            accuracy_score = EXCLUDED.accuracy_score, # [cite: 64]
            last_updated = NOW();
        """
        try:
            execute_query(
                query,
                (self.source_name, self.category, events_processed_count, successful_prediction_count, accuracy),
                commit=True
            )
            self.logger.info(f"Updated reliability for source: {self.source_name}, category: {self.category}")
        except Exception as e:
            self.logger.error(f"Failed to update source reliability for {self.source_name}, category: {self.category}. Error: {e}")
