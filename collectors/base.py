import time
import logging
import sys
import os
from abc import ABC, abstractmethod
from redis import Redis
from rq import Queue
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, RateLimitException

# Add the project root to the Python path if not already there
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.core.config import settings
from app.core.db import execute_query

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_conn = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)

QUEUES = {
    'data_raw': Queue('data.raw', connection=redis_conn),
    'data_validation': Queue('data.validation', connection=redis_conn),
    'data_breaking': Queue('data.breaking', connection=redis_conn),
}

class BaseCollector(ABC):
    # Default rate limit values. Subclasses can override these.
    CALLS = 5
    PERIOD = 60  # seconds

    def __init__(self, source_name: str, category: str):
        self.source_name = source_name
        self.category = category
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    @limits(calls=CALLS, period=PERIOD) # Apply the decorator here
    def _fetch_data(self) -> list[dict]:
        """
        Core logic to fetch data from the source.
        Must be implemented by subclasses.
        Should return a list of data items (dictionaries).
        Rate limiting is applied here.
        """
        pass

    def _standardize_data(self, item: dict) -> dict:
        return {
            "source": self.source_name,
            "event_type": "generic_event",
            "category": self.category,
            "content": item,
            "metadata": {"fetch_timestamp": time.time()},
            "relevance_score": None,
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def collect(self):
        """
        Orchestrates the data collection process with error handling and retries.
        Rate limiting is now handled by the decorator on _fetch_data.
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
                    standardized_items.append(standardized_item)
                except Exception as e:
                    self.logger.error(f"Error standardizing item from {self.source_name}: {item}. Error: {e}")

            self.publish_to_queue(standardized_items)
            self.logger.info(f"Successfully collected and queued {len(standardized_items)} items from {self.source_name}, category: {self.category}")
            return standardized_items

        except RateLimitException as rle:
            self.logger.warning(f"Rate limit exceeded for {self.source_name}. Waiting for {rle.period_remaining} seconds.")
            time.sleep(rle.period_remaining)
            raise # Re-raise to allow retry mechanism to work if not exhausted
        except Exception as e:
            self.logger.error(f"Unhandled error collecting data for {self.source_name}, category: {self.category}. Error: {e}", exc_info=True)
            raise # Re-raise to allow retry mechanism to work if not exhausted

    def publish_to_queue(self, data_items: list[dict], queue_name: str = 'data_raw'):
        """
        Publishes standardized data items to the specified Redis Queue.
        """
        target_queue = QUEUES.get(queue_name)
        if not target_queue:
            self.logger.error(f"Queue '{queue_name}' not found. Available queues: {list(QUEUES.keys())}")
            return

        for item in data_items:
            try:
                # The job for the worker could be a function path string and arguments
                # e.g., 'workers.data_validator_worker.process_raw_event'
                # For now, we'll just enqueue the data item itself. The worker will know how to handle it.
                target_queue.enqueue('workers.data_validator_worker.validate_and_store_raw_event', item)
                self.logger.debug(f"Enqueued item to '{queue_name}': {item.get('id', 'N/A')}")
            except Exception as e:
                self.logger.error(f"Failed to enqueue item to '{queue_name}': {item}. Error: {e}")

    def health_check(self) -> dict:
        """
        Basic health check method.
        Could be expanded to check API connectivity, etc.
        """
        return {"status": "healthy", "source": self.source_name, "category": self.category, "timestamp": time.time()}

    def update_source_reliability(self, events_processed_count: int, successful_prediction_count: int = 0, accuracy: float = 0.0):
        """
        Updates the source_reliability table.
        This might be called after a batch processing or based on downstream analysis feedback.
        """
        query = """
        INSERT INTO source_reliability (source, category, total_events_processed, successful_predictions, accuracy_score, last_updated)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (source, category) DO UPDATE SET
            total_events_processed = source_reliability.total_events_processed + EXCLUDED.total_events_processed,
            successful_predictions = source_reliability.successful_predictions + EXCLUDED.successful_predictions,
            -- Accuracy might need a more complex update logic
            accuracy_score = EXCLUDED.accuracy_score,
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
