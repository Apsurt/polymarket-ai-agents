import requests
import logging
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from collectors.base import BaseCollector
from app.core.config import settings
from app.core.db import execute_query, Json
from workers.data_validator_worker import validate_and_store_raw_event

logger = logging.getLogger(__name__)

class MarketCollector(BaseCollector):
    CALLS = 30
    PERIOD = 60

    def __init__(self, category: str, market_source_name: str = "polymarket_api"):
        super().__init__(source_name=market_source_name, category=category)
        self.polymarket_api_url = "https://polymarket.com/api/v2/markets"
        # self.api_key = settings.POLYMARKET_API_KEY
        self.category_mapping = {
            "political": "politics", "sports": "sports",
            "economic": "economy", "miscellaneous": "misc"
        }
        self.query_category = self.category_mapping.get(category.lower(), "all")

    def _fetch_data(self) -> list[dict]:
        self.logger.info(f"Fetching market data for category: {self.category} from {self.polymarket_api_url}")
        params = {'status': 'open', 'category': self.query_category, 'limit': 100}
        try:
            response = requests.get(self.polymarket_api_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            markets = data.get("data", [])
            self.logger.info(f"Found {len(markets)} markets for category: {self.category}.")
            # Deduplication, historical backfill, etc., would be implemented here or in separate methods.
            return markets
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {self.source_name} ({self.category}): {e}", exc_info=True)
            return []

    def _standardize_data(self, market: dict) -> dict:
        market_id = market.get("id") or market.get("slug")
        title = market.get("title", "N/A")
        current_price = market.get("currentPrice")
        volume_usd = market.get("volumeUsd")
        polymarket_category = market.get("category", "miscellaneous").lower()
        if polymarket_category in ["politics", "political"]: standardized_category = "political"
        elif polymarket_category in ["sports", "gaming"]: standardized_category = "sports"
        elif polymarket_category in ["economy", "business", "finance"]: standardized_category = "economic"
        else: standardized_category = "miscellaneous"

        # General standardized item for raw_data_events
        standard_event_item = {
            "source": self.source_name,
            "event_type": "market_data_polymarket", # More specific event_type
            "category": standardized_category,
            "content": market, # Store the full raw market data
            "metadata": {
                "fetch_timestamp": time.time(), "market_id": market_id, "title": title,
                "url": market.get("url"), "status": market.get("status"),
                "ends_at": market.get("endsAt")
            },
            "relevance_score": None
        }

        # Data specific for market_snapshots table
        market_snapshot_data = {
            "market_id_snapshot": str(market_id),
            "category_snapshot": standardized_category,
            "price_snapshot": float(current_price) if current_price is not None else None, # [cite: 117]
            "volume_snapshot": int(volume_usd) if volume_usd is not None else None, # [cite: 117]
            "market_data_snapshot": market
        }
        # Combine them for processing within the collect_and_store method
        standard_event_item['_market_snapshot_specific'] = market_snapshot_data
        return standard_event_item


    def _persist_market_snapshot(self, market_snapshot_data: dict):
        """Persists a single market snapshot to the database."""
        try:
            query = """
            INSERT INTO market_snapshots (market_id, category, price, volume, market_data)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (market_id) DO UPDATE SET
                category = EXCLUDED.category, price = EXCLUDED.price, volume = EXCLUDED.volume, # [cite: 120]
                market_data = EXCLUDED.market_data, timestamp = NOW(); # [cite: 121]
            """
            execute_query(query, (
                market_snapshot_data['market_id_snapshot'], market_snapshot_data['category_snapshot'],
                market_snapshot_data['price_snapshot'], market_snapshot_data['volume_snapshot'],
                Json(market_snapshot_data['market_data_snapshot'])
            ), commit=True)
            logger.info(f"Inserted/Updated market snapshot for market ID {market_snapshot_data['market_id_snapshot']} in DB.")
        except Exception as e:
            logger.error(f"Failed to insert/update market snapshot into DB: {market_snapshot_data['market_id_snapshot']}. Error: {e}")

    def collect_and_store(self):
        """Collects, standardizes, and stores market data."""
        collected_items = super().collect() # Calls BaseCollector's collect to get standardized items

        processed_raw_count = 0
        processed_snapshot_count = 0

        for item in collected_items:
            if item:
                snapshot_specific_data = item.pop('_market_snapshot_specific', None)

                # Store the main event data in raw_data_events
                try:
                    validate_and_store_raw_event(item)
                    processed_raw_count += 1
                except Exception as e:
                    logger.error(f"Error storing raw market event for {item.get('metadata', {}).get('market_id')}: {e}")

                # Store market snapshot data
                if snapshot_specific_data:
                    try:
                        self._persist_market_snapshot(snapshot_specific_data)
                        processed_snapshot_count +=1
                    except Exception as e:
                         logger.error(f"Error persisting market snapshot for {snapshot_specific_data.get('market_id_snapshot')}: {e}")

        logger.info(f"Processed {processed_raw_count} raw market events and {processed_snapshot_count} market snapshots.")
        return processed_raw_count + processed_snapshot_count


if __name__ == "__main__":
    collectors_to_run = [
        MarketCollector(category="political"),
        MarketCollector(category="sports"),
        MarketCollector(category="economic"),
        MarketCollector(category="miscellaneous")
    ]
    for collector_instance in collectors_to_run:
        logger.info(f"Running market collector for category: {collector_instance.category}")
        collector_instance.collect_and_store()
        time.sleep(1) # Small delay
