import requests
import logging
import time
import sys
import os
from datetime import datetime, timezone

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
        self.polymarket_api_url = "https://clob.polymarket.com/markets"
        self.category_mapping = {
            "political": "Politics", "sports": "Sports",
            "economic": "Business", "miscellaneous": "Miscellaneous"
        }
        self.query_category = self.category_mapping.get(category.lower(), category)

    def _fetch_data(self) -> list[dict]:
        self.logger.info(f"Fetching market data for category: {self.category} (querying as: {self.query_category}) from {self.polymarket_api_url}")
        # Added sorting parameters: order_by volume, descending
        params = {
            'status': 'active',
            'limit': 100,
            'order_by': 'volume_usd_24h', # Sort by 24h USD volume
            'order_direction': 'desc'      # Get highest volume first
        }
        if self.query_category and self.query_category.lower() != "all":
            params['category'] = self.query_category

        try:
            response = requests.get(self.polymarket_api_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            markets = data if isinstance(data, list) else data.get("data", [])
            self.logger.info(f"Found {len(markets)} markets for category: {self.category} (query: {self.query_category}, sorted by volume).")
            return markets
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {self.source_name} ({self.category}, query: {self.query_category}): {e}", exc_info=True)
            return []

    # ... (the _standardize_data, _persist_market_snapshot, collect_and_store methods remain the same as the previous version with strict filters)
    def _standardize_data(self, market: dict) -> dict | None:
        market_id_from_api = market.get("id")
        market_slug_from_api = market.get("market_slug")

        market_key_identifier = None
        if market_id_from_api:
            market_key_identifier = market_id_from_api
        elif market_slug_from_api:
            market_key_identifier = market_slug_from_api

        if not market_key_identifier:
            logger.debug( # Changed to debug to reduce noise if many items are like this from source
                f"Market item missing both an 'id' field and a 'market_slug' field. "
                f"Cannot determine a stable market identifier. Skipping. Market data: {market}"
            )
            return None

        # --- Additional Filtering Logic ---
        market_status_api = market.get("status")
        market_closed_flag_api = market.get("closed")
        ends_at_str_api = market.get("ends_at")

        if market_closed_flag_api is True:
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'closed' flag is true). Title: {market.get('question', 'N/A')}")
            return None

        if market_status_api is None:
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'status' field is null). Title: {market.get('question', 'N/A')}")
            return None
        if market_status_api.lower() != "active":
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'status' field is '{market_status_api}'). Title: {market.get('question', 'N/A')}")
            return None

        if ends_at_str_api is None:
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'ends_at' date is null). Title: {market.get('question', 'N/A')}")
            return None

        try:
            if ends_at_str_api.endswith('Z'):
                ends_at_dt = datetime.fromisoformat(ends_at_str_api[:-1] + '+00:00')
            else:
                ends_at_dt = datetime.fromisoformat(ends_at_str_api)

            if ends_at_dt.tzinfo is None or ends_at_dt.tzinfo.utcoffset(ends_at_dt) is None:
                logger.debug(f"Skipping market {market_key_identifier} (Filter: 'ends_at' date '{ends_at_str_api}' is naive). Title: {market.get('question', 'N/A')}")
                return None

            now_utc = datetime.now(timezone.utc)
            if ends_at_dt < now_utc:
                logger.debug(f"Skipping market {market_key_identifier} (Filter: 'ends_at' date {ends_at_str_api} is in the past). Title: {market.get('question', 'N/A')}")
                return None
        except ValueError as e:
            logger.debug(f"Skipping market {market_key_identifier} (Filter: Could not parse 'ends_at' date string '{ends_at_str_api}'). Error: {e}. Title: {market.get('question', 'N/A')}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing 'ends_at' date for market {market_key_identifier}. Date: '{ends_at_str_api}'. Error: {e}. Skipping market.")
            return None

        event_slug_for_url = market_slug_from_api
        title = market.get("question", "N/A")
        current_price = market.get("last_price")
        volume_usd = market.get("volume_usd_24h")
        polymarket_api_category = market.get("category", "miscellaneous")

        standardized_category = "miscellaneous"
        if isinstance(polymarket_api_category, str):
            if "politic" in polymarket_api_category.lower(): standardized_category = "political"
            elif "sport" in polymarket_api_category.lower(): standardized_category = "sports"
            elif "business" in polymarket_api_category.lower() or "econ" in polymarket_api_category.lower(): standardized_category = "economic"
            elif "crypto" in polymarket_api_category.lower(): standardized_category = "miscellaneous"

        constructed_url = "URL not available"
        if event_slug_for_url:
            constructed_url = f"https://polymarket.com/event/{event_slug_for_url}"
        elif market_key_identifier:
            constructed_url = f"https://polymarket.com/event/{market_key_identifier}"

        standard_event_item = {
            "source": self.source_name,
            "event_type": "market_data_polymarket",
            "category": standardized_category,
            "content": market,
            "metadata": {
                "fetch_timestamp": time.time(),
                "market_id": market_key_identifier,
                "title": title,
                "url": constructed_url,
                "status": market_status_api,
                "ends_at": ends_at_str_api,
                "api_category": polymarket_api_category
            },
            "relevance_score": None
        }

        market_snapshot_data = {
            "market_id_snapshot": str(market_key_identifier),
            "category_snapshot": standardized_category,
            "price_snapshot": float(current_price) if current_price is not None else None,
            "volume_snapshot": int(float(volume_usd)) if volume_usd is not None and volume_usd != "" else None,
            "market_data_snapshot": market
        }
        standard_event_item['_market_snapshot_specific'] = market_snapshot_data
        return standard_event_item

    def _persist_market_snapshot(self, market_snapshot_data: dict):
        try:
            if not market_snapshot_data.get('market_id_snapshot'):
                logger.error(f"Market ID is missing in snapshot_specific_data. Skipping persistence. Data: {market_snapshot_data}")
                return

            query = """
            INSERT INTO market_snapshots (market_id, source, category, price, volume, market_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (market_id) DO UPDATE SET
                source = EXCLUDED.source,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                volume = EXCLUDED.volume,
                market_data = EXCLUDED.market_data,
                timestamp = NOW();
            """
            params = (
                market_snapshot_data['market_id_snapshot'],
                self.source_name,
                market_snapshot_data['category_snapshot'],
                market_snapshot_data['price_snapshot'],
                market_snapshot_data['volume_snapshot'],
                Json(market_snapshot_data['market_data_snapshot'])
            )
            execute_query(query, params, commit=True)
        except Exception as e:
            failed_market_id = market_snapshot_data.get('market_id_snapshot', 'UNKNOWN_ID')
            logger.error(f"Failed to insert/update market snapshot into DB for market_id '{failed_market_id}'. Error: {e}", exc_info=True)


    def collect_and_store(self):
        collected_items = super().collect()

        processed_raw_count = 0
        processed_snapshot_count = 0

        for item in collected_items:
            if item:
                snapshot_specific_data = item.pop('_market_snapshot_specific', None)
                try:
                    if not all(k in item for k in ['source', 'event_type', 'category', 'content']):
                        logger.error(f"Standardized item missing essential fields for raw_data_events. Item: {item}")
                        continue
                    validate_and_store_raw_event(item)
                    processed_raw_count += 1
                except Exception as e:
                    logger.error(f"Error storing raw market event for {item.get('metadata', {}).get('market_id')}: {e}", exc_info=True)

                if snapshot_specific_data:
                    try:
                        self._persist_market_snapshot(snapshot_specific_data)
                        processed_snapshot_count +=1
                    except Exception as e:
                        pass

        if processed_raw_count > 0 or processed_snapshot_count > 0:
            logger.info(f"Processed {processed_raw_count} raw market events and {processed_snapshot_count} market snapshots for category {self.category} after filtering.")
        # Removed the "No new market data" log to reduce noise, as BaseCollector may log similar if _fetch_data returns empty.

        return processed_raw_count + processed_snapshot_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # For more detailed debugging of skipping logic, you might temporarily set level to DEBUG:
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('collectors.market_collector').setLevel(logging.DEBUG)


    categories_to_run = ["political", "sports", "economic", "miscellaneous"]

    for category_name in categories_to_run:
        logger.info(f"Running market collector for category: {category_name}")
        # Temporarily set logger level to DEBUG for the MarketCollector instance to see skip reasons
        # market_collector_logger = logging.getLogger('__main__.' + MarketCollector.__name__) # Or specific name if collector has its own named logger
        # original_level = market_collector_logger.level
        # market_collector_logger.setLevel(logging.DEBUG)

        collector_instance = MarketCollector(category=category_name)
        collector_instance.collect_and_store()

        # market_collector_logger.setLevel(original_level) # Reset level
        time.sleep(1)
