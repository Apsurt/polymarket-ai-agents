import requests
import logging
import time
import sys
import os
from datetime import datetime, timezone

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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
        self.polymarket_api_url = "https://gamma-api.polymarket.com/markets"

    def _fetch_data(self) -> list[dict]:
        current_iso_time_utc = datetime.now(timezone.utc).isoformat()
        self.logger.info(f"Fetching market data from {self.polymarket_api_url} for internal category '{self.category}'")
        self.logger.info(f"Using API filters: active=True, closed=False, end_date_gte={current_iso_time_utc}, order=volume")
        params = {
            'active': True, 'closed': False, 'end_date_gte': current_iso_time_utc,
            'limit': 100, 'order': 'volume', 'ascending': False,
        }
        markets_from_api = []
        try:
            response = requests.get(self.polymarket_api_url, params=params, timeout=15)
            self.logger.debug(f"Requesting URL: {response.url}")
            response.raise_for_status()
            data = response.json()
            markets_from_api = data if isinstance(data, list) else data.get("data", [])
            markets_from_api = markets_from_api or [] # Ensure it's a list
            self.logger.info(f"Fetched {len(markets_from_api)} markets from API after applying initial API filters.")
            if markets_from_api:
                self.logger.info(f"Logging the first fetched market object for inspection: {markets_from_api[0]}")
                unique_original_categories = set(m.get("category") for m in markets_from_api if m.get("category") and isinstance(m.get("category"), str))
                if unique_original_categories:
                    self.logger.info(f"Unique values from original 'category' field in API markets: {unique_original_categories}")
                else:
                    self.logger.info(f"Original 'category' field was mostly null, empty, or not found in fetched API markets.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {self.source_name}, category {self.category}: {e}", exc_info=True)
            if hasattr(e, 'response') and e.response is not None: self.logger.error(f"API Error Response: {e.response.text}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error in _fetch_data for {self.source_name}, category {self.category}: {e}", exc_info=True)
            return []

        filtered_markets = []
        for market in markets_from_api:
            text_for_keywords = market.get("question", "")
            if not isinstance(text_for_keywords, str): text_for_keywords = ""
            if not text_for_keywords:
                self.logger.debug(f"Market {market.get('id', 'N/A')} has no 'question' for keyword analysis. Skipping.")
                continue
            text_lower = text_for_keywords.lower()
            derived_internal_category = "miscellaneous"
            if any(kw in text_lower for kw in ["politic", "election", "government", "vote", "president", "senate", "congress", "biden", "trump", "white house"]): derived_internal_category = "political"
            elif any(kw in text_lower for kw in ["sport", "nfl", "nba", "mlb", "nhl", "soccer", "cup", "league", "game", "match", "player", "olympic", "esports", "football", "basketball", "baseball"]): derived_internal_category = "sports"
            elif any(kw in text_lower for kw in ["business", "econ", "finance", "stock", "market", "gdp", "fed", "nasdaq", "dow jones", "inflation", "recession", "interest rate", "earnings", "company"]): derived_internal_category = "economic"
            if derived_internal_category == self.category: filtered_markets.append(market)
        self.logger.info(f"Returning {len(filtered_markets)} markets for internal category '{self.category}' after keyword-based client-side filtering (using 'question' field).")
        return filtered_markets

    def _standardize_data(self, market: dict) -> dict | None:
        market_id_from_api = market.get("id")
        # CORRECTED: Use 'slug' from API
        market_slug_from_api = market.get("slug")
        market_key_identifier = market_id_from_api or market_slug_from_api

        if not market_key_identifier:
            logger.debug(f"Market item missing stable identifier. Skipping. Data: {market}")
            return None

        market_closed_flag_api = market.get("closed")
        market_is_active_bool = market.get("active")
        market_status_str = market.get("status") # This field seems to be often null in Gamma API
        # CORRECTED: Use 'endDate' from API
        ends_at_str_api = market.get("endDate")

        if market_closed_flag_api is True:
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'closed' is True). Title: {market.get('question', 'N/A')}")
            return None
        if market_is_active_bool is False:
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'active' is False). Title: {market.get('question', 'N/A')}")
            return None
        if market_is_active_bool is None and market_status_str is not None and market_status_str.lower() != "active":
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'active' N/A, 'status' is '{market_status_str}'). Title: {market.get('question', 'N/A')}")
            return None
        if market_is_active_bool is None and market_status_str is None and (not market_is_active_bool and not market_closed_flag_api): # If no status info at all, but active and not closed might be false
             logger.warning(f"Market {market_key_identifier} has no 'active' or 'status' field. Proceeding with caution if other checks pass.")

        if ends_at_str_api is None: # This check is now based on the correct 'endDate' field
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'endDate' field is null). Title: {market.get('question', 'N/A')}")
            return None

        try:
            if not isinstance(ends_at_str_api, str):
                raise ValueError(f"'endDate' not a string: {ends_at_str_api}")
            # API typically provides ISO 8601 with 'Z' or offset. Ensure tz-aware.
            if ends_at_str_api.endswith('Z'):
                ends_at_dt = datetime.fromisoformat(ends_at_str_api[:-1] + '+00:00')
            else:
                ends_at_dt = datetime.fromisoformat(ends_at_str_api)
            if ends_at_dt.tzinfo is None or ends_at_dt.tzinfo.utcoffset(ends_at_dt) is None:
                ends_at_dt = ends_at_dt.replace(tzinfo=timezone.utc) # Assume UTC if naive
            if ends_at_dt < datetime.now(timezone.utc):
                logger.debug(f"Skipping market {market_key_identifier} (Filter: 'endDate' in past: {ends_at_str_api}). Title: {market.get('question', 'N/A')}")
                return None
        except Exception as e: # Catch ValueError from fromisoformat or other issues
            logger.debug(f"Skipping market {market_key_identifier} (Filter: 'endDate' parse error '{ends_at_str_api}'): {e}. Title: {market.get('question', 'N/A')}")
            return None

        text_for_keywords_std = market.get("question", "")
        if not isinstance(text_for_keywords_std, str): text_for_keywords_std = ""
        standardized_category = "miscellaneous"
        text_lower_std = text_for_keywords_std.lower()
        if any(kw in text_lower_std for kw in ["politic", "election", "government", "vote", "president", "senate", "congress", "biden", "trump", "white house"]): standardized_category = "political"
        elif any(kw in text_lower_std for kw in ["sport", "nfl", "nba", "mlb", "nhl", "soccer", "cup", "league", "game", "match", "player", "olympic", "esports", "football", "basketball", "baseball"]): standardized_category = "sports"
        elif any(kw in text_lower_std for kw in ["business", "econ", "finance", "stock", "market", "gdp", "fed", "nasdaq", "dow jones", "inflation", "recession", "interest rate", "earnings", "company"]): standardized_category = "economic"

        original_api_category_field = market.get("category", "N/A") # Still useful to log original 'category' field value
        title = market.get("question", "N/A")
        # CORRECTED: use market_slug_from_api which is now from market.get("slug")
        constructed_url = f"https://polymarket.com/event/{market_slug_from_api or market_key_identifier}"

        standard_event_item = {
            "source": self.source_name, "event_type": "market_data_polymarket", "category": standardized_category, "content": market,
            "metadata": {
                "fetch_timestamp": time.time(), "market_id": str(market_key_identifier), "title": title, "url": constructed_url,
                "status_from_api": market_status_str, "active_flag_from_api": market_is_active_bool,
                "ends_at_from_api": ends_at_str_api, # Now correctly from 'endDate'
                "original_api_category_field": original_api_category_field,
                "text_used_for_categorization": text_for_keywords_std[:200]
            },
            "relevance_score": None,
        }

        # CORRECTED: Field names for snapshot data
        current_price = market.get("lastTradePrice")
        volume_for_snapshot = market.get("volume") # Using total volume; or market.get("volume24hr") for 24h volume

        market_snapshot_data = {
            "market_id_snapshot": str(market_key_identifier), "category_snapshot": standardized_category,
            "price_snapshot": float(current_price) if current_price is not None else None,
            "volume_snapshot": int(float(volume_for_snapshot)) if volume_for_snapshot is not None and str(volume_for_snapshot).strip() != "" else None,
            "market_data_snapshot": market
        }
        standard_event_item['_market_snapshot_specific'] = market_snapshot_data
        return standard_event_item

    def _persist_market_snapshot(self, market_snapshot_data: dict):
        try:
            market_id = market_snapshot_data.get('market_id_snapshot')
            if not market_id:
                logger.error(f"Market ID missing in snapshot. Skipping. Data: {market_snapshot_data}")
                return
            query = """
            INSERT INTO market_snapshots (market_id, source, category, price, volume, market_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (market_id) DO UPDATE SET
                source = EXCLUDED.source, category = EXCLUDED.category, price = EXCLUDED.price,
                volume = EXCLUDED.volume, market_data = EXCLUDED.market_data, timestamp = NOW();
            """
            params = (
                market_id, self.source_name, market_snapshot_data['category_snapshot'],
                market_snapshot_data['price_snapshot'], market_snapshot_data['volume_snapshot'],
                Json(market_snapshot_data['market_data_snapshot'])
            )
            execute_query(query, params, commit=True)
        except Exception as e:
            failed_market_id = market_snapshot_data.get('market_id_snapshot', 'UNKNOWN_ID')
            logger.error(f"Failed to persist market snapshot for market_id '{failed_market_id}'. Error: {e}", exc_info=True)

    def collect_and_store(self):
        standardized_items = super().collect()
        processed_raw_count, processed_snapshot_count = 0, 0
        if not standardized_items:
            self.logger.info(f"No items to store for category {self.category} after filtering and standardization.")
            return 0
        for item in standardized_items:
            if item:
                snapshot_data = item.pop('_market_snapshot_specific', None)
                try:
                    if not all(k in item for k in ['source', 'event_type', 'category', 'content']):
                        logger.error(f"Std item missing essential fields. Item: {item}")
                        continue
                    validate_and_store_raw_event(item)
                    processed_raw_count += 1
                except Exception as e:
                    logger.error(f"Error storing raw market event for market_id '{item.get('metadata', {}).get('market_id', 'N/A')}': {e}", exc_info=True)
                if snapshot_data:
                    try:
                        self._persist_market_snapshot(snapshot_data)
                        processed_snapshot_count += 1
                    except Exception: pass # Logged in _persist_market_snapshot
        if processed_raw_count > 0 or processed_snapshot_count > 0:
            self.logger.info(f"Processed {processed_raw_count} raw market events and {processed_snapshot_count} market snapshots for category {self.category}.")
        return processed_raw_count + processed_snapshot_count

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # logging.getLogger("MarketCollector").setLevel(logging.DEBUG) # For more verbose logs

    categories_to_run = ["political", "sports", "economic", "miscellaneous"]
    for category_name in categories_to_run:
        logger.info(f"Running market collector for category: {category_name}")
        try:
            collector = MarketCollector(category=category_name)
            items_processed = collector.collect_and_store()
            logger.info(f"Collector for category '{category_name}' processed {items_processed} items.")
        except Exception as e:
            logger.error(f"Unhandled error for category '{category_name}': {e}", exc_info=True)
        if len(categories_to_run) > 1:
            time.sleep(2)
