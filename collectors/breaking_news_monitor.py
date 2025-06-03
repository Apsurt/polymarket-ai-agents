import logging
import time
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)

from collectors.base import BaseCollector
from app.core.db import execute_query, Json

logger = logging.getLogger(__name__)

class BreakingNewsMonitor(BaseCollector):
    CALLS = 12
    PERIOD = 3600

    def __init__(self):
        super().__init__(source_name="breaking_news_monitor", category="monitoring")
        # self.llm_client = CheapLLMClient(model="gpt-3.5-turbo")

    def _fetch_data(self) -> list[dict]:
        self.logger.info("Checking for potential breaking news from recent raw_data_events...")
        try:
            query = """
            SELECT id, source, event_type, category, content, created_at FROM raw_data_events
            WHERE processed = FALSE AND created_at > NOW() - INTERVAL '15 minutes'
            ORDER BY created_at DESC LIMIT 20;
            """
            recent_events_tuples = execute_query(query, fetch_all=True)
            if not recent_events_tuples: return []
            recent_events = []
            for row in recent_events_tuples:
                recent_events.append({"id": row[0], "source": row[1], "event_type": row[2],
                                      "category_guess": row[3], "content": row[4], "created_at": row[5]})
            return recent_events
        except Exception as e:
            self.logger.error(f"Error fetching recent events for breaking news monitor: {e}")
            return []

    def _standardize_data(self, event_to_assess: dict) -> dict | None:
        # Simplified non-LLM version
        content_str = str(event_to_assess.get('content', '')).lower()
        is_breaking_flag = "breaking" in content_str or "urgent" in content_str or "alert" in content_str
        urgency = 0
        if "breaking" in content_str: urgency = max(urgency, 7)
        if "urgent" in content_str: urgency = max(urgency, 8)
        if "alert" in content_str: urgency = max(urgency, 9)
        if "flash" in content_str: urgency = max(urgency, 8)
        if not is_breaking_flag and "important" in content_str: urgency = max(urgency, 5)

        category = event_to_assess.get('category_guess', 'miscellaneous')
        if any(kw in content_str for kw in ['election', 'president', 'government', 'senate']): category = 'political'
        elif any(kw in content_str for kw in ['sports', 'game', 'match', 'player']): category = 'sports'
        elif any(kw in content_str for kw in ['market', 'economy', 'gdp', 'fed', 'stocks']): category = 'economic'

        if urgency < 5:
            self.logger.info(f"Event {event_to_assess.get('id')} considered not urgent enough by keyword scan ({urgency}).")
            return None

        self.logger.info(f"Breaking event (keyword-flagged): {event_to_assess.get('id')}, Urgency: {urgency}, Category: {category}")
        return {
            "raw_data_event_id": str(event_to_assess.get("id")), "category": category,
            "event_data": event_to_assess["content"], "urgency_score": urgency,
            "metadata": {"assessment_method": "keyword_scan", "monitor_timestamp": time.time()}
        }

    def _persist_breaking_events(self, breaking_items: list[dict]):
        """Persists breaking news items to the 'breaking_events' table."""
        if not breaking_items:
            return 0

        persisted_count = 0
        for item in breaking_items:
            if item: # Ensure item is not None
                try:
                    query = """
                    INSERT INTO breaking_events (raw_data_event_id, category, event_data, urgency_score, processed)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (raw_data_event_id) DO NOTHING;
                    """
                    execute_query(query, (
                        item['raw_data_event_id'], item['category'],
                        Json(item['event_data']), item['urgency_score']
                    ), commit=True)
                    logger.info(f"Inserted breaking event for raw_id {item['raw_data_event_id']} into breaking_events table.")
                    persisted_count +=1
                    # Logic for marking original raw_data_event as processed by monitor or adding 'monitor_checked_at' flag
                    # would go here if needed. For now, rely on time window and CONFLICT resolution.
                except Exception as e:
                    logger.error(f"Failed to insert breaking event into DB: {item}. Error: {e}")
        return persisted_count

    def monitor_and_store(self): # New method to orchestrate
        """Fetches, standardizes breaking news, and stores them."""
        # The collect method from BaseCollector will call _fetch_data and _standardize_data
        potential_breaking_items = super().collect() # This returns list of standardized breaking items or None

        if potential_breaking_items:
            count = self._persist_breaking_events(potential_breaking_items)
            self.logger.info(f"Breaking news monitor: persisted {count} breaking event(s).")
            return count
        else:
            self.logger.info("Breaking news monitor: No breaking events found or persisted in this run.")
            return 0

if __name__ == "__main__":
    monitor = BreakingNewsMonitor()
    # The monitor_and_store method runs one cycle of fetching, standardizing, and storing.
    # Actual periodic scheduling (e.g., every 5 minutes) should be handled by an external scheduler (like cron or a Python scheduler library).
    monitor.monitor_and_store()
