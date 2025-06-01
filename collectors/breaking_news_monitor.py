import logging
import time
import sys
import os

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from collectors.base import BaseCollector, QUEUES
from app.core.db import execute_query, Json
# from some_llm_client import CheaLLMClient # Replace with your actual LLM client (Ollama or API)

logger = logging.getLogger(__name__)

class BreakingNewsMonitor(BaseCollector):
    # This collector might run more frequently or have different logic
    # It might consume from a specific feed or re-evaluate recently collected items
    CALLS = 12 # e.g., once every 5 minutes
    PERIOD = 3600 # Hourly, but internal logic runs every 5 mins

    def __init__(self):
        # This monitor might not be tied to a single 'category' initially,
        # as it assesses urgency across incoming general news.
        # It then categorizes the breaking event.
        super().__init__(source_name="breaking_news_monitor", category="monitoring")
        # self.llm_client = CheapLLMClient(model="gpt-3.5-turbo") # Or your local Ollama model

    def _fetch_data(self) -> list[dict]:
        """
        Instead of fetching new external data, this might:
        1. Query `raw_data_events` for recently added, unprocessed items from high-signal sources.
        2. Or, subscribe to a very low-latency news feed if available.
        For this example, let's assume it queries recent raw_data_events.
        """
        self.logger.info("Checking for potential breaking news from recent raw_data_events...")
        try:
            # Fetch top N recent, unprocessed, high-signal events from raw_data_events
            # This query is illustrative.
            query = """
            SELECT id, source, event_type, category, content, created_at
            FROM raw_data_events
            WHERE processed = FALSE AND created_at > NOW() - INTERVAL '15 minutes'
            -- Add more filters: e.g., specific sources known for breaking news
            -- AND source IN ('twitter_verified_news', 'reuters_realtime_feed')
            ORDER BY created_at DESC
            LIMIT 20;
            """
            # This db call is conceptual for fetching items to evaluate.
            # `execute_query` is defined in app.core.db
            # recent_events = execute_query(query, fetch_all=True)
            recent_events_tuples = execute_query(query, fetch_all=True)

            if not recent_events_tuples:
                return []

            recent_events = []
            # Assuming columns: id, source, event_type, category, content, created_at
            # Convert tuples to dictionaries
            for row in recent_events_tuples:
                recent_events.append({
                    "id": row[0], "source": row[1], "event_type": row[2],
                    "category_guess": row[3], "content": row[4], "created_at": row[5]
                })
            return recent_events

        except Exception as e:
            self.logger.error(f"Error fetching recent events for breaking news monitor: {e}")
            return []


    def _standardize_data(self, event_to_assess: dict) -> dict | None:
        """
        Assess if an event is 'breaking' using an LLM and assign urgency and category.
        The output of this function will be the item enqueued to 'data.breaking'.
        """
        # prompt_template = f"""
        # Assess the following event for urgency and classify its primary category (political, sports, economic, miscellaneous).
        # Event content: {event_to_assess['content']}
        # Source: {event_to_assess['source']}
        #
        # Respond in JSON format with 'is_breaking' (boolean), 'urgency_score' (1-10),
        # 'category' (political, sports, economic, miscellaneous), and a 'brief_reasoning'.
        # Example: {{"is_breaking": true, "urgency_score": 8, "category": "political", "brief_reasoning": "Major election announcement."}}
        # """
        # try:
        #     # llm_response_str = self.llm_client.generate(prompt_template)
        #     # llm_response = json.loads(llm_response_str) # Make sure LLM output is valid JSON
        #
        #     # This is a MOCK LLM response for now:
        #     # Replace with actual LLM call.
        #     time.sleep(0.1) # Simulate LLM processing time
        #     is_breaking_mock = event_to_assess['content'].get("title", "").lower().startswith("breaking")
        #     llm_response = {
        #         "is_breaking": is_breaking_mock or (True if "urgent" in event_to_assess['content'].get("description","").lower() else False),
        #         "urgency_score": 8 if is_breaking_mock else 3,
        #         "category": event_to_assess.get('category_guess', 'miscellaneous'), # LLM should refine this
        #         "brief_reasoning": "Mocked LLM assessment."
        #     }

        #     if llm_response.get("is_breaking"):
        #         self.logger.info(f"Breaking event identified: {event_to_assess.get('id')}, Urgency: {llm_response.get('urgency_score')}")
        #         return {
        #             "raw_data_event_id": str(event_to_assess.get("id")), # Link to the original event
        #             "category": llm_response["category"],
        #             "event_data": event_to_assess["content"], # Or a summary from LLM
        #             "urgency_score": llm_response["urgency_score"],
        #             "metadata": {"llm_assessment": llm_response, "monitor_timestamp": time.time()}
        #         }
        #     return None # Not breaking
        # except Exception as e:
        #     self.logger.error(f"LLM assessment failed for event {event_to_assess.get('id')}: {e}")
        #     return None

        # --- SIMPLIFIED NON-LLM version for initial setup ---
        # Replace this with the LLM logic above once you have your client
        content_str = str(event_to_assess.get('content', '')).lower()
        is_breaking_flag = "breaking" in content_str or "urgent" in content_str or "alert" in content_str
        urgency = 0
        if "breaking" in content_str: urgency = max(urgency, 7)
        if "urgent" in content_str: urgency = max(urgency, 8)
        if "alert" in content_str: urgency = max(urgency, 9)
        if "flash" in content_str: urgency = max(urgency, 8) # e.g. news flash
        if not is_breaking_flag and "important" in content_str : urgency = max(urgency, 5)
        if not is_breaking_flag and not urgency: return None # Not deemed breaking by keywords

        # Basic category determination (LLM would be much better)
        category = event_to_assess.get('category_guess', 'miscellaneous')
        if any(kw in content_str for kw in ['election', 'president', 'government', 'senate']):
            category = 'political'
        elif any(kw in content_str for kw in ['sports', 'game', 'match', 'player']):
            category = 'sports'
        elif any(kw in content_str for kw in ['market', 'economy', 'gdp', 'fed', 'stocks']):
            category = 'economic'

        if urgency < 5: # Only consider events with a minimum urgency score
             self.logger.info(f"Event {event_to_assess.get('id')} considered not urgent enough by keyword scan ({urgency}).")
             return None

        self.logger.info(f"Breaking event (keyword-flagged): {event_to_assess.get('id')}, Urgency: {urgency}, Category: {category}")
        return {
            "raw_data_event_id": str(event_to_assess.get("id")),
            "category": category,
            "event_data": event_to_assess["content"], # Original content for now
            "urgency_score": urgency,
            "metadata": {"assessment_method": "keyword_scan", "monitor_timestamp": time.time()}
        }


    def publish_to_queue(self, data_items: list[dict], queue_name: str = 'data_breaking'): # Override default queue
        """
        Publishes breaking news items to the 'data.breaking' queue.
        And potentially to the database table 'breaking_events'.
        """
        super().publish_to_queue(data_items, queue_name) # Enqueues to RQ

        # Also, directly insert into the breaking_events table for persistence and tracking
        for item in data_items:
            if item: # Ensure item is not None
                try:
                    query = """
                    INSERT INTO breaking_events (raw_data_event_id, category, event_data, urgency_score, processed)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (raw_data_event_id) DO NOTHING; -- Avoid duplicates if monitor runs over same event
                    """
                    # Note: raw_data_event_id should be UUID. Ensure it's cast if necessary.
                    execute_query(query, (
                        item['raw_data_event_id'],
                        item['category'],
                        Json(item['event_data']),
                        item['urgency_score']
                    ), commit=True)
                    logger.info(f"Inserted breaking event for raw_id {item['raw_data_event_id']} into breaking_events table.")
                except Exception as e:
                    logger.error(f"Failed to insert breaking event into DB: {item}. Error: {e}")

                # Mark original raw_data_event as processed by the monitor
                # This is to avoid re-processing by this monitor in its _fetch_data query.
                # The main `processed` flag on `raw_data_events` is for the *next* stage (e.g. analysis).
                # So, maybe add a new flag like `checked_for_breaking` or handle in query.
                # For now, we assume the `created_at > NOW() - INTERVAL '15 minutes'` and `LIMIT` in `_fetch_data`
                # combined with the `ON CONFLICT DO NOTHING` handles basic deduplication for the `breaking_events` table.
                # A more robust solution would be to update a flag on `raw_data_events` like `monitor_checked_at`.


# Example: Run this monitor periodically (e.g., every 5 minutes via cron/scheduler)
if __name__ == "__main__":
    monitor = BreakingNewsMonitor()
    # This runs the full collect cycle, including the 5-minute internal logic.
    # The actual scheduling (every 5 mins) should be external.
    # The `collect` method here would be called by that scheduler.
    monitor.collect()
