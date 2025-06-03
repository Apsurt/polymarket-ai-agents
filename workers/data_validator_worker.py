import logging
from app.core.db import execute_query, get_db_connection
from psycopg2.extras import Json
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_and_store_raw_event(data_item: dict):
    logger.info(f"Received data for validation and storage: source={data_item.get('source')}")
    try:
        # 1. Validation (Example: using Pydantic or simple checks)
        if not all(k in data_item for k in ['source', 'event_type', 'category', 'content']): # [cite: 150]
            raise ValueError("Missing essential fields in data_item.")

        # 2. Store in raw_data_events table
        query = """
        INSERT INTO raw_data_events (source, event_type, category, content, metadata, relevance_score)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
        """

        params = (
            data_item['source'],
            data_item['event_type'],
            data_item['category'],
            Json(data_item['content']),
            Json(data_item.get('metadata')),
            data_item.get('relevance_score')
        )

        row_id = execute_query(query, params, fetch_one=True, commit=True)
        logger.info(f"Successfully validated and stored raw event. DB ID: {row_id[0] if row_id else 'N/A'}")

        # 3. (Optional) Further processing logic can be added here if needed,
        # now that direct invocation replaces queueing.
        # For example, if 'breaking_events' were identified here, logic to handle that could be triggered.

    except ValueError as ve:
        logger.error(f"Validation error for data item: {data_item}. Error: {ve}")
        # Optionally, implement alternative error handling (e.g., move to an error table)
    except Exception as e:
        logger.error(f"Error processing data item: {data_item}. Error: {e}", exc_info=True)
        # Handle DB errors, etc.
