import logging
from app.core.db import execute_query, get_db_connection
from psycopg2.extras import Json
from redis import Redis
from rq import Queue
from app.core.config import settings
# Import Pydantic schemas for validation if you define them in schemas/data_schemas.py
# from schemas.data_schemas import RawEventSchema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_conn = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
# Assuming QUEUES are defined centrally or re-defined here if worker is standalone
validation_queue = Queue('data.validation', connection=redis_conn) # For successfully validated data

def validate_and_store_raw_event(data_item: dict):
    logger.info(f"Received data for validation: source={data_item.get('source')}")
    try:
        # 1. Validation (Example: using Pydantic or simple checks)
        # if not RawEventSchema.model_validate(data_item): # Pydantic example
        #     raise ValueError("Data validation failed")
        if not all(k in data_item for k in ['source', 'event_type', 'category', 'content']):
            raise ValueError("Missing essential fields in data_item.")

        # 2. Store in raw_data_events table
        query = """
        INSERT INTO raw_data_events (source, event_type, category, content, metadata, relevance_score)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
        """
        # psycopg2.extras.Json is used to correctly serialize JSONB
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

        # 3. (Optional) Enqueue for further processing if needed (e.g., to a 'data.enrichment' queue)
        # For now, we assume validation + storage is the end of this worker's main task for raw data.
        # If 'breaking_events' are identified by a collector, they'd go to 'data.breaking' directly.
        # If this worker also identified breaking news, it could enqueue there.

        # If data needs to go to a generic "next step" after validation, use 'data.validation' queue
        # validated_data_item = data_item.copy()
        # validated_data_item['raw_data_id'] = row_id[0] # Add DB ID for tracing
        # validation_queue.enqueue('next_processing_step_function', validated_data_item)

    except ValueError as ve:
        logger.error(f"Validation error for data item: {data_item}. Error: {ve}")
        # Optionally, move to a dead-letter queue or error table
    except Exception as e:
        logger.error(f"Error processing data item: {data_item}. Error: {e}", exc_info=True)
        # Handle DB errors, queueing errors, etc.

# To run this worker:
# In your terminal, from the project root:
#PYTHONPATH=. uv python -m rq worker data.raw --url redis://localhost:6379
# (Ensure PYTHONPATH is set so it can find your modules like 'app.core.db')
# Alternatively, create a run_worker.sh script.
