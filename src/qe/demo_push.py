import logging
from typing import Generator

from opensearchpy import OpenSearch

try:
    from qe.demo_events import EventDTO, generate_random_events
except ModuleNotFoundError:
    from demo_events import EventDTO, generate_random_events

logger = logging.getLogger(__name__)


def push_events_to_opensearch(events: Generator[EventDTO], client: OpenSearch, index_name: str,) -> None:
    """Push events to OpenSearch index."""
    for n, event in enumerate(events, start=1):
        client.index(index=index_name, body=event)
        if n % 1000 == 0:
            logger.debug(f"Pushed {n} events to OpenSearch")
    logger.debug(f"Finished pushing events to OpenSearch. Total events: {n}")


if __name__ == "__main__":
    client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
    events = generate_random_events(1_000_000, 24 * 3600)
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG)
    push_events_to_opensearch(events, client, "events")
