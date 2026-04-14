import logging
from typing import Generator

from opensearchpy import OpenSearch, helpers

try:
    from qe.demo_events import EventDTO, generate_random_events
except ModuleNotFoundError:
    from demo_events import EventDTO, generate_random_events

logger = logging.getLogger(__name__)


def push_events_to_opensearch(
    events: Generator[EventDTO],
    client: OpenSearch,
    index_name: str,
    batch_size: int = 100000,
) -> None:
    """Push events to OpenSearch index using bulk indexing."""
    actions = ({"_index": index_name, "_source": event} for event in events)

    total_events = 0
    successful_events = 0
    failed_events = 0

    for total_events, (ok, _) in enumerate(
        helpers.streaming_bulk(client, actions, chunk_size=batch_size),
        start=1,
    ):
        if ok:
            successful_events += 1
        else:
            failed_events += 1

        if total_events % batch_size == 0:
            logger.debug(f"Pushed {total_events} events to OpenSearch")

    logger.debug(
        "Finished pushing events to OpenSearch. "
        f"Total events: {total_events}, successful: {successful_events}, failed: {failed_events}"
    )


if __name__ == "__main__":
    client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
    events = generate_random_events(10_000_000, 31 * 24 * 3600)
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG)
    push_events_to_opensearch(events, client, "events")
