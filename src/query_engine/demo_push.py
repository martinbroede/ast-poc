from typing import Generator

from demo_events import EventDTO
from opensearchpy import OpenSearch


def push_events_to_opensearch(events: Generator[EventDTO], client: OpenSearch, index_name: str,) -> None:
    """Push events to OpenSearch index."""
    for event in events:
        client.index(index=index_name, body=event)


if __name__ == "__main__":
    from demo_events import generate_random_events
    client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
    events = generate_random_events(100)
    count = push_events_to_opensearch(events, client, "demo_events")
