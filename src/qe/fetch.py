from opensearchpy import OpenSearch


def fetch_events_from_opensearch(
        client: OpenSearch,
        index_name: str = "events",
        constraints: dict | None = None,
        size: int = 10000) -> list[dict]:
    """
    Fetch events from OpenSearch index with optional query constraints.

    Args:
        client: OpenSearch client
        index_name: Index to query
        constraints: Query constraints as dict (e.g., {"user": "Alice", "role": "admin"})
        size: Maximum number of results to return

    Returns:
        List of events matching the query
    """
    if constraints is None:
        constraints = {}

    must_clauses = [
        {"match": {key: value}} for key, value in constraints.items()
    ]

    query = {
        "bool": {
            "must": must_clauses
        }
    } if must_clauses else {"match_all": {}}

    response = client.search(
        index=index_name,
        body={
            "query": query,
            "size": size,
        }
    )

    return [hit["_source"] for hit in response["hits"]["hits"]]


if __name__ == "__main__":
    client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
    events = fetch_events_from_opensearch(client)
    print(f"Fetched {len(events)} events:" if events else "No events found.")
    events_with_alice = fetch_events_from_opensearch(client, constraints={"user": "Alice"})
    print(f"Fetched {len(events_with_alice)} events for user Alice:" if events_with_alice else "No events found for user Alice.")