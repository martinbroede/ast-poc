from opensearchpy import OpenSearch


def reset_opensearch(client: OpenSearch) -> dict:
    """
    Delete all documents from all indexes in the cluster
    and then delete all indexes themselves. This is a destructive operation that will remove all data.

    Args:
        client: OpenSearch client

    Returns:
        Dictionary with index names as keys and deleted document counts as values
    """
    try:
        indexes_response = client.indices.get_alias(index="*")
        index_names = [idx for idx in indexes_response.keys()
                       if not idx.startswith(".")]  # Exclude system indexes
    except Exception:
        # If no indexes exist, return empty dict
        return {}

    results = {}
    for index_name in index_names:
        try:
            response = client.delete_by_query(
                index=index_name,
                body={"query": {"match_all": {}}}
            )
            results[index_name] = response["deleted"]
        except Exception as e:
            results[index_name] = f"Error: {str(e)}"

    for index_name in index_names:
        try:
            client.indices.delete(index=index_name)
        except Exception as e:
            results[index_name] = f"Error deleting index: {str(e)}"

    return results


if __name__ == "__main__":
    client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
    results = reset_opensearch(client)
    items = results.items()
    for index_name, deleted_count in items:
        print(f"  {index_name}: {deleted_count} documents deleted")
    if not items:
        print("No indexes found to delete.")
