from datetime import datetime
from random import Random
from typing import Generator, TypedDict

__TIME_RANGE_SECONDS = 3600
"""
How far back in time to generate events.
Setting to 1 hour will generate events with timestamps within the last hour, starting from the last full hour.
"""

__rnd = Random(0)

__users = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"]
__role = ["admin", "user", "guest"]
__hosts = ["A", "B", "C", "D", "E"]
__events = [f"E{i:02d}" for i in range(10)]


class EventDTO(TypedDict):
    user: str
    host: str
    code: str
    role: str
    timestamp: str


def generate_random_events(num_events: int) -> Generator[EventDTO]:
    last_full_hour = int(datetime.now().timestamp() // 3600 * 3600)
    for _ in range(num_events):
        yield EventDTO(
            user=__rnd.choice(__users),
            host=__rnd.choice(__hosts),
            code=__rnd.choice(__events),
            role=__rnd.choice(__role),
            timestamp=datetime.fromtimestamp(last_full_hour - __rnd.randint(0, __TIME_RANGE_SECONDS)).isoformat()
        )


if __name__ == "__main__":
    for event in generate_random_events(10):
        print(event)
