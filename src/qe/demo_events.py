from datetime import datetime
from random import Random
from typing import Generator, TypedDict

from faker import Faker

__rnd = Random(0)
__faker = Faker()
__faker.seed_instance(0)

__users = [f"{__faker.first_name()} {__faker.last_name()}" for _ in range(1000)]
__role = ["admin", "user", "guest"]
__role_weights = [1, 100, 200]
__hosts = ["A", "B", "C", "D", "E"]
__host_weights = [1, 2, 4, 8, 16]
__events = [f"E{i:02d}" for i in range(10)]
__event_weights = [len(__events) - i for i in range(len(__events))]


class EventDTO(TypedDict):
    user: str
    host: str
    code: str
    role: str
    timestamp: str


def generate_random_events(num_events: int, time_range: int = 3600) -> Generator[EventDTO]:
    """
    Generates random events with timestamps within the specified time range.
    The latest possible timestamp is the last full hour, the earliest possible timestamp is (last full hour - time_range).
    """
    last_full_hour = int(datetime.now().timestamp() // 3600 * 3600)
    print(f"Generating {num_events} events with timestamps between "
          f"{datetime.fromtimestamp(last_full_hour - time_range)} and {datetime.fromtimestamp(last_full_hour)}")
    for _ in range(num_events):
        yield EventDTO(
            user=__rnd.choice(__users),
            host=__rnd.choices(__hosts, weights=__host_weights, k=1)[0],
            code=__rnd.choices(__events, weights=__event_weights, k=1)[0],
            role=__rnd.choices(__role, weights=__role_weights, k=1)[0],
            timestamp=datetime.fromtimestamp(last_full_hour - __rnd.randint(0, time_range)).isoformat()
        )


if __name__ == "__main__":
    for event in generate_random_events(10):
        print(event)
