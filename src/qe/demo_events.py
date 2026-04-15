import logging
from datetime import datetime
from random import Random
from typing import Generator, TypedDict

from faker import Faker

logger = logging.getLogger(__name__)

__rnd = Random(0)
__faker = Faker()
__faker.seed_instance(0)

__admins = {f"{__faker.first_name()} {__faker.last_name()}" for _ in range(500)}
# ensure that members are not in admins:
__members = {f"{__faker.first_name()} {__faker.last_name()}" for _ in range(5000)} - __admins
# ensure that guests are not in admins or members:
__guests = {f"{__faker.first_name()} {__faker.last_name()}" for _ in range(6000)} - __admins - __members
# transform sets to lists for random selection:
__admins = list(__admins)
__members = list(__members)
__guests = list(__guests)
__users = {
    "admin": __admins,
    "member": __members,
    "guest": __guests
}
__role = ["admin", "member", "guest"]
__role_weights = [1, 100, 200]  # 100x more members than admins, 200x more guests than admins
__hosts = ["A", "B", "C", "D", "E"]
__host_weights = [1, 2, 4, 8, 16]
__events = [f"E{i:02d}" for i in range(20)]
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

    logger.debug(
        "%d possible unique users: %d admins, %d members, %d guests",
        len(__admins) + len(__members) + len(__guests),
        len(__admins),
        len(__members),
        len(__guests),)
    logger.debug(f"Generating {num_events} events with timestamps between "
                 f"{datetime.fromtimestamp(last_full_hour - time_range)} and {datetime.fromtimestamp(last_full_hour)}")

    for i in range(num_events):
        role = __rnd.choices(__role, weights=__role_weights, k=1)[0]
        yield EventDTO(
            user=__rnd.choice(__users[role]),
            host=__rnd.choices(__hosts, weights=__host_weights, k=1)[0],
            code=__rnd.choices(__events, weights=__event_weights, k=1)[0],
            role=role,
            timestamp=datetime.fromtimestamp(last_full_hour - __rnd.randint(0, time_range)).isoformat()
        )
        if i and i % 100000 == 0:
            logger.debug(f"Generated {i} ({(i / num_events) * 100:.2f}%) events")
