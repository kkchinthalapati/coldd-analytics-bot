import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from coldd.collector import Collector  # noqa: E402
from coldd.config import Config  # noqa: E402
from coldd.storage import empty_dataset  # noqa: E402


def make_cfg(**overrides) -> Config:
    base = dict(token="aaaa.bbbb.cccc", guild_id=111, owner_ids={1}, recent_message_cache=1000)
    base.update(overrides)
    return Config(**base)


@pytest.fixture
def cfg():
    return make_cfg()


@pytest.fixture
def data():
    return empty_dataset(now=1_700_000_000)


@pytest.fixture
def collector(data, cfg):
    return Collector(data, cfg)
