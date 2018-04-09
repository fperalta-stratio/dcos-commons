import pytest
import sdk_repository
import sdk_security
from tests import config

from sdk_utils import random_string


@pytest.fixture(scope='session')
def configure_universe(tmpdir_factory):
    yield from sdk_repository.universe_session(
        tmpdir_factory.mktemp(basename=random_string())
    )


@pytest.fixture(scope='session')
def configure_security(configure_universe):
    yield from sdk_security.security_session(config.SERVICE_NAME)
