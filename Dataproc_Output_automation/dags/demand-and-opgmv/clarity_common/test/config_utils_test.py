import pytest
import os
from clarity_common.config_utils import get_env


@pytest.mark.parametrize("env_set, expected", [('local', 'local'),
                                               ('nonprod', 'nonprod'),
                                               ('prod', 'prod'),
                                               ('test', 'local')])
def test_get_env(env_set, expected):
    os.environ['ENVIRONMENT'] = env_set
    res = get_env()
    assert expected == res
