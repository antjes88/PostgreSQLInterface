import pytest
import os
from dotenv import load_dotenv


@pytest.fixture(scope='session')
def create_env_variables_heroku():
    if os.path.isfile('.env'):
        load_dotenv(dotenv_path='.env')
    return os.environ['DATABASE_URL']
