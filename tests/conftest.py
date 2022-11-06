import pytest
import os
from dotenv import load_dotenv


@pytest.fixture(scope='session')
def create_env_variables_heroku():
    if os.path.isfile('.env'):
        load_dotenv(dotenv_path='.env')
    return os.environ['DATABASE_URL']


@pytest.fixture(scope='session')
def create_env_variables_gcp():
    if os.path.isfile('.env'):
        load_dotenv(dotenv_path='.env')
    return {'host': os.environ['HOST'],
            'database_name': os.environ['DATABASE_NAME'],
            'user_name': os.environ['USER_NAME'],
            'user_password': os.environ['USER_PASSWORD'],
            'port': os.environ['PORT']}
