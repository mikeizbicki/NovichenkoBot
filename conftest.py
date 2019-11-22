import pytest
import sqlalchemy

def pytest_addoption(parser):
    parser.addoption("--db", action="store", required=True)

def pytest_generate_tests(metafunc):
    engine = sqlalchemy.create_engine(metafunc.config.option.db) 
    connection = engine.connect()
    if 'connection' in metafunc.fixturenames:
        metafunc.parametrize('connection',[connection])
